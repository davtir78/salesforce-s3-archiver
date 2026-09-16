package cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/cache/redis"
	"github.com/davtir78/salesforce-s3-archiver/internal/config"
)

func TestBuildCache(t *testing.T) {
	{
		conf := &config.CacheConfig{
			Redis: &config.RedisConfig{Host: "localhost"},
		}
		c, err := BuildCache(conf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := c.(*redis.RedisCache); !ok {
			t.Errorf("Expected a RedisCache object from BuildCache")
		}
	}

	{
		c, err := BuildCache(&config.CacheConfig{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := c.(*MemoryCache); !ok {
			t.Errorf("Expected an in-process MemoryCache when Redis is not configured, got %T", c)
		}
	}

	{
		conf := &config.CacheConfig{
			Redis: &config.RedisConfig{Host: "localhost", Mode: "sentinel"},
		}
		if _, err := BuildCache(conf); err == nil {
			t.Errorf("Expected an error for an unknown redis mode")
		}
	}

	{
		conf := &config.CacheConfig{
			Redis: &config.RedisConfig{
				Host:    "localhost",
				IAMAuth: config.RedisIAMAuthConfig{Enabled: true, CacheName: "c", UserId: "u", Region: "ap-southeast-2"},
			},
		}
		if _, err := BuildCache(conf); err == nil {
			t.Errorf("Expected an error when IAM auth is enabled without TLS")
		}
	}
}

func TestKeyPrefixAppliesToAllKeys(t *testing.T) {
	inner := NewMemoryCache()
	c := &prefixedCache{inner: inner, prefix: "prod:"}
	if err := c.SetCacheVal("token", "abc"); err != nil {
		t.Fatal(err)
	}
	if err := SetPersistent(c, "watermark", "123"); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"prod:token", "prod:watermark"} {
		if v, _ := inner.GetCacheVal(key); v == nil {
			t.Errorf("%s not stored under the prefix", key)
		}
	}
	if v, _ := c.GetCacheVal("token"); v != "abc" {
		t.Errorf("prefixed get returned %v", v)
	}
	if err := c.DelCacheVal("token"); err != nil {
		t.Fatal(err)
	}
	if v, _ := inner.GetCacheVal("prod:token"); v != nil {
		t.Errorf("prefixed delete did not remove the key")
	}
}

func TestBuildCacheWithoutRedisKeepsTokensInProcess(t *testing.T) {
	c, err := BuildCache(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.(*MemoryCache); !ok {
		t.Fatalf("expected an in-process cache, got %T", c)
	}
	if err := c.SetCacheVal("t", "tok"); err != nil {
		t.Fatal(err)
	}
	if v, _ := c.GetCacheVal("t"); v != "tok" {
		t.Errorf("token not reusable: %v", v)
	}
}

// Without Redis, de-duplication markers must expire like they do in Redis, or
// a long-running collector accumulates one per archived row forever.
func TestMemoryCacheExpiresMarkersButNotWatermarks(t *testing.T) {
	c := NewMemoryCache()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }
	c.TTL = time.Hour

	for i := 0; i < 100; i++ {
		if err := c.SetCacheVal(fmt.Sprintf("marker-%d", i), "1"); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.SetPersistentVal("watermark", "t"); err != nil {
		t.Fatal(err)
	}
	if v, _ := c.GetCacheVal("marker-1"); v != "1" {
		t.Fatalf("marker should be readable before expiry, got %v", v)
	}

	now = now.Add(time.Hour)
	if v, _ := c.GetCacheVal("marker-1"); v != nil {
		t.Errorf("marker should have expired, got %v", v)
	}
	// Writing sweeps the rest, including keys never read again.
	if err := c.SetCacheVal("new", "1"); err != nil {
		t.Fatal(err)
	}
	if n := c.Len(); n != 2 {
		t.Errorf("expected only the watermark and the new marker to remain, got %d keys", n)
	}
	if v, _ := c.GetCacheVal("watermark"); v != "t" {
		t.Errorf("persistent values must not expire, got %v", v)
	}
}

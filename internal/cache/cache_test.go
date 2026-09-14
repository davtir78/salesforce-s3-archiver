package cache

import (
	"testing"

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
		if _, ok := c.(*DummyCache); !ok {
			t.Errorf("Expected a DummyCache object from BuildCache")
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

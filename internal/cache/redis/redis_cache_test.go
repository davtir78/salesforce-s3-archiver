package redis

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"
)

// Runs against a real server: REDIS_TEST_HOST=localhost REDIS_TEST_PORT=6379 go test ./internal/cache/redis
func TestRedisCacheTTLs(t *testing.T) {
	host := os.Getenv("REDIS_TEST_HOST")
	if host == "" {
		t.Skip("REDIS_TEST_HOST not set")
	}
	port, _ := strconv.Atoi(os.Getenv("REDIS_TEST_PORT"))
	c, err := NewRedisCache(RedisConfig{
		Host:     host,
		Port:     port,
		Password: os.Getenv("REDIS_TEST_PASSWORD"),
		TLS: TLSConfig{
			Enabled: os.Getenv("REDIS_TEST_TLS") == "true",
			CAFile:  os.Getenv("REDIS_TEST_CA_FILE"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Client.Close()
	ctx := context.Background()
	prefix := "ttl-test-" + strconv.FormatInt(time.Now().UnixNano(), 10)

	if err := c.SetPersistentVal(prefix+":watermark", "123"); err != nil {
		t.Fatal(err)
	}
	if ttl := c.Client.TTL(ctx, prefix+":watermark").Val(); ttl != -1 {
		t.Errorf("persistent value must have no TTL, got %v", ttl)
	}

	if err := c.SetCacheVal(prefix+":dedup", "1"); err != nil {
		t.Fatal(err)
	}
	ttl := c.Client.TTL(ctx, prefix+":dedup").Val()
	if ttl <= 0 || ttl > DefaultExpireDays*24*time.Hour {
		t.Errorf("expireDays=0 must default to %d days, got %v", DefaultExpireDays, ttl)
	}
	c.Client.Del(ctx, prefix+":watermark", prefix+":dedup")
}

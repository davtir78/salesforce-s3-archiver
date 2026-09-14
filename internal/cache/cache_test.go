package cache

import (
	"testing"

	"github.com/davtir78/salesforce-s3-archiver/internal/cache/redis"
	"github.com/davtir78/salesforce-s3-archiver/internal/config"
)

func TestBuildCache(t *testing.T) {
	{
		config := &config.CacheConfig{
			Redis: &config.RedisConfig{},
		}
		cache := BuildCache(config)
		_, ok := cache.(*redis.RedisCache)
		if !ok {
			t.Errorf("Expected a RedisCache object from BuildCache")
		}
	}

	{
		config := &config.CacheConfig{}
		cache := BuildCache(config)
		_, ok := cache.(*DummyCache)
		if !ok {
			t.Errorf("Expected a DummyCache object from BuildCache")
		}
	}
}

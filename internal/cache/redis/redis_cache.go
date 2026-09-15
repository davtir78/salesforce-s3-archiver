package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache implements the best-effort cache used for access tokens,
// watermarks and de-duplication markers. Stream replay checkpoints do NOT use
// this type: they live in the checkpoint store, which never sets a TTL.
type RedisCache struct {
	Conf   RedisConfig
	Client redis.UniversalClient
}

func (c *RedisCache) GetCacheVal(key string) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.opTimeout())
	defer cancel()
	val, err := c.Client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	return val, err
}

// DefaultExpireDays applies to tokens and de-duplication markers when expireDays
// is not set, so they cannot accumulate forever.
const DefaultExpireDays = 7

func (c *RedisCache) SetCacheVal(key string, val any) error {
	days := c.Conf.ExpireDays
	if days <= 0 {
		days = DefaultExpireDays
	}
	return c.set(key, val, time.Duration(days*24)*time.Hour)
}

// SetPersistentVal stores a value without expiry (used for watermarks).
func (c *RedisCache) SetPersistentVal(key string, val any) error {
	return c.set(key, val, 0)
}

func (c *RedisCache) set(key string, val any, ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.opTimeout())
	defer cancel()
	return c.Client.Set(ctx, key, val, ttl).Err()
}

func (c *RedisCache) DelCacheVal(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.opTimeout())
	defer cancel()
	return c.Client.Del(ctx, key).Err()
}

func (c *RedisCache) opTimeout() time.Duration {
	return c.Conf.ReadTimeout + c.Conf.WriteTimeout + 2*time.Second
}

func NewRedisCache(conf RedisConfig) (RedisCache, error) {
	client, err := NewClient(conf)
	if err != nil {
		return RedisCache{}, err
	}
	return RedisCache{Conf: conf.WithDefaults(), Client: client}, nil
}

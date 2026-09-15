package cache

import (
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/cache/redis"
	"github.com/davtir78/salesforce-s3-archiver/internal/config"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	goredis "github.com/redis/go-redis/v9"
)

type Cache interface {
	GetCacheVal(key string) (any, error)
	SetCacheVal(key string, val any) error
	DelCacheVal(key string) error
}

// PersistentCache is implemented by caches that can store values without expiry.
type PersistentCache interface {
	SetPersistentVal(key string, val any) error
}

// SetPersistent stores a value without expiry when the cache supports it.
func SetPersistent(c Cache, key string, val any) error {
	if p, ok := c.(PersistentCache); ok {
		return p.SetPersistentVal(key, val)
	}
	return c.SetCacheVal(key, val)
}

type DummyCache struct{}

func (c *DummyCache) GetCacheVal(key string) (any, error) {
	return nil, nil
}

func (c *DummyCache) SetCacheVal(key string, val any) error {
	return nil
}

func (c *DummyCache) DelCacheVal(key string) error {
	return nil
}

// RedisSettings converts the YAML redis config into client settings.
func RedisSettings(conf *config.RedisConfig) redis.RedisConfig {
	return redis.RedisConfig{
		Host:       conf.Host,
		Port:       int(conf.Port),
		DbNumber:   int(conf.DbNumber),
		Username:   conf.Username,
		Password:   conf.Password,
		ExpireDays: int(conf.ExpireDays),
		Mode:       conf.Mode,
		TLS: redis.TLSConfig{
			Enabled:            conf.TLS.Enabled,
			InsecureSkipVerify: conf.TLS.InsecureSkipVerify,
			CAFile:             conf.TLS.CAFile,
			ServerName:         conf.TLS.ServerName,
		},
		IAMAuth: redis.IAMAuthConfig{
			Enabled:    conf.IAMAuth.Enabled,
			CacheName:  conf.IAMAuth.CacheName,
			UserId:     conf.IAMAuth.UserId,
			Region:     conf.IAMAuth.Region,
			Serverless: conf.IAMAuth.Serverless,
		},
		DialTimeout:  time.Duration(conf.DialTimeoutSeconds) * time.Second,
		ReadTimeout:  time.Duration(conf.ReadTimeoutSeconds) * time.Second,
		WriteTimeout: time.Duration(conf.WriteTimeoutSeconds) * time.Second,
		MaxRetries:   conf.MaxRetries,
	}
}

// BuildRedisClient returns a raw client for components that need more than
// get/set (e.g. the stream checkpoint store), or nil when Redis is not configured.
func BuildRedisClient(conf *config.CacheConfig) (goredis.UniversalClient, error) {
	if conf == nil || conf.Redis == nil {
		return nil, nil
	}
	return redis.NewClient(RedisSettings(conf.Redis))
}

func BuildCache(conf *config.CacheConfig) (Cache, error) {
	if conf == nil {
		log.Warnf("No cache config")
		return &DummyCache{}, nil
	}
	if conf.Redis == nil {
		log.Warnf("No redis cache config")
		return &DummyCache{}, nil
	}
	log.Debugf("Using Redis cache")
	redisDb, err := redis.NewRedisCache(RedisSettings(conf.Redis))
	if err != nil {
		return nil, err
	}
	return &redisDb, nil
}

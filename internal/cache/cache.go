package cache

import (
	"github.com/davtir78/salesforce-s3-archiver/internal/cache/redis"
	"github.com/davtir78/salesforce-s3-archiver/internal/config"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
)

type Cache interface {
	GetCacheVal(key string) (any, error)
	SetCacheVal(key string, val any) error
	DelCacheVal(key string) error
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

func BuildCache(conf *config.CacheConfig) Cache {
	var db Cache
	if conf != nil {
		if conf.Redis != nil {
			log.Debugf("Using Redis cache")
			redisDb := redis.NewRedisCache(redis.RedisConfig{
				Host:       conf.Redis.Host,
				Port:       int(conf.Redis.Port),
				DbNumber:   int(conf.Redis.DbNumber),
				Password:   conf.Redis.Password,
				ExpireDays: int(conf.Redis.ExpireDays),
			})
			db = &redisDb
		} else {
			log.Warnf("No redis cache config")
			db = &DummyCache{}
		}
	} else {
		log.Warnf("No cache config")
		db = &DummyCache{}
	}

	return db
}

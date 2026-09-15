package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/redis/go-redis/v9"
)

var (
	renewScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0`)

	commitScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[2], ARGV[3])
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0`)

	releaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`)
)

// RedisStore keeps leases and checkpoints in Redis/Valkey/ElastiCache.
// Keys share a hash tag so the scripts work in cluster mode.
type RedisStore struct {
	Client    redis.UniversalClient
	KeyPrefix string
	Owner     string
	// How long to wait between acquisition attempts (default ttl/3).
	RetryInterval time.Duration
}

func NewRedisStore(client redis.UniversalClient, keyPrefix string) *RedisStore {
	return &RedisStore{Client: client, KeyPrefix: keyPrefix, Owner: NewOwner()}
}

func (s *RedisStore) keys(key string) (lease, replay string) {
	base := s.KeyPrefix + "sfarch:{" + key + "}"
	return base + ":lease", base + ":replay"
}

func (s *RedisStore) Acquire(ctx context.Context, key string, ttl time.Duration) (Lease, error) {
	if ttl < time.Second {
		return nil, fmt.Errorf("lease ttl must be at least 1s, got %s", ttl)
	}
	leaseKey, replayKey := s.keys(key)
	retry := s.RetryInterval
	if retry == 0 {
		retry = ttl / 3
	}
	waiting := false
	for {
		ok, err := s.Client.SetNX(ctx, leaseKey, s.Owner, ttl).Result()
		if err != nil {
			return nil, fmt.Errorf("acquiring lease %s: %w", leaseKey, err)
		}
		if ok {
			if waiting {
				log.Infof("Acquired checkpoint lease %s", leaseKey)
			}
			return &redisLease{store: s, leaseKey: leaseKey, replayKey: replayKey, ttl: ttl}, nil
		}
		if !waiting {
			holder, _ := s.Client.Get(ctx, leaseKey).Result()
			log.Warnf("Checkpoint lease %s is held by %q; waiting (another collector for this topic may be running)", leaseKey, holder)
			waiting = true
		}
		if err := sleepCtx(ctx, retry); err != nil {
			return nil, err
		}
	}
}

type redisLease struct {
	store     *RedisStore
	leaseKey  string
	replayKey string
	ttl       time.Duration
}

func (l *redisLease) Owner() string { return l.store.Owner }

func (l *redisLease) Load(ctx context.Context) ([]byte, bool, error) {
	val, err := l.store.Client.Get(ctx, l.replayKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("loading checkpoint %s: %w", l.replayKey, err)
	}
	return val, true, nil
}

func (l *redisLease) Commit(ctx context.Context, replayId []byte) error {
	if len(replayId) == 0 {
		return errors.New("refusing to commit an empty replay ID")
	}
	res, err := commitScript.Run(ctx, l.store.Client, []string{l.leaseKey, l.replayKey},
		l.store.Owner, l.ttl.Milliseconds(), replayId).Int()
	if err != nil {
		return fmt.Errorf("committing checkpoint %s: %w", l.replayKey, err)
	}
	if res != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (l *redisLease) Renew(ctx context.Context) error {
	res, err := renewScript.Run(ctx, l.store.Client, []string{l.leaseKey}, l.store.Owner, l.ttl.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("renewing lease %s: %w", l.leaseKey, err)
	}
	if res != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (l *redisLease) Release(ctx context.Context) error {
	return releaseScript.Run(ctx, l.store.Client, []string{l.leaseKey}, l.store.Owner).Err()
}

package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/cache/redis"
)

// MemoryCache is an in-process Cache for tests and single-run development, and
// the fallback when no Redis is configured. Like Redis, values set with
// SetCacheVal expire (tokens and de-duplication markers), while values set with
// SetPersistentVal do not (watermarks), so a long-running process does not
// accumulate a marker for every row it has ever archived.
type MemoryCache struct {
	mu     sync.Mutex
	vals   map[string]memoryEntry
	lastGC time.Time
	// TTL for SetCacheVal; defaults to Redis's default expiry.
	TTL time.Duration
	// Err, when set, is returned by every operation (simulates an outage).
	Err error
	// now is replaceable in tests.
	now func() time.Time
}

type memoryEntry struct {
	val     any
	expires time.Time // zero = never
}

func NewMemoryCache() *MemoryCache {
	return &MemoryCache{
		vals: map[string]memoryEntry{},
		TTL:  time.Duration(redis.DefaultExpireDays) * 24 * time.Hour,
		now:  time.Now,
	}
}

func (c *MemoryCache) GetCacheVal(key string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Err != nil {
		return nil, c.Err
	}
	e, ok := c.vals[key]
	if !ok {
		return nil, nil
	}
	if !e.expires.IsZero() && !c.now().Before(e.expires) {
		delete(c.vals, key)
		return nil, nil
	}
	return e.val, nil
}

func (c *MemoryCache) SetCacheVal(key string, val any) error {
	return c.set(key, val, c.TTL)
}

func (c *MemoryCache) SetPersistentVal(key string, val any) error {
	return c.set(key, val, 0)
}

func (c *MemoryCache) set(key string, val any, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Err != nil {
		return c.Err
	}
	now := c.now()
	e := memoryEntry{val: fmt.Sprint(val)} // match Redis, which returns strings
	if ttl > 0 {
		e.expires = now.Add(ttl)
	}
	c.vals[key] = e
	// Sweep expired entries at most once a minute, so the cost is amortised
	// and keys that are never read again are still released.
	if now.Sub(c.lastGC) >= time.Minute {
		c.lastGC = now
		for k, v := range c.vals {
			if !v.expires.IsZero() && !now.Before(v.expires) {
				delete(c.vals, k)
			}
		}
	}
	return nil
}

func (c *MemoryCache) DelCacheVal(key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Err != nil {
		return c.Err
	}
	delete(c.vals, key)
	return nil
}

// Clear removes every key.
func (c *MemoryCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.vals = map[string]memoryEntry{}
}

// Len returns the number of stored keys, including expired ones not yet swept.
func (c *MemoryCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.vals)
}

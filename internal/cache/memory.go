package cache

import (
	"fmt"
	"sync"
)

// MemoryCache is an in-process Cache for tests and single-run development.
type MemoryCache struct {
	mu   sync.Mutex
	vals map[string]any
	// Err, when set, is returned by every operation (simulates an outage).
	Err error
}

func NewMemoryCache() *MemoryCache {
	return &MemoryCache{vals: map[string]any{}}
}

func (c *MemoryCache) GetCacheVal(key string) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Err != nil {
		return nil, c.Err
	}
	return c.vals[key], nil
}

func (c *MemoryCache) SetCacheVal(key string, val any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Err != nil {
		return c.Err
	}
	// Match Redis, which returns stored values as strings.
	c.vals[key] = fmt.Sprint(val)
	return nil
}

func (c *MemoryCache) SetPersistentVal(key string, val any) error {
	return c.SetCacheVal(key, val)
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
	c.vals = map[string]any{}
}

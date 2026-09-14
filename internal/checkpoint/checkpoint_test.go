package checkpoint

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	sfredis "github.com/davtir78/salesforce-s3-archiver/internal/cache/redis"
)

// storeFactory returns two independent handles on the same backend.
type storeFactory func(t *testing.T) (Store, Store, func(key string))

func memoryFactory(t *testing.T) (Store, Store, func(string)) {
	a := NewMemoryStore()
	return a, a.Share(), a.ExpireLease
}

func redisFactory(t *testing.T) (Store, Store, func(string)) {
	host := os.Getenv("REDIS_TEST_HOST")
	if host == "" {
		t.Skip("REDIS_TEST_HOST not set")
	}
	port, _ := strconv.Atoi(os.Getenv("REDIS_TEST_PORT"))
	conf := sfredis.RedisConfig{
		Host:     host,
		Port:     port,
		Password: os.Getenv("REDIS_TEST_PASSWORD"),
		TLS: sfredis.TLSConfig{
			Enabled: os.Getenv("REDIS_TEST_TLS") == "true",
			CAFile:  os.Getenv("REDIS_TEST_CA_FILE"),
		},
	}
	client, err := sfredis.NewClient(conf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	prefix := "test-" + NewOwner() + ":"
	a := NewRedisStore(client, prefix)
	b := NewRedisStore(client, prefix)
	a.RetryInterval = 20 * time.Millisecond
	b.RetryInterval = 20 * time.Millisecond
	expire := func(key string) {
		leaseKey, _ := a.keys(key)
		client.Del(context.Background(), leaseKey)
	}
	return a, b, expire
}

func TestStores(t *testing.T) {
	for name, f := range map[string]storeFactory{"memory": memoryFactory, "redis": redisFactory} {
		t.Run(name, func(t *testing.T) { runStoreContract(t, f) })
	}
}

func runStoreContract(t *testing.T, factory storeFactory) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, b, expire := factory(t)
	key := Key("prod", "/event/LoginEventStream")

	la, err := a.Acquire(ctx, key, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := la.Load(ctx); err != nil || ok {
		t.Fatalf("expected no checkpoint, ok=%v err=%v", ok, err)
	}
	if err := la.Commit(ctx, nil); err == nil {
		t.Errorf("empty replay ID must be rejected")
	}
	replay := []byte{0x00, 0x01, 0xff, '{', '}'}
	if err := la.Commit(ctx, replay); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := la.Load(ctx); err != nil || !ok || string(got) != string(replay) {
		t.Fatalf("binary replay ID not preserved: %v %v %v", got, ok, err)
	}

	// A second collector must wait while the lease is held.
	shortCtx, shortCancel := context.WithTimeout(ctx, 150*time.Millisecond)
	if _, err := b.Acquire(shortCtx, key, 5*time.Second); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("second acquire should block, got %v", err)
	}
	shortCancel()

	// Simulate lease expiry (e.g. partition); the other collector takes over.
	expire(key)
	lb, err := b.Acquire(ctx, key, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok, _ := lb.Load(ctx); !ok || string(got) != string(replay) {
		t.Errorf("new holder must see committed checkpoint")
	}

	// The old holder can no longer move the checkpoint or renew.
	if err := la.Commit(ctx, []byte("stale")); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("stale holder commit: got %v, want ErrLeaseLost", err)
	}
	if err := la.Renew(ctx); !errors.Is(err, ErrLeaseLost) {
		t.Errorf("stale holder renew: got %v, want ErrLeaseLost", err)
	}
	if err := la.Release(ctx); err != nil {
		t.Errorf("stale release should be a no-op: %v", err)
	}
	if err := lb.Commit(ctx, []byte("new")); err != nil {
		t.Errorf("current holder commit failed after stale release: %v", err)
	}
	if err := lb.Renew(ctx); err != nil {
		t.Errorf("renew failed: %v", err)
	}
	if err := lb.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// After release the first handle can acquire again immediately.
	la2, err := a.Acquire(ctx, key, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got, _, _ := la2.Load(ctx); string(got) != "new" {
		t.Errorf("checkpoint = %q, want new", got)
	}
	la2.Release(ctx)
}

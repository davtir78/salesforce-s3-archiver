// Package checkpoint stores Pub/Sub replay checkpoints behind a per-topic lease.
//
// Only the current lease holder may commit a checkpoint. If a second collector
// for the same topic starts (for example during a node partition) it waits for
// the lease, and a holder that lost its lease can no longer move the
// checkpoint. Checkpoints never expire.
package checkpoint

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

var (
	// ErrLeaseLost is returned when the lease expired or was taken by another owner.
	ErrLeaseLost = errors.New("checkpoint lease lost")
)

type Store interface {
	// Acquire blocks until the lease for key is held or ctx is done.
	Acquire(ctx context.Context, key string, ttl time.Duration) (Lease, error)
}

type Lease interface {
	Owner() string
	// Load returns the stored replay ID, or ok=false if none exists.
	Load(ctx context.Context) (replayId []byte, ok bool, err error)
	// Commit stores replayId if the lease is still held, and extends the lease.
	Commit(ctx context.Context, replayId []byte) error
	// Renew extends the lease.
	Renew(ctx context.Context) error
	// Release gives up the lease if still held.
	Release(ctx context.Context) error
	// Clear deletes the stored checkpoint (operator reset) if the lease is still held.
	Clear(ctx context.Context) error
}

// Key builds the lease/checkpoint key for a stream instance and topic.
func Key(instance, topic string) string {
	r := strings.NewReplacer("{", "_", "}", "_")
	return r.Replace(instance) + "/" + r.Replace(topic)
}

// NewOwner returns a unique owner token (hostname plus random suffix).
func NewOwner() string {
	host, _ := os.Hostname()
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return host + "-" + hex.EncodeToString(b)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

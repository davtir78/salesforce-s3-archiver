package checkpoint

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MemoryStore is an in-process Store for tests. Several MemoryStore handles
// created with Share() behave like separate collectors sharing one backend.
type MemoryStore struct {
	backend *memoryBackend
	owner   string

	// CommitHook runs before a commit is applied; an error aborts the commit.
	CommitHook func(replayId []byte) error
	// AfterCommitHook runs after a commit is applied; an error simulates a
	// crash after the checkpoint was persisted.
	AfterCommitHook func(replayId []byte) error
	// LoadHook can inject load failures.
	LoadHook func() error
}

type memoryBackend struct {
	mu          sync.Mutex
	leases      map[string]memoryLeaseState
	checkpoints map[string][]byte
	now         func() time.Time
}

type memoryLeaseState struct {
	owner   string
	expires time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		backend: &memoryBackend{leases: map[string]memoryLeaseState{}, checkpoints: map[string][]byte{}, now: time.Now},
		owner:   NewOwner(),
	}
}

// Share returns a new handle (distinct owner) on the same backend.
func (s *MemoryStore) Share() *MemoryStore {
	return &MemoryStore{backend: s.backend, owner: NewOwner()}
}

// Checkpoint returns the stored value for key.
func (s *MemoryStore) Checkpoint(key string) ([]byte, bool) {
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	v, ok := s.backend.checkpoints[key]
	return append([]byte(nil), v...), ok
}

// Set writes a checkpoint directly, bypassing leases (test setup only).
func (s *MemoryStore) Set(key string, replayId []byte) {
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	s.backend.checkpoints[key] = append([]byte(nil), replayId...)
}

// ExpireLease forcibly expires the lease for key (simulates a partition).
func (s *MemoryStore) ExpireLease(key string) {
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	delete(s.backend.leases, key)
}

func (s *MemoryStore) Acquire(ctx context.Context, key string, ttl time.Duration) (Lease, error) {
	for {
		b := s.backend
		b.mu.Lock()
		st, held := b.leases[key]
		if !held || b.now().After(st.expires) || st.owner == s.owner {
			b.leases[key] = memoryLeaseState{owner: s.owner, expires: b.now().Add(ttl)}
			b.mu.Unlock()
			return &memoryLease{store: s, key: key, ttl: ttl}, nil
		}
		b.mu.Unlock()
		if err := sleepCtx(ctx, 10*time.Millisecond); err != nil {
			return nil, err
		}
	}
}

type memoryLease struct {
	store *MemoryStore
	key   string
	ttl   time.Duration
}

func (l *memoryLease) Owner() string { return l.store.owner }

func (l *memoryLease) holds() bool {
	st, ok := l.store.backend.leases[l.key]
	return ok && st.owner == l.store.owner && !l.store.backend.now().After(st.expires)
}

func (l *memoryLease) Load(ctx context.Context) ([]byte, bool, error) {
	if l.store.LoadHook != nil {
		if err := l.store.LoadHook(); err != nil {
			return nil, false, err
		}
	}
	b := l.store.backend
	b.mu.Lock()
	defer b.mu.Unlock()
	v, ok := b.checkpoints[l.key]
	return append([]byte(nil), v...), ok, nil
}

func (l *memoryLease) Commit(ctx context.Context, replayId []byte) error {
	if len(replayId) == 0 {
		return errors.New("refusing to commit an empty replay ID")
	}
	if l.store.CommitHook != nil {
		if err := l.store.CommitHook(replayId); err != nil {
			return err
		}
	}
	// Behave like a network store: a cancelled context fails the call.
	if err := ctx.Err(); err != nil {
		return err
	}
	b := l.store.backend
	b.mu.Lock()
	if !l.holds() {
		b.mu.Unlock()
		return ErrLeaseLost
	}
	b.checkpoints[l.key] = append([]byte(nil), replayId...)
	b.leases[l.key] = memoryLeaseState{owner: l.store.owner, expires: b.now().Add(l.ttl)}
	b.mu.Unlock()
	if l.store.AfterCommitHook != nil {
		return l.store.AfterCommitHook(replayId)
	}
	return nil
}

func (l *memoryLease) Renew(ctx context.Context) error {
	b := l.store.backend
	b.mu.Lock()
	defer b.mu.Unlock()
	if !l.holds() {
		return ErrLeaseLost
	}
	b.leases[l.key] = memoryLeaseState{owner: l.store.owner, expires: b.now().Add(l.ttl)}
	return nil
}

func (l *memoryLease) Release(ctx context.Context) error {
	b := l.store.backend
	b.mu.Lock()
	defer b.mu.Unlock()
	if l.holds() {
		delete(b.leases, l.key)
	}
	return nil
}

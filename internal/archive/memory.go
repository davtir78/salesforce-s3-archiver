package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

// MemorySink keeps objects in memory. It is used by tests and supports fault
// injection through the BeforeStore and AfterStore hooks.
type MemorySink struct {
	mu        sync.Mutex
	objects   map[string][]byte
	manifests map[string]Manifest
	writes    int

	// BeforeStore is called before an object is stored; returning an error
	// simulates a failed upload (nothing is stored).
	BeforeStore func(meta ObjectMeta, writeNumber int) error
	// AfterStore is called after the object and manifest are stored; returning
	// an error simulates a crash between a successful upload and the caller
	// learning about it.
	AfterStore func(meta ObjectMeta, writeNumber int) error
	// StoreDelay simulates a slow store; the wait is cut short if the write
	// context is cancelled.
	StoreDelay time.Duration
}

func NewMemorySink() *MemorySink {
	return &MemorySink{objects: map[string][]byte{}, manifests: map[string]Manifest{}}
}

func (s *MemorySink) Write(ctx context.Context, meta ObjectMeta, fill func(w RecordWriter) error) (Manifest, error) {
	s.mu.Lock()
	s.writes++
	n := s.writes
	s.mu.Unlock()

	if s.BeforeStore != nil {
		if err := s.BeforeStore(meta, n); err != nil {
			return Manifest{}, err
		}
	}
	m, err := encodeObject(ctx, meta, fill, func(ctx context.Context, body *os.File, m *Manifest) error {
		if s.StoreDelay > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(s.StoreDelay):
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		name := meta.Name
		if name == "" {
			name = UniqueName(m.IngestedAt)
		}
		dataKey, _ := ObjectPaths(meta, name)
		m.ObjectKey = dataKey
		b, err := io.ReadAll(body)
		if err != nil {
			return err
		}
		// Round-trip the manifest through JSON like a real store would.
		mb, _ := json.Marshal(m)
		var stored Manifest
		_ = json.Unmarshal(mb, &stored)

		s.mu.Lock()
		s.objects[dataKey] = b
		s.manifests[dataKey] = stored
		s.mu.Unlock()
		return nil
	})
	if err != nil {
		return m, err
	}
	if s.AfterStore != nil {
		if err := s.AfterStore(meta, n); err != nil {
			return Manifest{}, err
		}
	}
	return m, nil
}

// Keys returns the stored data keys in sorted order.
func (s *MemorySink) Keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.objects))
	for k := range s.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (s *MemorySink) Manifest(key string) (Manifest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.manifests[key]
	return m, ok
}

// Lines decodes every stored object.
func (s *MemorySink) Lines() ([]Line, error) {
	var out []Line
	for _, k := range s.Keys() {
		s.mu.Lock()
		b := s.objects[k]
		s.mu.Unlock()
		err := DecodeObject(bytes.NewReader(b), func(l Line) error {
			out = append(out, l)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// ErrInjected is returned by fault hooks in tests.
var ErrInjected = errors.New("injected fault")

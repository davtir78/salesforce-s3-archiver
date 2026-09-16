package archive

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// LocalSink writes objects to a directory using the same key layout as S3.
// Intended for development and tests.
type LocalSink struct {
	Dir    string
	Prefix string
}

func NewLocalSink(dir, prefix string) (*LocalSink, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &LocalSink{Dir: dir, Prefix: prefix}, nil
}

func (s *LocalSink) Write(ctx context.Context, meta ObjectMeta, fill func(w RecordWriter) error) (Manifest, error) {
	return encodeObject(ctx, meta, fill, func(ctx context.Context, body *os.File, m *Manifest) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := meta.Name
		if name == "" {
			name = UniqueName(m.IngestedAt)
		}
		dataKey, manifestKey := ObjectPaths(meta, name)
		dataKey = joinPrefix(s.Prefix, dataKey)
		manifestKey = joinPrefix(s.Prefix, manifestKey)
		m.ObjectKey = dataKey

		if err := s.atomicWrite(dataKey, body); err != nil {
			return fmt.Errorf("writing data object: %w", err)
		}
		mb, err := json.MarshalIndent(m, "", "  ")
		if err != nil {
			return err
		}
		if err := s.atomicWriteBytes(manifestKey, mb); err != nil {
			return fmt.Errorf("writing manifest: %w", err)
		}
		return nil
	})
}

func (s *LocalSink) atomicWrite(key string, r io.Reader) error {
	path := filepath.Join(s.Dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := io.Copy(tmp, r); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	// Without an fsync of the directory the rename can be lost in a crash,
	// while Write already reported success and the checkpoint advanced.
	return syncDir(filepath.Dir(path))
}

func (s *LocalSink) atomicWriteBytes(key string, b []byte) error {
	return s.atomicWrite(key, bytesReader(b))
}

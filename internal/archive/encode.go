package archive

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"time"
)

// Line is the JSON shape of one NDJSON line in a data object: a small envelope
// around the unchanged source record in Payload.
type Line struct {
	EventId   string         `json:"event_id,omitempty"`
	EventType string         `json:"event_type"`
	Timestamp time.Time      `json:"timestamp"`
	Source    string         `json:"source"`
	Env       string         `json:"env,omitempty"`
	OrgId     string         `json:"org_id,omitempty"`
	Instance  string         `json:"instance,omitempty"`
	ReplayId  string         `json:"replay_id,omitempty"`
	Payload   map[string]any `json:"payload"`
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// encoder streams records into a gzip-compressed temporary file.
type encoder struct {
	meta       ObjectMeta
	file       *os.File
	compressed *countingWriter
	hasher     hash.Hash
	gz         *gzip.Writer
	raw        *countingWriter
	buf        *bufio.Writer
	enc        *json.Encoder
	count      int64
	first      *time.Time
	last       *time.Time
}

func newEncoder(meta ObjectMeta) (*encoder, error) {
	f, err := os.CreateTemp("", "sf-archive-*.json.gz")
	if err != nil {
		return nil, fmt.Errorf("creating temp object file: %w", err)
	}
	h := sha256.New()
	compressed := &countingWriter{w: io.MultiWriter(f, h)}
	gz := gzip.NewWriter(compressed)
	raw := &countingWriter{w: gz}
	buf := bufio.NewWriterSize(raw, 256*1024)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	return &encoder{meta: meta, file: f, compressed: compressed, hasher: h, gz: gz, raw: raw, buf: buf, enc: enc}, nil
}

func (e *encoder) WriteRecord(r Record) error {
	line := Line{
		EventId:   r.Id,
		EventType: r.Type,
		Timestamp: r.Timestamp.UTC(),
		Source:    e.meta.Source,
		Env:       e.meta.Env,
		OrgId:     e.meta.OrgId,
		Instance:  e.meta.Instance,
		Payload:   r.Payload,
	}
	if line.Payload == nil {
		line.Payload = map[string]any{}
	}
	if len(r.ReplayId) > 0 {
		line.ReplayId = base64.StdEncoding.EncodeToString(r.ReplayId)
	}
	if err := e.enc.Encode(&line); err != nil {
		return fmt.Errorf("encoding record: %w", err)
	}
	e.count++
	ts := line.Timestamp
	if ts.IsZero() {
		// A zero timestamp would make the manifest range start at year 1.
		return nil
	}
	if e.first == nil || ts.Before(*e.first) {
		t := ts
		e.first = &t
	}
	if e.last == nil || ts.After(*e.last) {
		t := ts
		e.last = &t
	}
	return nil
}

// finish flushes and closes the compressed stream, rewinds the file and
// returns the partially filled manifest.
func (e *encoder) finish() (Manifest, error) {
	if err := e.buf.Flush(); err != nil {
		return Manifest{}, err
	}
	if err := e.gz.Close(); err != nil {
		return Manifest{}, err
	}
	if err := e.file.Sync(); err != nil {
		return Manifest{}, err
	}
	if _, err := e.file.Seek(0, io.SeekStart); err != nil {
		return Manifest{}, err
	}
	return Manifest{
		ManifestVersion:   ManifestVersion,
		Collector:         CollectorName,
		CollectorVersion:  CollectorVersion,
		Source:            e.meta.Source,
		OrgId:             e.meta.OrgId,
		Instance:          e.meta.Instance,
		EventType:         e.meta.EventType,
		RecordCount:       e.count,
		UncompressedBytes: e.raw.n,
		CompressedBytes:   e.compressed.n,
		SHA256:            hex.EncodeToString(e.hasher.Sum(nil)),
		FirstTimestamp:    e.first,
		LastTimestamp:     e.last,
		Lineage:           e.meta.Lineage,
	}, nil
}

// close releases the temp file. Safe to call multiple times.
func (e *encoder) close() {
	if e.file == nil {
		return
	}
	name := e.file.Name()
	e.file.Close()
	os.Remove(name)
	e.file = nil
}

// encodeObject runs fill into a temp file and hands the file to store.
func encodeObject(
	ctx context.Context,
	meta ObjectMeta,
	fill func(w RecordWriter) error,
	store func(ctx context.Context, body *os.File, m *Manifest) error,
) (Manifest, error) {
	if meta.Source == "" {
		return Manifest{}, errors.New("archive: object source is required")
	}
	if meta.PartitionTime.IsZero() {
		meta.PartitionTime = time.Now()
	}
	enc, err := newEncoder(meta)
	if err != nil {
		return Manifest{}, err
	}
	defer enc.close()

	if err := fill(enc); err != nil {
		return Manifest{}, err
	}
	m, err := enc.finish()
	if err != nil {
		return Manifest{}, fmt.Errorf("finalising object: %w", err)
	}
	m.IngestedAt = time.Now().UTC()
	if err := store(ctx, enc.file, &m); err != nil {
		return Manifest{}, err
	}
	return m, nil
}

// DecodeObject reads a gzip NDJSON data object.
func DecodeObject(r io.Reader, fn func(Line) error) error {
	_, err := DecodeObjectCounted(r, fn)
	return err
}

// DecodeObjectCounted is DecodeObject plus the uncompressed byte count, so
// verification can check a manifest without buffering the whole object.
func DecodeObjectCounted(r io.Reader, fn func(Line) error) (int64, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return 0, err
	}
	defer gz.Close()
	counter := &countingReader{r: gz}
	dec := json.NewDecoder(bufio.NewReader(counter))
	dec.UseNumber()
	for {
		var line Line
		err := dec.Decode(&line)
		if err == io.EOF {
			return counter.n, nil
		}
		if err != nil {
			return counter.n, err
		}
		if err := fn(line); err != nil {
			return counter.n, err
		}
	}
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

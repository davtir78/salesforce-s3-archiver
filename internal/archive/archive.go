// Package archive writes collected Salesforce data to durable storage as
// gzip-compressed NDJSON objects, each accompanied by a manifest.
//
// A Sink.Write call only returns nil once both the data object and its
// manifest are durably stored. Callers must not advance any checkpoint or
// watermark before Write returns nil.
package archive

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	SourceStream   = "stream"
	SourceEventLog = "eventlog"
	SourceSOQL     = "soql"
	SourceLimits   = "limits"

	ManifestVersion = 1
	CollectorName   = "salesforce-s3-archiver"
)

// CollectorVersion is set at build time with -ldflags.
var CollectorVersion = "dev"

// Record is one archived event, log line or query row.
type Record struct {
	Type       string
	Timestamp  time.Time
	Attributes map[string]any
	// Opaque Pub/Sub replay ID, streams only.
	ReplayId []byte
}

// ObjectMeta describes the object being written.
type ObjectMeta struct {
	Source    string
	OrgId     string
	Instance  string
	EventType string
	// Time used for the year/month/day/hour partition.
	PartitionTime time.Time
	// Deterministic object name (without extension). When set, rewriting the
	// same logical input overwrites the same key, making processing idempotent.
	// When empty a unique name is generated so concurrent writers never collide.
	Name string
	// Free-form lineage stored in the manifest (topic, replay range, file ID...).
	Lineage map[string]string
}

// Manifest is stored next to (but outside of) the data prefix for every object.
type Manifest struct {
	ManifestVersion   int               `json:"manifestVersion"`
	Collector         string            `json:"collector"`
	CollectorVersion  string            `json:"collectorVersion"`
	Source            string            `json:"source"`
	OrgId             string            `json:"orgId"`
	Instance          string            `json:"instance"`
	EventType         string            `json:"eventType"`
	ObjectKey         string            `json:"objectKey"`
	RecordCount       int64             `json:"recordCount"`
	UncompressedBytes int64             `json:"uncompressedBytes"`
	CompressedBytes   int64             `json:"compressedBytes"`
	SHA256            string            `json:"sha256"`
	FirstTimestamp    *time.Time        `json:"firstTimestamp,omitempty"`
	LastTimestamp     *time.Time        `json:"lastTimestamp,omitempty"`
	IngestedAt        time.Time         `json:"ingestedAt"`
	Lineage           map[string]string `json:"lineage,omitempty"`
}

// RecordWriter receives records for a single object.
type RecordWriter interface {
	WriteRecord(r Record) error
}

// Sink stores objects. Write calls fill to stream records into the object and
// returns only after the object and manifest are durably stored.
type Sink interface {
	Write(ctx context.Context, meta ObjectMeta, fill func(w RecordWriter) error) (Manifest, error)
}

// WriteRecords is a convenience wrapper for in-memory record slices.
func WriteRecords(ctx context.Context, sink Sink, meta ObjectMeta, records []Record) (Manifest, error) {
	return sink.Write(ctx, meta, func(w RecordWriter) error {
		for _, r := range records {
			if err := w.WriteRecord(r); err != nil {
				return err
			}
		}
		return nil
	})
}

var unsafeKeyChars = regexp.MustCompile(`[^A-Za-z0-9_.\-]`)

func sanitize(s, fallback string) string {
	s = unsafeKeyChars.ReplaceAllString(strings.TrimSpace(s), "_")
	if s == "" {
		return fallback
	}
	return s
}

// UniqueName returns a collision-resistant object name.
func UniqueName(now time.Time) string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return fmt.Sprintf("%d-%s", now.UnixMilli(), hex.EncodeToString(b))
}

// ObjectPaths returns the data key and manifest key for meta, relative to the
// configured prefix.
func ObjectPaths(meta ObjectMeta, name string) (dataKey, manifestKey string) {
	t := meta.PartitionTime.UTC()
	partition := fmt.Sprintf("source=%s/org_id=%s/event_type=%s/year=%04d/month=%02d/day=%02d/hour=%02d",
		sanitize(meta.Source, "unknown"),
		sanitize(meta.OrgId, "unknown"),
		sanitize(meta.EventType, "unknown"),
		t.Year(), int(t.Month()), t.Day(), t.Hour(),
	)
	name = sanitize(name, "object")
	return "raw/" + partition + "/" + name + ".json.gz",
		"manifests/" + partition + "/" + name + ".manifest.json"
}

func joinPrefix(prefix, key string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return key
	}
	return prefix + "/" + key
}

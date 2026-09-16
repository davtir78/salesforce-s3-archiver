package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/archive"
)

func TestObjectStatsCompare(t *testing.T) {
	first := time.Date(2026, 9, 15, 1, 0, 0, 0, time.UTC)
	last := first.Add(time.Minute)
	good := objectStats{records: 3, compressed: 120, uncompressed: 300, sha256: "abc", first: &first, last: &last}
	manifest := archive.Manifest{RecordCount: 3, CompressedBytes: 120, UncompressedBytes: 300, SHA256: "abc",
		FirstTimestamp: &first, LastTimestamp: &last}

	if why := good.compare(manifest); why != "" {
		t.Errorf("matching object reported as mismatched: %s", why)
	}

	cases := map[string]func(*objectStats){
		"record count":  func(s *objectStats) { s.records = 4 },
		"compressed":    func(s *objectStats) { s.compressed = 121 },
		"uncompressed":  func(s *objectStats) { s.uncompressed = 301 },
		"sha256":        func(s *objectStats) { s.sha256 = "def" },
		"timestamps":    func(s *objectStats) { other := last.Add(time.Hour); s.last = &other },
		"missing times": func(s *objectStats) { s.first = nil },
	}
	for name, mutate := range cases {
		bad := good
		mutate(&bad)
		if why := bad.compare(manifest); why == "" {
			t.Errorf("%s difference was not detected", name)
		}
	}
}

func TestCompareLedger(t *testing.T) {
	g := compare([]string{"a", "b", "c"}, map[string]int{"a": 1, "b": 3})
	if g.Expected != 3 || g.Archived != 2 || g.Missing != 1 || g.Duplicates != 2 {
		t.Errorf("unexpected gap: %+v", g)
	}
	if len(g.Examples) != 1 || g.Examples[0] != "c" {
		t.Errorf("missing example should name the absent id: %v", g.Examples)
	}
	if empty := compare(nil, nil); empty.Missing != 0 || empty.Duplicates != 0 {
		t.Errorf("empty comparison should be clean: %+v", empty)
	}
}

func TestSameTime(t *testing.T) {
	a := time.Now().UTC()
	b := a
	if !sameTime(nil, nil) || !sameTime(&a, &b) {
		t.Error("equal times should compare equal")
	}
	c := a.Add(time.Second)
	if sameTime(&a, nil) || sameTime(nil, &a) || sameTime(&a, &c) {
		t.Error("different times should not compare equal")
	}
}

// The verifier must accept exactly what the writer produces, including records
// with zero timestamps (left out of the manifest range) and objects where every
// timestamp is zero (no range at all).
func TestScanObjectMatchesWriterManifest(t *testing.T) {
	sink := archive.NewMemorySink()
	now := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	meta := archive.ObjectMeta{Source: archive.SourceSOQL, OrgId: "00D", Instance: "test", EventType: "SetupAuditTrail", PartitionTime: now}
	batches := map[string][]archive.Record{
		"mixed": {
			{Type: "SetupAuditTrail", Timestamp: time.Time{}, Id: "a", Payload: map[string]any{"Id": "a"}},
			{Type: "SetupAuditTrail", Timestamp: now, Id: "b", Payload: map[string]any{"Id": "b"}},
			{Type: "SetupAuditTrail", Timestamp: now.Add(time.Minute), Id: "c", Payload: map[string]any{"Id": "c"}},
		},
		"all zero": {
			{Type: "SetupAuditTrail", Id: "d", Payload: map[string]any{"Id": "d"}},
			{Type: "SetupAuditTrail", Id: "e", Payload: map[string]any{"Id": "e"}},
		},
	}
	for name, records := range batches {
		meta.PartitionTime = meta.PartitionTime.Add(time.Hour) // distinct keys
		m, err := archive.WriteRecords(context.Background(), sink, meta, records)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		body, ok := sink.Objects()[m.ObjectKey]
		if !ok {
			t.Fatalf("%s: object %s not stored", name, m.ObjectKey)
		}
		var visited int
		stats, err := scanObject(bytes.NewReader(body), func(archive.Line) { visited++ })
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if why := stats.compare(m); why != "" {
			t.Errorf("%s: a consistent object failed verification: %s", name, why)
		}
		if visited != len(records) {
			t.Errorf("%s: visited %d records, want %d", name, visited, len(records))
		}
	}
}

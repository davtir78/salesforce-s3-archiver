package main

import (
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

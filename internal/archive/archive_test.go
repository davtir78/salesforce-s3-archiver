package archive

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestObjectPaths(t *testing.T) {
	meta := ObjectMeta{
		Source:        SourceStream,
		OrgId:         "00D000000000001",
		EventType:     "/event/Login Event",
		PartitionTime: time.Date(2026, 9, 11, 9, 30, 0, 0, time.FixedZone("AEST", 10*3600)),
	}
	data, manifest := ObjectPaths(meta, "abc")
	// Unsafe characters are replaced and a short hash of the original is
	// appended, so different values cannot share a partition.
	wantPrefix := "raw/source=stream/org_id=00D000000000001/event_type=_event_Login_Event-"
	if !strings.HasPrefix(data, wantPrefix) || !strings.HasSuffix(data, "/year=2026/month=09/day=10/hour=23/abc.json.gz") {
		t.Errorf("unexpected data key %s", data)
	}
	if !strings.HasPrefix(manifest, "manifests/source=stream/") || !strings.HasSuffix(manifest, "/abc.manifest.json") {
		t.Errorf("unexpected manifest key %s", manifest)
	}
	if joinPrefix("/archive/", data) != "archive/"+data {
		t.Errorf("prefix join failed")
	}
}

func TestUniqueNamesDoNotCollide(t *testing.T) {
	now := time.Now()
	seen := map[string]bool{}
	for i := 0; i < 10000; i++ {
		n := UniqueName(now)
		if seen[n] {
			t.Fatalf("duplicate name %s", n)
		}
		seen[n] = true
	}
}

func sampleRecords(n int) []Record {
	base := time.Date(2026, 9, 14, 1, 0, 0, 0, time.UTC)
	out := make([]Record, n)
	for i := range out {
		out[i] = Record{
			Type:      "LoginEventStream",
			Timestamp: base.Add(time.Duration(n-i) * time.Second),
			Payload:   map[string]any{"EventIdentifier": i, "Html": "<b>&</b>"},
			ReplayId:  []byte{0, 0, 0, byte(i)},
		}
	}
	return out
}

func TestLocalSinkRoundTrip(t *testing.T) {
	dir := t.TempDir()
	sink, err := NewLocalSink(dir, "pfx")
	if err != nil {
		t.Fatal(err)
	}
	meta := ObjectMeta{Source: SourceStream, OrgId: "org", Instance: "prod", EventType: "LoginEventStream",
		Lineage: map[string]string{"topic": "/event/LoginEventStream"}}
	records := sampleRecords(250)
	m, err := WriteRecords(context.Background(), sink, meta, records)
	if err != nil {
		t.Fatal(err)
	}
	if m.RecordCount != 250 {
		t.Errorf("record count = %d", m.RecordCount)
	}
	if !m.FirstTimestamp.Before(*m.LastTimestamp) {
		t.Errorf("first/last timestamps wrong: %v %v", m.FirstTimestamp, m.LastTimestamp)
	}

	dataPath := filepath.Join(dir, filepath.FromSlash(m.ObjectKey))
	b, err := os.ReadFile(dataPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	if hex.EncodeToString(sum[:]) != m.SHA256 || int64(len(b)) != m.CompressedBytes {
		t.Errorf("manifest checksum/size does not match stored object")
	}

	manifestKey := strings.Replace(strings.Replace(m.ObjectKey, "/raw/", "/manifests/", 1), ".json.gz", ".manifest.json", 1)
	mb, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(manifestKey)))
	if err != nil {
		t.Fatalf("manifest not stored: %v", err)
	}
	var stored Manifest
	if err := json.Unmarshal(mb, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.SHA256 != m.SHA256 || stored.Lineage["topic"] != "/event/LoginEventStream" {
		t.Errorf("stored manifest mismatch: %+v", stored)
	}

	f, _ := os.Open(dataPath)
	defer f.Close()
	var lines []Line
	if err := DecodeObject(f, func(l Line) error { lines = append(lines, l); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(lines) != 250 || lines[3].ReplayId == "" || lines[3].OrgId != "org" || lines[3].Payload["Html"] != "<b>&</b>" {
		t.Errorf("decoded lines wrong: %d %+v", len(lines), lines[3])
	}

	// No temp files should be left behind next to the data.
	entries, _ := os.ReadDir(filepath.Dir(dataPath))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".tmp-") {
			t.Errorf("temp file left behind: %s", e.Name())
		}
	}
}

func TestDeterministicNameOverwrites(t *testing.T) {
	sink := NewMemorySink()
	meta := ObjectMeta{Source: SourceEventLog, OrgId: "org", EventType: "Login", Name: "elf-0AT000000000001",
		PartitionTime: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)}
	for i := 0; i < 3; i++ {
		if _, err := WriteRecords(context.Background(), sink, meta, sampleRecords(5)); err != nil {
			t.Fatal(err)
		}
	}
	if keys := sink.Keys(); len(keys) != 1 {
		t.Errorf("expected one key after idempotent rewrites, got %v", keys)
	}
	meta.Name = ""
	for i := 0; i < 3; i++ {
		if _, err := WriteRecords(context.Background(), sink, meta, sampleRecords(5)); err != nil {
			t.Fatal(err)
		}
	}
	if keys := sink.Keys(); len(keys) != 4 {
		t.Errorf("expected unique keys for unnamed objects, got %d", len(keys))
	}
}

func TestFillErrorStoresNothing(t *testing.T) {
	sink := NewMemorySink()
	_, err := sink.Write(context.Background(), ObjectMeta{Source: SourceSOQL}, func(w RecordWriter) error {
		_ = w.WriteRecord(Record{Type: "x", Timestamp: time.Now()})
		return errors.New("boom")
	})
	if err == nil || len(sink.Keys()) != 0 {
		t.Errorf("expected error and no stored objects, got err=%v keys=%v", err, sink.Keys())
	}
}

func TestS3UploadTimesOutOnHungConnection(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // never answers until the test ends
	}))
	defer srv.Close()
	defer close(release)

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	sink, err := NewS3Sink(context.Background(), S3Options{
		Bucket: "b", Endpoint: srv.URL, ForcePathStyle: true, Region: "us-east-1",
		MaxAttempts: 1, UploadTimeout: 300 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = WriteRecords(context.Background(), sink, ObjectMeta{Source: SourceStream}, sampleRecords(3))
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("upload did not time out promptly: %s", elapsed)
	}
}

// The envelope is the contract downstream tools and Athena tables read.
func TestLineEnvelope(t *testing.T) {
	sink := NewMemorySink()
	meta := ObjectMeta{Source: SourceStream, OrgId: "00D5j0", Instance: "myorg-prod", Env: "prod",
		EventType: "LoginEventStream"}
	_, err := WriteRecords(context.Background(), sink, meta, []Record{{
		Type:      "LoginEventStream",
		Id:        "9b2c7f4e",
		Timestamp: time.Date(2026, 9, 15, 1, 2, 3, 4_000_000, time.UTC),
		Payload:   map[string]any{"EventUuid": "9b2c7f4e", "SourceIp": "10.0.4.7"},
		ReplayId:  []byte{0, 0, 2, 98},
	}})
	if err != nil {
		t.Fatal(err)
	}
	raw := sink.Objects()[sink.Keys()[0]]
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	line, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(line, &got); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	want := map[string]any{
		"event_id": "9b2c7f4e", "event_type": "LoginEventStream",
		"timestamp": "2026-09-15T01:02:03.004Z", "source": "stream", "env": "prod",
		"org_id": "00D5j0", "instance": "myorg-prod", "replay_id": "AAACYg==",
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%s = %#v, want %#v", k, got[k], w)
		}
	}
	payload, ok := got["payload"].(map[string]any)
	if !ok || payload["SourceIp"] != "10.0.4.7" {
		t.Errorf("payload not preserved: %#v", got["payload"])
	}
	if len(got) != len(want)+1 {
		t.Errorf("unexpected envelope fields: %v", got)
	}
}

func TestSanitisingDoesNotMergeValues(t *testing.T) {
	seen := map[string]string{}
	for _, eventType := range []string{"Login Event", "Login/Event", "Login_Event", "Login+Event"} {
		meta := ObjectMeta{Source: SourceStream, OrgId: "org", EventType: eventType,
			PartitionTime: time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)}
		key, _ := ObjectPaths(meta, "obj")
		if other, clash := seen[key]; clash {
			t.Errorf("%q and %q map to the same key %s", other, eventType, key)
		}
		seen[key] = eventType
	}
}

func TestZeroTimestampsAreNotCounted(t *testing.T) {
	sink := NewMemorySink()
	valid := time.Date(2026, 9, 15, 1, 0, 0, 0, time.UTC)
	m, err := WriteRecords(context.Background(), sink, ObjectMeta{Source: SourceSOQL, OrgId: "org"}, []Record{
		{Type: "T", Timestamp: time.Time{}, Payload: map[string]any{"a": 1}},
		{Type: "T", Timestamp: valid, Payload: map[string]any{"a": 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if m.RecordCount != 2 {
		t.Errorf("both records should be archived, got %d", m.RecordCount)
	}
	if m.FirstTimestamp == nil || !m.FirstTimestamp.Equal(valid) || !m.LastTimestamp.Equal(valid) {
		t.Errorf("zero timestamps must not widen the manifest range: %v..%v", m.FirstTimestamp, m.LastTimestamp)
	}
}

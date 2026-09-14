package eventlog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/archive"
	"github.com/davtir78/salesforce-s3-archiver/internal/cache"
	"github.com/davtir78/salesforce-s3-archiver/internal/config"
	"github.com/davtir78/salesforce-s3-archiver/internal/mocksf"
)

type elfHarness struct {
	t     *testing.T
	mock  *mocksf.Server
	conf  *config.EventLogConfig
	db    *cache.MemoryCache
	sink  *archive.MemorySink
	c     *Collector
	dlDir string
}

func newELFHarness(t *testing.T, opts mocksf.Options) *elfHarness {
	t.Helper()
	if opts.QueryPageSize == 0 {
		opts.QueryPageSize = 3 // force pagination
	}
	mock, httpURL, _, err := mocksf.Start(opts, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mock.Stop)
	o := mock.Options()
	conf := &config.EventLogConfig{
		Name:           "test",
		ApiVer:         "64.0",
		RequestTimeout: 10,
		Auth: config.AuthConfig{
			TokenUrl:   httpURL,
			ClientCred: &config.ClientCredAuth{ClientId: o.ClientId, ClientSecret: o.ClientSecret},
		},
		InitialTimeInterval: config.TimeIntervalConfig{Hours: 2},
		Limits:              config.LimitsConfig{ApiVer: "64.0"},
		SkipLimits:          true,
	}
	db := cache.NewMemoryCache()
	sink := archive.NewMemorySink()
	c := NewCollector(conf, o.OrgId, db, sink)
	c.DownloadDir = t.TempDir()
	return &elfHarness{t: t, mock: mock, conf: conf, db: db, sink: sink, c: c, dlDir: c.DownloadDir}
}

func (h *elfHarness) archivedRequestIds() map[string]int {
	lines, err := h.sink.Lines()
	if err != nil {
		h.t.Fatal(err)
	}
	counts := map[string]int{}
	for _, l := range lines {
		if l.Source == archive.SourceEventLog {
			id, _ := l.Attributes["REQUEST_ID"].(string)
			counts[id]++
		}
	}
	return counts
}

func (h *elfHarness) missingRows() int {
	counts := h.archivedRequestIds()
	missing := 0
	for _, ids := range h.mock.Ledger().EventLogFiles {
		for _, id := range ids {
			if counts[id] == 0 {
				missing++
			}
		}
	}
	return missing
}

func (h *elfHarness) assertNoTempFiles() {
	h.t.Helper()
	entries, _ := os.ReadDir(h.dlDir)
	if len(entries) != 0 {
		h.t.Errorf("downloaded CSV files left behind: %d", len(entries))
	}
}

func TestArchivesEventLogFilesAcrossPages(t *testing.T) {
	h := newELFHarness(t, mocksf.Options{EventLogRows: 40})
	now := time.Now()
	for i := 0; i < 10; i++ {
		h.mock.AddEventLogFile([]string{"Login", "API", "URI"}[i%3], now.Add(-time.Duration(10-i)*time.Minute), 40)
	}
	if err := h.c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m := h.missingRows(); m != 0 {
		t.Fatalf("%d rows missing", m)
	}
	if keys := h.sink.Keys(); len(keys) != 10 {
		t.Errorf("expected 10 objects (one per file), got %d", len(keys))
	}
	for _, k := range h.sink.Keys() {
		if !strings.Contains(k, "/elf-0AT") {
			t.Errorf("EventLogFile objects must use deterministic names: %s", k)
		}
		m, _ := h.sink.Manifest(k)
		if m.RecordCount != 40 || m.Lineage["eventLogFileId"] == "" {
			t.Errorf("bad manifest %+v", m)
		}
	}
	lines, _ := h.sink.Lines()
	if v := lines[0].Attributes["LOGIN_TYPE"]; v != `Application, with "quotes"` {
		t.Errorf("CSV quoting not preserved: %q", v)
	}
	if _, ok := lines[0].Attributes["RUN_TIME"].(string); !ok {
		t.Errorf("values must be kept as original strings")
	}
	h.assertNoTempFiles()

	// Watermark is the newest CreatedDate; a second poll archives nothing new.
	if _, ok := h.c.watermark(h.c.logFilesWatermarkKey()); !ok {
		t.Fatal("watermark not stored")
	}
	if err := h.c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if keys := h.sink.Keys(); len(keys) != 10 {
		t.Errorf("re-poll created new objects: %d", len(keys))
	}

	// Losing the processed-file cache reprocesses files into the same keys.
	h.db.Clear()
	if err := h.c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if keys := h.sink.Keys(); len(keys) != 10 {
		t.Errorf("reprocessing after cache loss must be idempotent, got %d objects", len(keys))
	}
}

func TestEventLogFailuresDoNotAdvanceWatermark(t *testing.T) {
	h := newELFHarness(t, mocksf.Options{EventLogRows: 20})
	base := time.Now().Add(-30 * time.Minute)
	for i := 0; i < 6; i++ {
		h.mock.AddEventLogFile("Login", base.Add(time.Duration(i)*time.Minute), 20)
	}

	// 1. The first download is truncated: nothing archived, no watermark.
	h.mock.SetFaults(mocksf.Faults{TruncateNextDownload: 10})
	err := h.c.Poll(context.Background())
	if err == nil {
		t.Fatal("expected a download error")
	}
	if len(h.sink.Keys()) != 0 {
		t.Errorf("nothing should be archived after the first file failed")
	}
	if _, ok := h.c.watermark(h.c.logFilesWatermarkKey()); ok {
		t.Errorf("watermark must not be set when the first file failed")
	}
	h.assertNoTempFiles()

	// 2. Archive write fails on the 4th object: 3 archived, watermark at file 3.
	h.sink.BeforeStore = func(_ archive.ObjectMeta, n int) error {
		if n == 4 {
			return archive.ErrInjected
		}
		return nil
	}
	if err := h.c.Poll(context.Background()); !errors.Is(err, archive.ErrInjected) {
		t.Fatalf("expected injected archive error, got %v", err)
	}
	if n := len(h.sink.Keys()); n != 3 {
		t.Errorf("expected 3 archived files before the failure, got %d", n)
	}
	wm, ok := h.c.watermark(h.c.logFilesWatermarkKey())
	if !ok || !wm.Before(base.Add(3*time.Minute)) {
		t.Errorf("watermark %v advanced past the failed file", wm)
	}

	// 3. REST failure while listing.
	h.sink.BeforeStore = nil
	h.mock.SetFaults(mocksf.Faults{FailRestRequests: 1})
	if err := h.c.Poll(context.Background()); err == nil {
		t.Errorf("expected listing error")
	}

	// 4. Recovery: everything is archived, nothing lost.
	if err := h.c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m := h.missingRows(); m != 0 {
		t.Errorf("%d rows lost", m)
	}
	h.assertNoTempFiles()
}

// Regression test for the upstream file descriptor leak: CSV files were opened
// and never closed. On Windows an open file cannot be deleted, so leftover
// files also prove the handle was not released.
func TestCsvFilesAreClosed(t *testing.T) {
	h := newELFHarness(t, mocksf.Options{})
	for i := 0; i < 30; i++ {
		h.mock.AddEventLogFile("URI", time.Now().Add(-time.Duration(i)*time.Second), 5)
	}
	before := openFileCount()
	if err := h.c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.assertNoTempFiles()
	if after := openFileCount(); before >= 0 && after > before+5 {
		t.Errorf("open file descriptors grew from %d to %d", before, after)
	}

	// streamCsv closes the file even when the writer fails mid-file.
	path := filepath.Join(t.TempDir(), "x.csv")
	os.WriteFile(path, []byte("EVENT_TYPE,REQUEST_ID\nLogin,a\nLogin,b\n"), 0o600)
	err := streamCsv(path, "Login", nil, failingWriter{})
	if err == nil {
		t.Fatal("expected writer error")
	}
	if err := os.Remove(path); err != nil {
		t.Errorf("file still open after streamCsv error: %v", err)
	}
}

type failingWriter struct{}

func (failingWriter) WriteRecord(archive.Record) error { return errors.New("boom") }

func openFileCount() int {
	if runtime.GOOS != "linux" {
		return -1
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}

func TestMalformedCsvFailsTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.csv")
	os.WriteFile(path, []byte("EVENT_TYPE,REQUEST_ID\nLogin,a,extra\n"), 0o600)
	if err := streamCsv(path, "Login", nil, recordCollector(func(archive.Record) {})); err == nil {
		t.Errorf("rows with the wrong field count must fail rather than be skipped")
	}
}

func TestCustomQueriesPaginateDedupeAndRetry(t *testing.T) {
	h := newELFHarness(t, mocksf.Options{})
	h.conf.SkipLogFiles = true
	h.conf.RecordsPerObject = 4
	h.conf.CustomQueries = []config.QueryConfig{{
		Soql:      config.SoqlConfig{Select: []string{"Id", "Action", "CreatedDate"}, From: "SetupAuditTrail"},
		ApiVer:    "64.0",
		ApiName:   "rest",
		Timestamp: "CreatedDate",
	}}
	h.mock.AddCustomRecords("SetupAuditTrail", time.Now().Add(-10*time.Minute), 11)

	if err := h.c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	countIds := func() (map[string]int, int) {
		lines, _ := h.sink.Lines()
		m := map[string]int{}
		for _, l := range lines {
			id, _ := l.Attributes["Id"].(string)
			m[id]++
		}
		return m, len(lines)
	}
	ids, total := countIds()
	if len(ids) != 11 || total != 11 {
		t.Fatalf("expected 11 unique rows, got %d (%d lines)", len(ids), total)
	}
	if n := len(h.sink.Keys()); n != 3 {
		t.Errorf("expected 3 objects with recordsPerObject=4, got %d", n)
	}

	// Overlapping poll: unchanged rows are not archived again.
	if err := h.c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, total := countIds(); total != 11 {
		t.Errorf("overlapping poll duplicated rows: %d lines", total)
	}

	// A failed query leaves the watermark alone; new rows arrive next poll.
	key := h.c.queryWatermarkKey(&h.conf.CustomQueries[0])
	wmBefore, _ := h.c.watermark(key)
	h.mock.AddCustomRecords("SetupAuditTrail", time.Now().Add(-2*time.Second), 5)
	h.mock.SetFaults(mocksf.Faults{FailRestRequests: 1})
	if err := h.c.Poll(context.Background()); err == nil {
		t.Fatal("expected query failure")
	}
	if wmAfter, _ := h.c.watermark(key); !wmAfter.Equal(wmBefore) {
		t.Errorf("watermark moved after a failed query")
	}
	if err := h.c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	ids, total = countIds()
	if len(ids) != 16 || total != 16 {
		t.Errorf("expected 16 unique rows after recovery, got %d unique / %d lines", len(ids), total)
	}
	lines, _ := h.sink.Lines()
	if _, ok := lines[0].Attributes["attributes"]; ok {
		t.Errorf("salesforce 'attributes' metadata should be removed")
	}
}

func TestLimitsArchived(t *testing.T) {
	h := newELFHarness(t, mocksf.Options{})
	h.conf.SkipLogFiles = true
	h.conf.SkipLimits = false
	h.conf.Limits.Names = []string{"DailyApiRequests"}
	if err := h.c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	lines, _ := h.sink.Lines()
	if len(lines) != 1 || lines[0].Source != archive.SourceLimits || lines[0].Attributes["limitName"] != "DailyApiRequests" {
		t.Errorf("unexpected limits output: %+v", lines)
	}
}

func TestBuildCsvRecordFromSample(t *testing.T) {
	_, filename, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(filename), "testdata", "login_logs_sample.csv")
	var records []archive.Record
	w := recordCollector(func(r archive.Record) { records = append(records, r) })
	if err := streamCsv(path, "Login", nil, w); err != nil {
		t.Fatal(err)
	}
	if len(records) != 9 {
		t.Fatalf("expected 9 rows, got %d", len(records))
	}
	r := records[0]
	want := time.Date(2025, 10, 1, 11, 0, 3, 509*1_000_000, time.UTC)
	if !r.Timestamp.Equal(want) || r.Type != "Login" {
		t.Errorf("timestamp/type = %v %s", r.Timestamp, r.Type)
	}
	if r.Attributes["URI"] != "/services/oauth2/token" || r.Attributes["RUN_TIME"] != "157" || len(r.Attributes) != 34 {
		t.Errorf("unexpected attributes (%d): %v", len(r.Attributes), r.Attributes)
	}

	// Field mapping keeps mapped fields plus identifying columns.
	records = nil
	if err := streamCsv(path, "Login", FieldMapping{"URI": true}, w); err != nil {
		t.Fatal(err)
	}
	if got := len(records[0].Attributes); got != 4 {
		t.Errorf("mapped record should have URI, EVENT_TYPE, TIMESTAMP, REQUEST_ID; got %v", records[0].Attributes)
	}
}

type recordCollector func(archive.Record)

func (f recordCollector) WriteRecord(r archive.Record) error { f(r); return nil }

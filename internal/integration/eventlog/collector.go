package eventlog

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/archive"
	"github.com/davtir78/salesforce-s3-archiver/internal/cache"
	"github.com/davtir78/salesforce-s3-archiver/internal/config"
	"github.com/davtir78/salesforce-s3-archiver/internal/integration/eventlog/query"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/davtir78/salesforce-s3-archiver/internal/metrics"
)

const (
	LogDateFormat = "2006-01-02T15:04:05.999999-0700"

	defaultOverlap          = 60 * time.Minute
	defaultRecordsPerObject = 10000
)

type FieldMapping = map[string]bool

// Collector archives EventLogFiles, custom SOQL query results and org limits.
//
// Watermarks only move past data that has been durably archived. EventLogFile
// objects use deterministic keys, so reprocessing a file (e.g. after losing the
// processed-file cache) overwrites the same object instead of duplicating it.
type Collector struct {
	conf  *config.EventLogConfig
	orgId string
	env   string
	db    cache.Cache
	sink  archive.Sink
	// Directory for downloaded CSV files (default os.TempDir()).
	DownloadDir string
	now         func() time.Time
}

func NewCollector(conf *config.EventLogConfig, orgId, env string, db cache.Cache, sink archive.Sink) *Collector {
	return &Collector{conf: conf, orgId: orgId, env: env, db: db, sink: sink, now: time.Now}
}

// Poll runs one collection cycle. It returns an error if any part failed; the
// affected watermarks are left where they were so the next poll retries.
func (c *Collector) Poll(ctx context.Context) error {
	var errs []error
	if !c.conf.SkipLogFiles {
		if err := c.collectLogFiles(ctx); err != nil {
			errs = append(errs, fmt.Errorf("event log files: %w", err))
		}
	}
	for i := range c.conf.CustomQueries {
		if err := c.collectCustomQuery(ctx, &c.conf.CustomQueries[i]); err != nil {
			errs = append(errs, fmt.Errorf("custom query on %s: %w", c.conf.CustomQueries[i].Soql.From, err))
		}
	}
	if !c.conf.SkipLimits {
		if err := c.collectLimits(ctx); err != nil {
			errs = append(errs, fmt.Errorf("limits: %w", err))
		}
	}
	if len(errs) == 0 {
		metrics.LastSuccessfulPoll.SetToCurrentTime()
	}
	return errors.Join(errs...)
}

func (c *Collector) overlap() time.Duration {
	if c.conf.WatermarkOverlapMinutes > 0 {
		return time.Duration(c.conf.WatermarkOverlapMinutes) * time.Minute
	}
	return defaultOverlap
}

// since returns the query start for a watermark key: watermark minus overlap,
// or the initial interval when no watermark exists.
func (c *Collector) since(key string) (time.Time, *time.Time, error) {
	wm, ok, err := c.watermark(key)
	if err != nil {
		return time.Time{}, nil, err
	}
	if ok {
		return wm.Add(-c.overlap()), &wm, nil
	}
	interval := c.conf.InitialTimeInterval
	d := time.Duration(interval.Hours)*time.Hour + time.Duration(interval.Minutes)*time.Minute
	if d == 0 {
		d = time.Hour
	}
	return c.now().Add(-d), nil, nil
}

// watermark returns the stored watermark. A cache read error is returned
// rather than treated as "no watermark": falling back to the initial interval
// would move the watermark forward past records that were never read.
func (c *Collector) watermark(key string) (time.Time, bool, error) {
	val, err := c.db.GetCacheVal(key)
	if err != nil {
		metrics.EventLogFailures.WithLabelValues("watermark").Inc()
		return time.Time{}, false, fmt.Errorf("reading watermark '%s': %w", key, err)
	}
	s, ok := val.(string)
	if !ok || s == "" {
		return time.Time{}, false, nil
	}
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("invalid watermark '%s' = %q: fix or delete the key", key, s)
	}
	return time.UnixMilli(ms), true, nil
}

func (c *Collector) setWatermark(key string, t time.Time) {
	if err := cache.SetPersistent(c.db, key, strconv.FormatInt(t.UnixMilli(), 10)); err != nil {
		log.Warnf("Error storing watermark '%s' (data is archived; next poll may re-read it): %v", key, err)
		return
	}
	metrics.Watermark.WithLabelValues(key).Set(float64(t.UnixMilli()) / 1000)
}

func (c *Collector) logFilesWatermarkKey() string {
	return c.conf.Name + "_last_run_ts"
}

func (c *Collector) processedKey(id string) string {
	return c.conf.Name + "_elf_" + id
}

func (c *Collector) isProcessed(key string) bool {
	val, err := c.db.GetCacheVal(key)
	if err != nil {
		log.Warnf("Error reading processed marker (reprocessing is idempotent): %v", err)
		return false
	}
	return val != nil
}

func (c *Collector) markProcessed(key string) {
	if err := c.db.SetCacheVal(key, "1"); err != nil {
		log.Warnf("Error storing processed marker '%s': %v", key, err)
	}
}

func (c *Collector) collectLogFiles(ctx context.Context) error {
	key := c.logFilesWatermarkKey()
	since, current, err := c.since(key)
	if err != nil {
		return err
	}
	// SOQL datetimes have second precision; use the same precision for the
	// upper bound and the watermark so records in the boundary second are
	// not excluded by the query yet covered by the watermark.
	until := c.now().Truncate(time.Second)

	records, err := query.RequestLogFiles(ctx, c.conf, c.db, since, until)
	if err != nil {
		metrics.EventLogFailures.WithLabelValues("list").Inc()
		return err
	}
	log.Infof("Found %d EventLogFile records since %s", len(records), since.UTC().Format(time.RFC3339))

	var newWatermark time.Time
	if current != nil {
		newWatermark = *current
	}
	archived := 0
	for i := range records {
		rec := &records[i]
		created, err := parseSFDate(rec.CreatedDate)
		if err != nil {
			metrics.EventLogFailures.WithLabelValues("parse").Inc()
			return fmt.Errorf("EventLogFile %s has invalid CreatedDate %q: %w", rec.Id, rec.CreatedDate, err)
		}
		pkey := c.processedKey(rec.Id)
		if !c.isProcessed(pkey) {
			if err := c.archiveLogFile(ctx, rec, created); err != nil {
				// Stop here: never move the watermark past a file that failed.
				if newWatermark.After(time.Time{}) {
					c.setWatermark(key, newWatermark)
				}
				return fmt.Errorf("EventLogFile %s (%s): %w", rec.Id, rec.EventType, err)
			}
			c.markProcessed(pkey)
			archived++
		}
		if created.After(newWatermark) {
			newWatermark = created
		}
	}
	if newWatermark.After(time.Time{}) {
		c.setWatermark(key, newWatermark)
	}
	if archived > 0 {
		log.Infof("Archived %d EventLogFile(s)", archived)
	}
	return nil
}

func (c *Collector) archiveLogFile(ctx context.Context, rec *query.EventLogfileRecord, created time.Time) error {
	path, err := query.DownloadCsvFile(ctx, c.conf, c.db, rec, c.DownloadDir)
	if err != nil {
		metrics.EventLogFailures.WithLabelValues("download").Inc()
		return err
	}
	defer func() {
		if err := os.Remove(path); err != nil {
			log.Warnf("Error deleting CSV file %s: %v", path, err)
		}
	}()

	// The partition must be stable across reprocessing, otherwise elf-<Id>
	// would land under a different key and no longer be idempotent.
	logDate, err := parseSFDate(rec.LogDate)
	if err != nil {
		log.Warnf("EventLogFile %s has unparseable LogDate %q; partitioning by CreatedDate", rec.Id, rec.LogDate)
		logDate = created
	}
	meta := archive.ObjectMeta{
		Source:        archive.SourceEventLog,
		OrgId:         c.orgId,
		Env:           c.env,
		Instance:      c.conf.Name,
		EventType:     rec.EventType,
		PartitionTime: logDate,
		// Deterministic: reprocessing overwrites the same object.
		Name: "elf-" + rec.Id,
		Lineage: map[string]string{
			"eventLogFileId": rec.Id,
			"logDate":        rec.LogDate,
			"createdDate":    rec.CreatedDate,
			"interval":       rec.Interval,
			"sequence":       strconv.Itoa(rec.Sequence),
		},
	}
	mapping := c.fieldMapping(rec.EventType)
	m, err := c.sink.Write(ctx, meta, func(w archive.RecordWriter) error {
		return streamCsv(path, rec.Id, rec.EventType, mapping, w)
	})
	var malformed *malformedCsvError
	if errors.As(err, &malformed) {
		return c.handleMalformed(ctx, rec, meta, path, malformed)
	}
	if err != nil {
		metrics.EventLogFailures.WithLabelValues("archive").Inc()
		metrics.UploadFailures.WithLabelValues(archive.SourceEventLog).Inc()
		return err
	}
	metrics.ObjectsArchived.WithLabelValues(archive.SourceEventLog).Inc()
	metrics.RecordsArchived.WithLabelValues(archive.SourceEventLog, rec.EventType).Add(float64(m.RecordCount))
	metrics.EventLogFilesArchived.WithLabelValues(rec.EventType).Inc()
	log.Debugf("Archived EventLogFile %s: %d rows to %s", rec.Id, m.RecordCount, m.ObjectKey)
	return nil
}

func (c *Collector) fieldMapping(eventType string) FieldMapping {
	// NOTE: viper's mapstructure lowercases all map keys.
	fields, ok := c.conf.FieldMapping[strings.ToLower(eventType)]
	if !ok || len(fields) == 0 {
		return nil
	}
	m := FieldMapping{}
	for _, f := range fields {
		m[f] = true
	}
	return m
}

// streamCsv reads a CSV file and writes each row as a record. The file is
// always closed (the upstream exporter leaked the descriptor).
func streamCsv(path, fileId, eventType string, mapping FieldMapping, w archive.RecordWriter) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := csv.NewReader(bufio.NewReaderSize(f, 256*1024))
	reader.ReuseRecord = true
	header, err := reader.Read()
	if err == io.EOF {
		return nil // empty file
	}
	if err != nil {
		return &malformedCsvError{line: 1, err: err}
	}
	labels := append([]string(nil), header...)
	reader.FieldsPerRecord = len(labels)

	for line := 2; ; line++ {
		row, err := reader.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return &malformedCsvError{line: line, err: err}
		}
		if err := w.WriteRecord(buildCsvRecord(labels, row, fileId, line, eventType, mapping)); err != nil {
			return err
		}
	}
}

// buildCsvRecord keeps every column value as the original string (archival
// fidelity; the upstream exporter converted numbers and truncated strings).
func buildCsvRecord(labels, row []string, fileId string, line int, eventType string, mapping FieldMapping) archive.Record {
	attrs := make(map[string]any, len(labels))
	ts := time.Time{}
	for i, label := range labels {
		value := row[i]
		switch label {
		case "EVENT_TYPE":
			if value != "" {
				eventType = value
			}
		case "TIMESTAMP_DERIVED":
			if t, err := time.Parse(time.RFC3339Nano, value); err == nil && ts.IsZero() {
				ts = t
			}
		case "TIMESTAMP":
			if t, err := time.Parse("20060102150405.999999", value); err == nil {
				ts = t
			}
		}
		if mapping != nil && !mapping[label] && label != "EVENT_TYPE" && label != "TIMESTAMP" && label != "REQUEST_ID" {
			continue
		}
		attrs[label] = value
	}
	if ts.IsZero() {
		ts = time.Now()
	}
	return archive.Record{Type: eventType, Id: csvRowId(fileId, line), Timestamp: ts.UTC(), Payload: attrs}
}

// csvRowId identifies one row of one EventLogFile. Rows have no unique field of
// their own (REQUEST_ID repeats across the rows of a request), so the file ID
// and line number are hashed into a stable ID that survives reprocessing.
func csvRowId(fileId string, line int) string {
	if fileId == "" {
		return ""
	}
	sum := sha256.Sum256(fmt.Appendf(nil, "%s|%d", fileId, line))
	return hex.EncodeToString(sum[:16])
}

func (c *Collector) queryWatermarkKey(q *config.QueryConfig) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%v|%s|%s|%s|%s|%s", q.Soql.From, q.Soql.Select, q.Soql.Where, q.Soql.Tail, q.Timestamp, q.EndTimestamp, q.ApiName)
	return c.conf.Name + "_query_" + hex.EncodeToString(h.Sum(nil))[:16] + "_last_run_ts"
}

func (c *Collector) collectCustomQuery(ctx context.Context, q *config.QueryConfig) error {
	key := c.queryWatermarkKey(q)
	since, _, err := c.since(key)
	if err != nil {
		return err
	}
	// SOQL datetimes have second precision; use the same precision for the
	// upper bound and the watermark so records in the boundary second are
	// not excluded by the query yet covered by the watermark.
	until := c.now().Truncate(time.Second)

	rows, err := query.RequestCustomQuery(ctx, q, c.conf, c.db, since, until)
	if err != nil {
		metrics.EventLogFailures.WithLabelValues("query").Inc()
		return err
	}

	perObject := c.conf.RecordsPerObject
	if perObject <= 0 {
		perObject = defaultRecordsPerObject
	}

	var pending []archive.Record
	var pendingKeys []string
	skipped := 0
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		meta := archive.ObjectMeta{
			Source:        archive.SourceSOQL,
			OrgId:         c.orgId,
			Env:           c.env,
			Instance:      c.conf.Name,
			EventType:     pending[0].Type,
			PartitionTime: until,
			Lineage: map[string]string{
				"from":  q.Soql.From,
				"since": since.UTC().Format(time.RFC3339),
				"until": until.UTC().Format(time.RFC3339),
			},
		}
		m, err := archive.WriteRecords(ctx, c.sink, meta, pending)
		if err != nil {
			metrics.EventLogFailures.WithLabelValues("archive").Inc()
			metrics.UploadFailures.WithLabelValues(archive.SourceSOQL).Inc()
			return err
		}
		metrics.ObjectsArchived.WithLabelValues(archive.SourceSOQL).Inc()
		metrics.RecordsArchived.WithLabelValues(archive.SourceSOQL, q.Soql.From).Add(float64(m.RecordCount))
		// Only mark rows as seen once they are durable.
		for _, k := range pendingKeys {
			c.markProcessed(k)
		}
		pending, pendingKeys = nil, nil
		return nil
	}

	for _, row := range rows {
		dedupKey := c.rowDedupKey(q, row)
		if dedupKey != "" && c.isProcessed(dedupKey) {
			skipped++
			continue
		}
		rec := buildCustomRecord(row, q)
		pending = append(pending, rec)
		if dedupKey != "" {
			pendingKeys = append(pendingKeys, dedupKey)
		}
		if len(pending) >= perObject {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	c.setWatermark(key, until)
	log.Debugf("Custom query on %s: %d rows, %d already archived", q.Soql.From, len(rows), skipped)
	return nil
}

// rowDedupKey identifies a row version: record Id (or custom ID fields) plus
// the query timestamp value, so updated records are archived again while
// overlapping polls do not duplicate unchanged rows.
func (c *Collector) rowDedupKey(q *config.QueryConfig, row map[string]any) string {
	id, _ := row["Id"].(string)
	if id == "" {
		id = buildCustomId(row, q)
	}
	if id == "" {
		return ""
	}
	return c.conf.Name + "_soql_" + q.Soql.From + "_" + id + "_" + fmt.Sprint(row[q.Timestamp])
}

func buildCustomRecord(row map[string]any, q *config.QueryConfig) archive.Record {
	attrs := make(map[string]any, len(row))
	for k, v := range row {
		attrs[k] = v
	}
	eventType := q.Soql.From
	if a, ok := row["attributes"].(map[string]any); ok {
		if t, ok := a["type"].(string); ok && t != "" {
			eventType = t
		}
	}
	delete(attrs, "attributes")
	recordId, _ := row["Id"].(string)
	if recordId == "" {
		recordId = buildCustomId(row, q)
	}
	ts := time.Now()
	if s, ok := row[q.Timestamp].(string); ok {
		if t, err := parseSFDate(s); err == nil {
			ts = t
		}
	}
	return archive.Record{Type: eventType, Id: recordId, Timestamp: ts.UTC(), Payload: attrs}
}

func buildCustomId(record map[string]any, customQuery *config.QueryConfig) string {
	if len(customQuery.CustomId) == 0 {
		return ""
	}
	hashVal := sha256.New()
	for _, fieldName := range customQuery.CustomId {
		fieldVal, ok := record[fieldName]
		if !ok {
			log.Warnf("Custom ID field '%s' is not present in the event of type '%s'.", fieldName, customQuery.Soql.From)
			return ""
		}
		fmt.Fprintf(hashVal, "%v", fieldVal)
	}
	return hex.EncodeToString(hashVal.Sum(nil))
}

func (c *Collector) collectLimits(ctx context.Context) error {
	limits, err := query.RequestLimits(ctx, c.conf, c.db)
	if err != nil {
		metrics.EventLogFailures.WithLabelValues("limits").Inc()
		return err
	}
	now := c.now().UTC()
	records := make([]archive.Record, 0, len(limits))
	for name, l := range limits {
		records = append(records, archive.Record{
			Type:      "Limits",
			Timestamp: now,
			Id:        name + "@" + now.Format(time.RFC3339),
			Payload: map[string]any{
				"limitName":      name,
				"limitMax":       l.Max,
				"limitRemaining": l.Remaining,
			},
		})
	}
	if len(records) == 0 {
		return nil
	}
	_, err = archive.WriteRecords(ctx, c.sink, archive.ObjectMeta{
		Source: archive.SourceLimits, OrgId: c.orgId, Env: c.env, Instance: c.conf.Name, EventType: "Limits", PartitionTime: now,
	}, records)
	if err != nil {
		metrics.UploadFailures.WithLabelValues(archive.SourceLimits).Inc()
	}
	return err
}

func parseSFDate(s string) (time.Time, error) {
	for _, layout := range []string{LogDateFormat, "2006-01-02T15:04:05.000Z0700", time.RFC3339Nano} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognised date %q", s)
}

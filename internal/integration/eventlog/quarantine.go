package eventlog

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/archive"
	"github.com/davtir78/salesforce-s3-archiver/internal/integration/eventlog/query"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/davtir78/salesforce-s3-archiver/internal/metrics"
)

// DefaultMalformedAttempts is how many polls a file must fail CSV parsing
// before it is quarantined.
const DefaultMalformedAttempts = 3

// malformedCsvError marks a file whose content cannot be parsed. Unlike
// download or upload failures, retrying will not fix it.
type malformedCsvError struct {
	line int
	err  error
}

func (e *malformedCsvError) Error() string {
	return fmt.Sprintf("malformed CSV at line %d: %v", e.line, e.err)
}

func (e *malformedCsvError) Unwrap() error { return e.err }

func (c *Collector) malformedAttempts() int {
	if c.conf.MalformedFileAttempts > 0 {
		return c.conf.MalformedFileAttempts
	}
	return DefaultMalformedAttempts
}

// handleMalformed retries a malformed file on later polls (a truncated or
// corrupted download can look malformed). Once it has failed
// malformedFileAttempts times, the raw file is archived line by line to a
// quarantine object so the data is kept and processing can move past it.
func (c *Collector) handleMalformed(ctx context.Context, rec *query.EventLogfileRecord, meta archive.ObjectMeta, path string, cause *malformedCsvError) error {
	key := c.conf.Name + "_elf_malformed_" + rec.Id
	attempts := 1
	if val, err := c.db.GetCacheVal(key); err == nil {
		if s, ok := val.(string); ok {
			if n, err := strconv.Atoi(s); err == nil {
				attempts = n + 1
			}
		}
	}
	metrics.EventLogFailures.WithLabelValues("malformed").Inc()
	if attempts < c.malformedAttempts() {
		if err := c.db.SetCacheVal(key, strconv.Itoa(attempts)); err != nil {
			log.Warnf("Error storing malformed attempt count for %s: %v", rec.Id, err)
		}
		return fmt.Errorf("attempt %d of %d: %w", attempts, c.malformedAttempts(), cause)
	}

	qmeta := meta
	qmeta.Name = meta.Name + "-quarantine"
	qmeta.Lineage = map[string]string{}
	for k, v := range meta.Lineage {
		qmeta.Lineage[k] = v
	}
	qmeta.Lineage["quarantineReason"] = cause.Error()
	qmeta.Lineage["attempts"] = strconv.Itoa(attempts)

	m, err := c.sink.Write(ctx, qmeta, func(w archive.RecordWriter) error {
		return streamRawLines(path, rec.Id, rec.EventType, cause, w)
	})
	if err != nil {
		return fmt.Errorf("quarantining malformed EventLogFile %s: %w", rec.Id, err)
	}
	metrics.EventLogQuarantined.WithLabelValues(rec.EventType).Inc()
	log.Errorf("QUARANTINED EventLogFile %s (%s) after %d attempts: %v. Raw lines archived to %s; investigate and reprocess manually.",
		rec.Id, rec.EventType, attempts, cause, m.ObjectKey)
	if err := c.db.DelCacheVal(key); err != nil {
		log.Warnf("Error clearing malformed attempt count for %s: %v", rec.Id, err)
	}
	return nil
}

// streamRawLines archives every physical line of the file unparsed.
func streamRawLines(path, fileId, eventType string, cause *malformedCsvError, w archive.RecordWriter) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 256*1024)
	now := time.Now().UTC()
	for n := 1; ; n++ {
		line, err := r.ReadString('\n')
		if len(line) > 0 {
			rec := archive.Record{
				Type:      eventType,
				Id:        csvRowId(fileId, n),
				Timestamp: now,
				Payload: map[string]any{
					"quarantined":      true,
					"lineNumber":       n,
					"rawLine":          line,
					"quarantineReason": cause.Error(),
				},
			}
			if werr := w.WriteRecord(rec); werr != nil {
				return werr
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// DefaultUnavailableAttempts is how many polls a file may fail to download
// with a permanent error before it is recorded as unavailable and skipped.
const DefaultUnavailableAttempts = 5

func (c *Collector) unavailableAttempts() int {
	if c.conf.UnavailableFileAttempts > 0 {
		return c.conf.UnavailableFileAttempts
	}
	return DefaultUnavailableAttempts
}

// handleDownloadFailure decides whether a file that cannot be downloaded should
// keep blocking newer files. Transient failures (5xx, timeouts, S3 problems)
// always block, because skipping them would lose data that is still available.
// A permanently rejected file (400, 404, 410) is recorded as unavailable after
// several polls so the watermark can move past it; the gap is archived as a
// tombstone object and reported loudly.
func (c *Collector) handleDownloadFailure(ctx context.Context, rec *query.EventLogfileRecord, created time.Time, cause error) error {
	var httpErr *query.HTTPError
	permanent := errors.As(cause, &httpErr) && httpErr.Permanent()
	if !permanent {
		return cause
	}

	key := c.conf.Name + "_elf_unavailable_" + rec.Id
	attempts := 1
	if val, err := c.db.GetCacheVal(key); err == nil {
		if str, ok := val.(string); ok {
			if n, convErr := strconv.Atoi(str); convErr == nil {
				attempts = n + 1
			}
		}
	}
	if attempts < c.unavailableAttempts() {
		if err := c.db.SetCacheVal(key, strconv.Itoa(attempts)); err != nil {
			log.Warnf("Error storing unavailable attempt count for %s: %v", rec.Id, err)
		}
		return fmt.Errorf("attempt %d of %d: %w", attempts, c.unavailableAttempts(), cause)
	}

	logDate, err := parseSFDate(rec.LogDate)
	if err != nil {
		logDate = created
	}
	meta := archive.ObjectMeta{
		Source:        archive.SourceEventLog,
		OrgId:         c.orgId,
		Env:           c.env,
		Instance:      c.conf.Name,
		EventType:     rec.EventType,
		PartitionTime: logDate,
		Name:          "elf-" + rec.Id + "-unavailable",
		Lineage: map[string]string{
			"eventLogFileId":    rec.Id,
			"createdDate":       rec.CreatedDate,
			"logDate":           rec.LogDate,
			"unavailableReason": cause.Error(),
			"attempts":          strconv.Itoa(attempts),
		},
	}
	record := archive.Record{
		Type:      rec.EventType,
		Id:        csvRowId(rec.Id, 0),
		Timestamp: created.UTC(),
		Payload: map[string]any{
			"unavailable":       true,
			"eventLogFileId":    rec.Id,
			"unavailableReason": cause.Error(),
			"attempts":          attempts,
		},
	}
	if _, err := archive.WriteRecords(ctx, c.sink, meta, []archive.Record{record}); err != nil {
		return fmt.Errorf("recording unavailable EventLogFile %s: %w", rec.Id, err)
	}
	metrics.EventLogUnavailable.WithLabelValues(rec.EventType).Inc()
	log.Errorf("UNAVAILABLE EventLogFile %s (%s) after %d attempts: %v. Salesforce will not serve this file; a tombstone was archived and processing has moved past it.",
		rec.Id, rec.EventType, attempts, cause)
	if err := c.db.DelCacheVal(key); err != nil {
		log.Warnf("Error clearing unavailable attempt count for %s: %v", rec.Id, err)
	}
	return nil
}

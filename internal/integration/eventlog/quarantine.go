package eventlog

import (
	"bufio"
	"context"
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
		return streamRawLines(path, rec.EventType, cause, w)
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
func streamRawLines(path, eventType string, cause *malformedCsvError, w archive.RecordWriter) error {
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
				Timestamp: now,
				Attributes: map[string]any{
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

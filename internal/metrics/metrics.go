// Package metrics exposes Prometheus metrics and health endpoints.
package metrics

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	RecordsArchived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sfarchive_records_archived_total",
		Help: "Records durably written to the archive.",
	}, []string{"source", "name"})

	ObjectsArchived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sfarchive_objects_archived_total",
		Help: "Objects durably written to the archive.",
	}, []string{"source"})

	UploadFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sfarchive_upload_failures_total",
		Help: "Failed archive write attempts (including retried attempts).",
	}, []string{"source"})

	CheckpointCommits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sfarchive_stream_checkpoint_commits_total",
		Help: "Replay checkpoints committed after durable archive writes.",
	}, []string{"topic"})

	CheckpointFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sfarchive_stream_checkpoint_failures_total",
		Help: "Failed replay checkpoint reads, writes or lease renewals.",
	}, []string{"topic", "op"})

	LastCommittedEventTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sfarchive_stream_last_committed_event_timestamp_seconds",
		Help: "Event time of the newest event covered by the committed checkpoint. Alert on time() minus this (replay age).",
	}, []string{"topic"})

	LastCommitTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sfarchive_stream_last_commit_timestamp_seconds",
		Help: "Wall-clock time of the last checkpoint commit (including keepalive commits).",
	}, []string{"topic"})

	Reconnects = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sfarchive_stream_reconnects_total",
		Help: "Subscription reconnects by reason.",
	}, []string{"topic", "reason"})

	BufferedEvents = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sfarchive_stream_buffered_events",
		Help: "Events received but not yet durably archived.",
	}, []string{"topic"})

	EventLogFilesArchived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sfarchive_eventlog_files_archived_total",
		Help: "EventLogFiles durably archived.",
	}, []string{"event_type"})

	EventLogFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sfarchive_eventlog_failures_total",
		Help: "EventLogFile/SOQL collection failures by stage.",
	}, []string{"stage"})

	Watermark = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sfarchive_watermark_timestamp_seconds",
		Help: "Current watermark for EventLogFile and custom query collection.",
	}, []string{"name"})

	LastSuccessfulPoll = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sfarchive_eventlog_last_successful_poll_timestamp_seconds",
		Help: "Time of the last poll that completed without errors.",
	})
)

var ready atomic.Bool

// SetReady marks the process ready (e.g. all stream leases acquired).
func SetReady(v bool) { ready.Store(v) }

// Serve runs the metrics/health server until ctx is done. addr "" disables it.
func Serve(ctx context.Context, addr string) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ready"))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Errorf("metrics server: %v", err)
		}
	}()
}

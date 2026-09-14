package stream

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/archive"
	"github.com/davtir78/salesforce-s3-archiver/internal/checkpoint"
	"github.com/davtir78/salesforce-s3-archiver/internal/integration/stream/pubsub/proto"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/davtir78/salesforce-s3-archiver/internal/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// MaxAppetite is the largest num_requested the Pub/Sub API accepts.
	MaxAppetite = 100

	ReplayEarliest = "EARLIEST"
	ReplayLatest   = "LATEST"

	errorCodeAuth = "sfdc.platform.eventbus.grpc.service.auth.error"
	// Salesforce keeps the subscription alive with keepalives within 270s.
	streamIdleTimeout = 300 * time.Second
)

var (
	// ErrNoCheckpoint means the topic has never been checkpointed and no
	// initialReplay was configured. Starting at LATEST silently would skip data.
	ErrNoCheckpoint = errors.New("no replay checkpoint exists for topic; set eventStream.initialReplay to EARLIEST or LATEST to bootstrap it deliberately")
	// ErrReplayUnavailable means the stored replay ID was rejected (for example
	// it is older than the retention window). Falling back would hide data loss.
	ErrReplayUnavailable = errors.New("stored replay ID was rejected by Salesforce; events may have been lost. Investigate, backfill, and reset the checkpoint deliberately")
)

// FatalError stops the collector. Everything else triggers a reconnect.
type FatalError struct{ Err error }

func (e *FatalError) Error() string { return e.Err.Error() }
func (e *FatalError) Unwrap() error { return e.Err }

func fatal(err error) error { return &FatalError{Err: err} }

func IsFatal(err error) bool {
	var f *FatalError
	return errors.As(err, &f)
}

// Client is the subset of the Pub/Sub client used by the subscriber.
type Client interface {
	Authenticate(ctx context.Context) error
	OrgId() string
	Subscribe(ctx context.Context) (proto.PubSub_SubscribeClient, error)
	DecodeEvent(ctx context.Context, event *proto.ConsumerEvent) (map[string]any, string, error)
}

type SubscriberOptions struct {
	Topic    string
	Instance string
	// Organisation ID override; defaults to the authenticated org.
	OrgId string
	// Events requested per FetchRequest.
	Appetite int32
	// Flush when this many events are buffered.
	MaxEvents int
	// Flush when the oldest buffered event has waited this long.
	MaxAge time.Duration
	// EARLIEST, LATEST or empty (refuse to start without a checkpoint).
	InitialReplay string
	LeaseTTL      time.Duration
	// Attempts for each archive write before giving up (fatal).
	WriteAttempts int
	// Maximum reconnect backoff.
	MaxBackoff time.Duration
	// Base retry delay (tests use small values).
	RetryDelay time.Duration
}

func (o *SubscriberOptions) defaults() {
	if o.Appetite <= 0 {
		o.Appetite = MaxAppetite
	}
	if o.Appetite > MaxAppetite {
		log.Warnf("appetite %d exceeds the Pub/Sub API maximum of %d events per FetchRequest; using %d", o.Appetite, MaxAppetite, MaxAppetite)
		o.Appetite = MaxAppetite
	}
	if o.MaxEvents <= 0 {
		o.MaxEvents = 1000
	}
	if o.MaxAge <= 0 {
		o.MaxAge = 60 * time.Second
	}
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = 30 * time.Second
	}
	if o.WriteAttempts <= 0 {
		o.WriteAttempts = 5
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 60 * time.Second
	}
	if o.RetryDelay <= 0 {
		o.RetryDelay = time.Second
	}
	o.InitialReplay = strings.ToUpper(strings.TrimSpace(o.InitialReplay))
}

// Subscriber archives one topic. Its invariant: the committed replay ID never
// covers an event that is not durably stored in the archive.
type Subscriber struct {
	opts   SubscriberOptions
	client Client
	sink   archive.Sink
	store  checkpoint.Store
	logger interface {
		Info(string, ...any)
		Warn(string, ...any)
		Error(string, ...any)
	}

	// OnActive is called with true once the lease is held and the checkpoint
	// loaded (the subscriber is about to archive), and false when Run returns.
	OnActive func(active bool)

	lease checkpoint.Lease
	// Last committed replay ID (nil until the first commit when bootstrapping).
	committed []byte

	buffer      []archive.Record
	bufferStart time.Time
}

func NewSubscriber(opts SubscriberOptions, client Client, sink archive.Sink, store checkpoint.Store) *Subscriber {
	opts.defaults()
	return &Subscriber{
		opts:   opts,
		client: client,
		sink:   sink,
		store:  store,
		logger: log.With("topic", opts.Topic, "instance", opts.Instance),
	}
}

// Run acquires the topic lease and archives the topic until ctx is cancelled
// (returns nil after a final flush) or a fatal error occurs.
func (s *Subscriber) Run(ctx context.Context) error {
	switch s.opts.InitialReplay {
	case "", ReplayEarliest, ReplayLatest:
	default:
		return fatal(fmt.Errorf("invalid initialReplay %q", s.opts.InitialReplay))
	}

	lease, err := s.store.Acquire(ctx, checkpoint.Key(s.opts.Instance, s.opts.Topic), s.opts.LeaseTTL)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		metrics.CheckpointFailures.WithLabelValues(s.opts.Topic, "acquire").Inc()
		return fatal(fmt.Errorf("acquiring checkpoint lease: %w", err))
	}
	s.lease = lease
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		lease.Release(releaseCtx)
	}()

	replay, ok, err := lease.Load(ctx)
	if err != nil {
		metrics.CheckpointFailures.WithLabelValues(s.opts.Topic, "load").Inc()
		return fatal(fmt.Errorf("loading checkpoint: %w", err))
	}
	if ok {
		s.committed = replay
		s.logger.Info("Resuming from stored replay checkpoint", "replayId", encodeReplay(replay))
	} else {
		if s.opts.InitialReplay == "" {
			return fatal(ErrNoCheckpoint)
		}
		s.logger.Warn("No checkpoint found; bootstrapping subscription", "initialReplay", s.opts.InitialReplay)
	}

	if s.OnActive != nil {
		s.OnActive(true)
		defer s.OnActive(false)
	}
	metrics.LeaseHeld.WithLabelValues(s.opts.Topic).Set(1)
	defer metrics.LeaseHeld.WithLabelValues(s.opts.Topic).Set(0)

	renewStop := make(chan struct{})
	renewErr := make(chan error, 1)
	go s.renewLease(ctx, renewStop, renewErr)
	defer close(renewStop)

	backoff := s.opts.RetryDelay
	for {
		err := s.session(ctx, renewErr)
		if ctx.Err() != nil {
			// Shutting down. A fatal error here means the final flush or commit
			// failed; report it so the process exits non-zero.
			if IsFatal(err) {
				return err
			}
			return nil
		}
		if IsFatal(err) {
			return err
		}
		reason := "error"
		if isAuthError(err) {
			reason = "auth"
		}
		metrics.Reconnects.WithLabelValues(s.opts.Topic, reason).Inc()
		s.logger.Warn("Subscription ended; reconnecting", "error", fmt.Sprint(err), "backoff", backoff.String())
		if sleepErr := sleep(ctx, backoff); sleepErr != nil {
			return nil
		}
		if err == nil {
			backoff = s.opts.RetryDelay
		} else if backoff *= 2; backoff > s.opts.MaxBackoff {
			backoff = s.opts.MaxBackoff
		}
	}
}

func (s *Subscriber) renewLease(ctx context.Context, stop <-chan struct{}, out chan<- error) {
	interval := s.opts.LeaseTTL / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastOK := time.Now()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := s.lease.Renew(ctx)
			if err == nil {
				lastOK = time.Now()
				continue
			}
			metrics.CheckpointFailures.WithLabelValues(s.opts.Topic, "renew").Inc()
			if errors.Is(err, checkpoint.ErrLeaseLost) || time.Since(lastOK) > s.opts.LeaseTTL-interval/2 {
				select {
				case out <- fatal(fmt.Errorf("checkpoint lease could not be kept: %w", err)):
				default:
				}
				return
			}
			s.logger.Warn("Lease renewal failed; will retry", "error", err.Error())
		}
	}
}

type recvResult struct {
	resp *proto.FetchResponse
	err  error
}

// session runs one subscription stream.
func (s *Subscriber) session(ctx context.Context, renewErr <-chan error) (retErr error) {
	if s.client.OrgId() == "" {
		if err := s.client.Authenticate(ctx); err != nil {
			return fmt.Errorf("authenticating: %w", err)
		}
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := s.client.Subscribe(streamCtx)
	if err != nil {
		return fmt.Errorf("opening subscription: %w", err)
	}

	req := &proto.FetchRequest{TopicName: s.opts.Topic, NumRequested: s.opts.Appetite}
	switch {
	case s.committed != nil:
		req.ReplayPreset = proto.ReplayPreset_CUSTOM
		req.ReplayId = s.committed
	case s.opts.InitialReplay == ReplayEarliest:
		req.ReplayPreset = proto.ReplayPreset_EARLIEST
	default:
		req.ReplayPreset = proto.ReplayPreset_LATEST
	}
	if err := stream.Send(req); err != nil && err != io.EOF {
		return fmt.Errorf("sending initial fetch request: %w", err)
	}
	pending := s.opts.Appetite

	results := make(chan recvResult)
	go func() {
		for {
			resp, err := stream.Recv()
			select {
			case results <- recvResult{resp, err}:
			case <-streamCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	// Whatever ends the session, try to persist what is buffered. On a
	// shutdown the parent context is already cancelled, so use a detached one.
	defer func() {
		// After a fatal error (failed write/commit, lost lease) the buffer is
		// abandoned: the checkpoint was not advanced, so a restart replays it.
		if len(s.buffer) == 0 || (retErr != nil && IsFatal(retErr)) {
			s.buffer = nil
			return
		}
		flushCtx, flushCancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer flushCancel()
		if err := s.flush(flushCtx); err != nil {
			if retErr == nil || !IsFatal(retErr) {
				retErr = err
			}
		}
	}()

	flushTimer := time.NewTimer(time.Hour)
	flushTimer.Stop()
	idle := time.NewTimer(streamIdleTimeout)
	defer idle.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-renewErr:
			return err

		case <-idle.C:
			return errors.New("no message or keepalive received within the idle timeout")

		case <-flushTimer.C:
			if err := s.flush(ctx); err != nil {
				return err
			}

		case res := <-results:
			if res.err != nil {
				return s.classifyStreamError(ctx, stream, res.err)
			}
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(streamIdleTimeout)

			resp := res.resp
			for _, ev := range resp.GetEvents() {
				rec, err := s.decode(ctx, ev)
				if err != nil {
					// An undecodable event cannot be skipped without losing it.
					return fatal(fmt.Errorf("decoding event with replay ID %s: %w", encodeReplay(ev.GetReplayId()), err))
				}
				if len(s.buffer) == 0 {
					s.bufferStart = time.Now()
					flushTimer.Reset(s.opts.MaxAge)
				}
				s.buffer = append(s.buffer, rec)
				pending--
				if len(s.buffer) >= s.opts.MaxEvents {
					flushTimer.Stop()
					if err := s.flush(ctx); err != nil {
						return err
					}
				}
			}
			metrics.BufferedEvents.WithLabelValues(s.opts.Topic).Set(float64(len(s.buffer)))

			// Keepalive with outstanding demand: nothing is pending delivery up
			// to latest_replay_id, so it is safe to move the checkpoint there.
			// This keeps quiet topics from ageing out of the retention window.
			if len(resp.GetEvents()) == 0 && len(s.buffer) == 0 && resp.GetPendingNumRequested() > 0 && len(resp.GetLatestReplayId()) > 0 {
				if err := s.commit(ctx, resp.GetLatestReplayId(), nil); err != nil {
					return err
				}
			}

			if pending <= s.opts.Appetite/2 {
				more := s.opts.Appetite - pending
				if err := stream.Send(&proto.FetchRequest{TopicName: s.opts.Topic, NumRequested: more}); err != nil && err != io.EOF {
					return fmt.Errorf("requesting more events: %w", err)
				}
				pending += more
			}
		}
	}
}

func (s *Subscriber) decode(ctx context.Context, ev *proto.ConsumerEvent) (archive.Record, error) {
	fields, eventType, err := s.client.DecodeEvent(ctx, ev)
	if err != nil {
		return archive.Record{}, err
	}
	return archive.Record{
		Type:       eventType,
		Timestamp:  eventTimestamp(fields),
		Attributes: fields,
		ReplayId:   append([]byte(nil), ev.GetReplayId()...),
	}, nil
}

func eventTimestamp(fields map[string]any) time.Time {
	for _, name := range []string{"EventDate", "CreatedDate"} {
		switch v := fields[name].(type) {
		case int64:
			return time.UnixMilli(v).UTC()
		case int32:
			return time.UnixMilli(int64(v)).UTC()
		case float64:
			return time.UnixMilli(int64(v)).UTC()
		}
	}
	return time.Now().UTC()
}

func (s *Subscriber) classifyStreamError(ctx context.Context, stream proto.PubSub_SubscribeClient, err error) error {
	code := ""
	if vals := stream.Trailer().Get("error-code"); len(vals) > 0 {
		code = vals[0]
	}
	st, _ := status.FromError(err)
	switch {
	case code == errorCodeAuth || st.Code() == codes.Unauthenticated:
		s.logger.Warn("Pub/Sub session rejected; re-authenticating", "errorCode", code)
		if authErr := s.client.Authenticate(ctx); authErr != nil {
			return fmt.Errorf("re-authenticating: %w", authErr)
		}
		return &authError{err}
	case strings.Contains(strings.ToLower(code), "replayid"):
		return fatal(fmt.Errorf("%w (error-code %s, replay ID %s): %v", ErrReplayUnavailable, code, encodeReplay(s.committed), err))
	case err == io.EOF:
		return errors.New("subscription stream closed by server")
	default:
		return fmt.Errorf("subscription stream error (error-code %q): %w", code, err)
	}
}

type authError struct{ err error }

func (e *authError) Error() string { return "authentication refreshed after: " + e.err.Error() }
func (e *authError) Unwrap() error { return e.err }

func isAuthError(err error) bool {
	var a *authError
	return errors.As(err, &a)
}

// flush durably archives the buffer, then commits the checkpoint.
func (s *Subscriber) flush(ctx context.Context) error {
	if len(s.buffer) == 0 {
		return nil
	}
	orgId := s.opts.OrgId
	if orgId == "" {
		orgId = s.client.OrgId()
	}

	// Group by event type; a topic normally carries one type.
	groups := map[string][]archive.Record{}
	var order []string
	for _, r := range s.buffer {
		if _, seen := groups[r.Type]; !seen {
			order = append(order, r.Type)
		}
		groups[r.Type] = append(groups[r.Type], r)
	}

	partition := time.Now().UTC()
	for _, eventType := range order {
		records := groups[eventType]
		meta := archive.ObjectMeta{
			Source:        archive.SourceStream,
			OrgId:         orgId,
			Instance:      s.opts.Instance,
			EventType:     eventType,
			PartitionTime: partition,
			Lineage: map[string]string{
				"topic":         s.opts.Topic,
				"replayIdFirst": encodeReplay(records[0].ReplayId),
				"replayIdLast":  encodeReplay(records[len(records)-1].ReplayId),
			},
		}
		if err := s.writeWithRetry(ctx, meta, records); err != nil {
			return err
		}
		metrics.RecordsArchived.WithLabelValues(archive.SourceStream, s.opts.Topic).Add(float64(len(records)))
	}

	last := s.buffer[len(s.buffer)-1]
	newest := last.Timestamp
	for _, r := range s.buffer {
		if r.Timestamp.After(newest) {
			newest = r.Timestamp
		}
	}
	if err := s.commit(ctx, last.ReplayId, &newest); err != nil {
		return err
	}
	s.logger.Info("Archived batch and committed checkpoint", "events", len(s.buffer), "replayId", encodeReplay(last.ReplayId),
		"bufferedFor", time.Since(s.bufferStart).Round(time.Millisecond).String())
	s.buffer = nil
	metrics.BufferedEvents.WithLabelValues(s.opts.Topic).Set(0)
	return nil
}

func (s *Subscriber) writeWithRetry(ctx context.Context, meta archive.ObjectMeta, records []archive.Record) error {
	delay := s.opts.RetryDelay
	var lastErr error
	for attempt := 1; attempt <= s.opts.WriteAttempts; attempt++ {
		_, err := archive.WriteRecords(ctx, s.sink, meta, records)
		if err == nil {
			metrics.ObjectsArchived.WithLabelValues(archive.SourceStream).Inc()
			return nil
		}
		lastErr = err
		metrics.UploadFailures.WithLabelValues(archive.SourceStream).Inc()
		s.logger.Error("Archive write failed", "attempt", attempt, "of", s.opts.WriteAttempts, "events", len(records), "error", err.Error())
		if attempt == s.opts.WriteAttempts {
			break
		}
		if sleep(ctx, delay) != nil {
			break
		}
		if delay *= 2; delay > 30*time.Second {
			delay = 30 * time.Second
		}
	}
	// The checkpoint was not advanced; restarting replays these events.
	return fatal(fmt.Errorf("archive write failed after retries, checkpoint not advanced: %w", lastErr))
}

func (s *Subscriber) commit(ctx context.Context, replayId []byte, newestEvent *time.Time) error {
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		err := s.lease.Commit(ctx, replayId)
		if err == nil {
			s.committed = append([]byte(nil), replayId...)
			metrics.CheckpointCommits.WithLabelValues(s.opts.Topic).Inc()
			metrics.LastCommitTime.WithLabelValues(s.opts.Topic).SetToCurrentTime()
			if newestEvent != nil {
				metrics.LastCommittedEventTime.WithLabelValues(s.opts.Topic).Set(float64(newestEvent.UnixMilli()) / 1000)
			}
			return nil
		}
		lastErr = err
		metrics.CheckpointFailures.WithLabelValues(s.opts.Topic, "commit").Inc()
		if errors.Is(err, checkpoint.ErrLeaseLost) {
			break
		}
		if sleep(ctx, s.opts.RetryDelay) != nil {
			break
		}
	}
	// Data is archived but the checkpoint did not move: a restart re-archives
	// these events (duplicates, never loss).
	return fatal(fmt.Errorf("committing checkpoint failed: %w", lastErr))
}

func encodeReplay(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(b)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

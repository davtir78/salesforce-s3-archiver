package stream

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/archive"
	"github.com/davtir78/salesforce-s3-archiver/internal/checkpoint"
	"github.com/davtir78/salesforce-s3-archiver/internal/config"
	"github.com/davtir78/salesforce-s3-archiver/internal/integration/stream/pubsub/grpcclient"
	"github.com/davtir78/salesforce-s3-archiver/internal/mocksf"
)

const testTopic = "/event/LoginEventStream"

type harness struct {
	t        *testing.T
	mock     *mocksf.Server
	httpURL  string
	grpcAddr string
	sink     *archive.MemorySink
	store    *checkpoint.MemoryStore
	key      string
}

func newHarness(t *testing.T, opts mocksf.Options) *harness {
	t.Helper()
	if len(opts.Topics) == 0 {
		opts.Topics = []string{testTopic}
	}
	if opts.KeepaliveInterval == 0 {
		opts.KeepaliveInterval = 200 * time.Millisecond
	}
	mock, httpURL, grpcAddr, err := mocksf.Start(opts, "127.0.0.1:0", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mock.Stop)
	return &harness{
		t: t, mock: mock, httpURL: httpURL, grpcAddr: grpcAddr,
		sink:  archive.NewMemorySink(),
		store: checkpoint.NewMemoryStore(),
		key:   checkpoint.Key("test", testTopic),
	}
}

func (h *harness) client() (*grpcclient.PubSubClient, func()) {
	o := h.mock.Options()
	c, err := grpcclient.NewGRPCClient(grpcclient.Options{
		Endpoint: h.grpcAddr,
		Insecure: true,
		Auth: config.AuthConfig{
			TokenUrl:   h.httpURL,
			ClientCred: &config.ClientCredAuth{ClientId: o.ClientId, ClientSecret: o.ClientSecret},
		},
	})
	if err != nil {
		h.t.Fatal(err)
	}
	return c, c.Close
}

func (h *harness) options() SubscriberOptions {
	return SubscriberOptions{
		Topic:         testTopic,
		Instance:      "test",
		Appetite:      40,
		MaxEvents:     25,
		MaxAge:        100 * time.Millisecond,
		InitialReplay: ReplayEarliest,
		LeaseTTL:      3 * time.Second,
		WriteAttempts: 2,
		RetryDelay:    5 * time.Millisecond,
		MaxBackoff:    50 * time.Millisecond,
	}
}

// run starts a subscriber and returns a stop function that cancels it and
// returns Run's result.
func (h *harness) run(opts SubscriberOptions, store checkpoint.Store) (done <-chan error, stop func() error) {
	client, closeFn := h.client()
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	notify := make(chan error, 1)
	var result error
	go func() {
		defer closeFn()
		result = NewSubscriber(opts, client, h.sink, store).Run(ctx)
		close(finished)
		notify <- result
	}()
	var once sync.Once
	var stopErr error
	return notify, func() error {
		once.Do(func() {
			cancel()
			select {
			case <-finished:
				stopErr = result
			case <-time.After(10 * time.Second):
				stopErr = errors.New("subscriber did not stop")
			}
		})
		return stopErr
	}
}

// archived returns counts of archived EventIdentifiers.
func (h *harness) archived() map[string]int {
	lines, err := h.sink.Lines()
	if err != nil {
		h.t.Fatal(err)
	}
	counts := map[string]int{}
	for _, l := range lines {
		id, _ := l.Attributes["EventIdentifier"].(string)
		counts[id]++
	}
	return counts
}

// reconcile returns how many ledger events are missing and how many duplicates exist.
func (h *harness) reconcile() (missing int, duplicates int, total int) {
	counts := h.archived()
	ledger := h.mock.Ledger().Streams[testTopic]
	for _, id := range ledger {
		if counts[id] == 0 {
			missing++
		}
	}
	for _, c := range counts {
		if c > 1 {
			duplicates += c - 1
		}
	}
	return missing, duplicates, len(ledger)
}

func (h *harness) waitAllArchived(timeout time.Duration) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if missing, _, _ := h.reconcile(); missing == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	missing, _, total := h.reconcile()
	h.t.Fatalf("timed out: %d of %d events not archived", missing, total)
}

func (h *harness) waitCheckpoint(seq uint64, timeout time.Duration) {
	h.t.Helper()
	want := string(mocksf.ReplayID(seq))
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got, ok := h.store.Checkpoint(h.key); ok && string(got) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, _ := h.store.Checkpoint(h.key)
	h.t.Fatalf("checkpoint = %x, want %x", got, want)
}

func (h *harness) publish(n int) {
	h.t.Helper()
	if _, err := h.mock.PublishEvents(testTopic, n); err != nil {
		h.t.Fatal(err)
	}
}

func TestArchivesEveryEventAndCommitsAfterDurability(t *testing.T) {
	h := newHarness(t, mocksf.Options{})
	h.publish(260)

	// Assert the invariant on every commit: the committed replay ID is covered
	// by the archive at the moment it is written.
	var violations atomic.Int32
	h.store.CommitHook = func(replayId []byte) error {
		lines, _ := h.sink.Lines()
		for _, l := range lines {
			if l.ReplayId != "" && l.ReplayId == encodeReplay(replayId) {
				return nil
			}
		}
		// Keepalive commits advance past events only when none are pending.
		if len(lines) < 260 {
			violations.Add(1)
		}
		return nil
	}

	_, stop := h.run(h.options(), h.store)
	h.waitAllArchived(10 * time.Second)
	h.waitCheckpoint(260, 5*time.Second)
	if err := stop(); err != nil {
		t.Fatalf("Run returned %v", err)
	}
	if v := violations.Load(); v != 0 {
		t.Errorf("%d commits advanced the checkpoint beyond archived data", v)
	}

	missing, dups, total := h.reconcile()
	if missing != 0 || dups != 0 || total != 260 {
		t.Errorf("missing=%d dups=%d total=%d", missing, dups, total)
	}

	// Every field is kept, including non-union ones the upstream decoder dropped.
	lines, _ := h.sink.Lines()
	l := lines[0]
	for _, field := range []string{"EventUuid", "CreatedDate", "CreatedById", "EventIdentifier", "SourceIp", "Score", "IsSuccess"} {
		if _, ok := l.Attributes[field]; !ok {
			t.Errorf("field %s missing from archived event: %v", field, l.Attributes)
		}
	}
	if l.EventType != "LoginEventStream" || l.OrgId != h.mock.Options().OrgId || l.ReplayId == "" {
		t.Errorf("unexpected archived line metadata: %+v", l)
	}
	for _, k := range h.sink.Keys() {
		m, _ := h.sink.Manifest(k)
		if m.Lineage["topic"] != testTopic || m.Lineage["replayIdFirst"] == "" || m.Lineage["replayIdLast"] == "" {
			t.Errorf("manifest lineage incomplete: %+v", m.Lineage)
		}
	}
}

func TestRefusesToStartWithoutCheckpoint(t *testing.T) {
	h := newHarness(t, mocksf.Options{})
	h.publish(10)
	opts := h.options()
	opts.InitialReplay = ""
	done, stop := h.run(opts, h.store)
	defer stop()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNoCheckpoint) || !IsFatal(err) {
			t.Fatalf("got %v, want fatal ErrNoCheckpoint", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber did not refuse to start")
	}
	if len(h.sink.Keys()) != 0 {
		t.Errorf("nothing should be archived")
	}
}

func TestReplayOutsideRetentionIsFatal(t *testing.T) {
	h := newHarness(t, mocksf.Options{RetentionEvents: 20})
	h.publish(100)
	h.store.Set(h.key, mocksf.ReplayID(5))
	done, stop := h.run(h.options(), h.store)
	defer stop()
	select {
	case err := <-done:
		if !errors.Is(err, ErrReplayUnavailable) {
			t.Fatalf("got %v, want ErrReplayUnavailable", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscriber did not stop on an expired replay ID")
	}
	if got, _ := h.store.Checkpoint(h.key); string(got) != string(mocksf.ReplayID(5)) {
		t.Errorf("checkpoint must not be moved on replay failure, got %x", got)
	}
}

func TestLatestBootstrapAndKeepaliveCheckpoint(t *testing.T) {
	h := newHarness(t, mocksf.Options{KeepaliveInterval: 100 * time.Millisecond})
	h.publish(50) // published before the first subscription: skipped by LATEST
	opts := h.options()
	opts.InitialReplay = ReplayLatest
	_, stop := h.run(opts, h.store)
	defer stop()

	// With no traffic the keepalive moves the checkpoint to the head.
	h.waitCheckpoint(50, 5*time.Second)
	h.publish(30)
	h.waitCheckpoint(80, 5*time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	counts := h.archived()
	if len(counts) != 30 {
		t.Errorf("expected only the 30 events published after subscribing, got %d", len(counts))
	}
}

func TestGracefulShutdownFlushesBuffer(t *testing.T) {
	h := newHarness(t, mocksf.Options{})
	opts := h.options()
	opts.MaxEvents = 10000
	opts.MaxAge = time.Hour
	_, stop := h.run(opts, h.store)
	h.publish(37)
	time.Sleep(700 * time.Millisecond) // events are buffered, not yet flushed
	if n := len(h.sink.Keys()); n != 0 {
		t.Fatalf("expected no flush before shutdown, got %d objects", n)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if missing, dups, _ := h.reconcile(); missing != 0 || dups != 0 {
		t.Errorf("after graceful shutdown missing=%d dups=%d", missing, dups)
	}
	got, ok := h.store.Checkpoint(h.key)
	if !ok || string(got) != string(mocksf.ReplayID(37)) {
		t.Errorf("checkpoint after shutdown = %x", got)
	}
}

func TestTokenExpiryAndStreamDropsRecover(t *testing.T) {
	h := newHarness(t, mocksf.Options{})
	h.mock.SetFaults(mocksf.Faults{DropStreamAfterEvents: 33})
	_, stop := h.run(h.options(), h.store)
	defer stop()
	for i := 0; i < 6; i++ {
		h.publish(40)
		time.Sleep(150 * time.Millisecond)
		if i%2 == 1 {
			h.mock.ExpireTokens()
		}
	}
	h.waitAllArchived(20 * time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	missing, dups, total := h.reconcile()
	t.Logf("total=%d missing=%d duplicates=%d", total, missing, dups)
	if missing != 0 {
		t.Errorf("%d events lost", missing)
	}
}

// Each fault point of the failure matrix: the subscriber stops, a fresh
// subscriber resumes from the checkpoint, and nothing is lost.
func TestCrashMatrix(t *testing.T) {
	type faultSetup func(h *harness, k int)
	cases := map[string]faultSetup{
		"upload fails (nothing stored)": func(h *harness, k int) {
			h.sink.BeforeStore = func(_ archive.ObjectMeta, n int) error {
				if n >= k {
					return archive.ErrInjected
				}
				return nil
			}
		},
		"crash after upload before checkpoint": func(h *harness, k int) {
			h.sink.AfterStore = func(_ archive.ObjectMeta, n int) error {
				if n >= k {
					return archive.ErrInjected
				}
				return nil
			}
		},
		"checkpoint write fails": func(h *harness, k int) {
			var n atomic.Int32
			h.store.CommitHook = func([]byte) error {
				if int(n.Add(1)) >= k {
					return archive.ErrInjected
				}
				return nil
			}
		},
		"crash after checkpoint persisted": func(h *harness, k int) {
			var n atomic.Int32
			h.store.AfterCommitHook = func([]byte) error {
				if int(n.Add(1)) >= k {
					return checkpoint.ErrLeaseLost
				}
				return nil
			}
		},
	}
	for name, setup := range cases {
		for _, k := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/k=%d", name, k), func(t *testing.T) {
				h := newHarness(t, mocksf.Options{})
				h.publish(200)
				setup(h, k)

				done, stop := h.run(h.options(), h.store)
				select {
				case err := <-done:
					if !IsFatal(err) {
						t.Fatalf("expected a fatal error, got %v", err)
					}
				case <-time.After(10 * time.Second):
					stop()
					t.Fatal("fault did not stop the subscriber")
				}
				stop()

				// Restart without faults on a new handle (new process).
				restarted := h.store.Share()
				h.sink.BeforeStore, h.sink.AfterStore = nil, nil
				_, stop2 := h.run(h.options(), restarted)
				h.waitAllArchived(10 * time.Second)
				h.waitCheckpoint(200, 5*time.Second)
				if err := stop2(); err != nil {
					t.Fatal(err)
				}
				missing, dups, _ := h.reconcile()
				t.Logf("missing=%d duplicates=%d", missing, dups)
				if missing != 0 {
					t.Errorf("%d events lost", missing)
				}
				// Duplicates are bounded by what was in flight when the fault hit.
				if dups > 2*h.options().MaxEvents*h.options().WriteAttempts {
					t.Errorf("unexpectedly many duplicates: %d", dups)
				}
			})
		}
	}
}

func TestSecondCollectorWaitsForLeaseThenTakesOver(t *testing.T) {
	h := newHarness(t, mocksf.Options{})
	h.publish(100)
	_, stopA := h.run(h.options(), h.store)
	h.waitCheckpoint(100, 10*time.Second)

	storeB := h.store.Share()
	doneB, stopB := h.run(h.options(), storeB)
	defer stopB()
	h.publish(50)
	time.Sleep(500 * time.Millisecond)
	select {
	case err := <-doneB:
		t.Fatalf("second collector should be waiting for the lease, returned %v", err)
	default:
	}
	h.waitCheckpoint(150, 10*time.Second)

	// A stops (e.g. rolling deploy); B acquires the lease and continues.
	if err := stopA(); err != nil {
		t.Fatal(err)
	}
	h.publish(60)
	h.waitAllArchived(10 * time.Second)
	h.waitCheckpoint(210, 10*time.Second)
	if err := stopB(); err != nil {
		t.Fatal(err)
	}
	if missing, _, _ := h.reconcile(); missing != 0 {
		t.Errorf("%d events lost across handover", missing)
	}
}

func TestStolenLeaseStopsOldHolder(t *testing.T) {
	h := newHarness(t, mocksf.Options{})
	h.publish(20)
	doneA, stopA := h.run(h.options(), h.store)
	defer stopA()
	h.waitCheckpoint(20, 5*time.Second)

	// Simulate a partition: the lease expires and another collector takes it.
	h.store.ExpireLease(h.key)
	storeB := h.store.Share()
	_, stopB := h.run(h.options(), storeB)
	defer stopB()

	h.publish(30)
	select {
	case err := <-doneA:
		if !errors.Is(err, checkpoint.ErrLeaseLost) {
			t.Fatalf("old holder returned %v, want ErrLeaseLost", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("old holder kept running after losing its lease")
	}
	h.waitAllArchived(10 * time.Second)
	h.waitCheckpoint(50, 5*time.Second)
}

// Randomised chaos: repeated crashes, graceful restarts, stream drops and token
// expiry while events are continuously published. Nothing may be lost.
func TestChaosNoLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test skipped in -short mode")
	}
	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	t.Logf("seed=%d", seed)

	h := newHarness(t, mocksf.Options{RetentionEvents: 0})
	h.mock.StartGenerator(mocksf.GeneratorConfig{EventsPerSecond: 400})
	defer h.mock.StopGenerator()

	for round := 0; round < 25; round++ {
		store := h.store.Share()
		h.sink.BeforeStore, h.sink.AfterStore = nil, nil
		faultAt := rng.Intn(6) + 1
		switch rng.Intn(5) {
		case 0:
			h.sink.BeforeStore = func(_ archive.ObjectMeta, n int) error {
				if n >= faultAt+len(h.sink.Keys()) {
					return archive.ErrInjected
				}
				return nil
			}
		case 1:
			var c atomic.Int32
			h.sink.AfterStore = func(_ archive.ObjectMeta, n int) error {
				if int(c.Add(1)) >= faultAt {
					return archive.ErrInjected
				}
				return nil
			}
		case 2:
			var c atomic.Int32
			store.CommitHook = func([]byte) error {
				if int(c.Add(1)) >= faultAt {
					return archive.ErrInjected
				}
				return nil
			}
		case 3:
			h.mock.SetFaults(mocksf.Faults{DropStreamAfterEvents: rng.Intn(80) + 10})
		case 4:
			go func() {
				time.Sleep(time.Duration(rng.Intn(300)) * time.Millisecond)
				h.mock.ExpireTokens()
			}()
		}
		done, stop := h.run(h.options(), store)
		select {
		case <-done:
		case <-time.After(time.Duration(rng.Intn(400)+100) * time.Millisecond):
		}
		stop()
		h.mock.SetFaults(mocksf.Faults{})
	}

	h.mock.StopGenerator()
	h.sink.BeforeStore, h.sink.AfterStore = nil, nil
	final := h.store.Share()
	_, stop := h.run(h.options(), final)
	h.waitAllArchived(30 * time.Second)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	missing, dups, total := h.reconcile()
	t.Logf("seed=%d total=%d missing=%d duplicates=%d objects=%d", seed, total, missing, dups, len(h.sink.Keys()))
	if missing != 0 {
		t.Fatalf("%d of %d events lost (seed %d)", missing, total, seed)
	}
}

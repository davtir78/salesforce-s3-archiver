package mocksf

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/davtir78/salesforce-s3-archiver/internal/integration/stream/pubsub/proto"
	"github.com/davtir78/salesforce-s3-archiver/internal/log"
	"github.com/linkedin/goavro/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// Trailer error codes. The auth code matches what the upstream exporter
	// handles; the replay code is modelled on Salesforce documentation and
	// should be verified against a real org.
	ErrorCodeAuth          = "sfdc.platform.eventbus.grpc.service.auth.error"
	ErrorCodeReplayInvalid = "sfdc.platform.eventbus.grpc.subscription.fetch.replayid.corrupted"
	ErrorCodeTopic         = "sfdc.platform.eventbus.grpc.subscription.topic.notfound"
)

type storedEvent struct {
	seq      uint64
	id       string
	payload  []byte
	schemaId string
}

type topic struct {
	name      string
	schemaId  string
	schema    string
	codec     *goavro.Codec
	retention int

	mu      sync.Mutex
	cond    *sync.Cond
	events  []storedEvent // retained window, ascending seq
	nextSeq uint64
	ledger  []string // every EventIdentifier ever published
}

func newTopic(name string, retention int) (*topic, error) {
	schema := schemaFor(name)
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("schema for %s: %w", name, err)
	}
	sum := sha256.Sum256([]byte(schema))
	t := &topic{
		name:      name,
		schemaId:  hex.EncodeToString(sum[:8]),
		schema:    schema,
		codec:     codec,
		retention: retention,
		nextSeq:   1,
	}
	t.cond = sync.NewCond(&t.mu)
	return t, nil
}

func eventName(topicName string) string {
	parts := strings.Split(topicName, "/")
	return parts[len(parts)-1]
}

// schemaFor mixes required (non-union) fields with nullable unions, like real
// Real-Time Event Monitoring schemas.
func schemaFor(topicName string) string {
	return fmt.Sprintf(`{
  "type": "record",
  "name": %q,
  "namespace": "com.sforce.eventbus",
  "fields": [
    {"name": "CreatedDate", "type": "long"},
    {"name": "CreatedById", "type": "string"},
    {"name": "EventUuid", "type": "string"},
    {"name": "EventIdentifier", "type": ["null", "string"], "default": null},
    {"name": "EventDate", "type": ["null", "long"], "default": null},
    {"name": "UserId", "type": ["null", "string"], "default": null},
    {"name": "Username", "type": ["null", "string"], "default": null},
    {"name": "SourceIp", "type": ["null", "string"], "default": null},
    {"name": "Score", "type": ["null", "double"], "default": null},
    {"name": "IsSuccess", "type": ["null", "boolean"], "default": null},
    {"name": "Sequence", "type": ["null", "long"], "default": null}
  ]
}`, eventName(topicName))
}

func replayBytes(seq uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, seq)
	return b
}

func replaySeq(b []byte) (uint64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(b), true
}

// publish appends n generated events and returns their identifiers.
func (t *topic) publish(n int, orgUser string) ([]string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		seq := t.nextSeq
		id := fmt.Sprintf("%s-%010d-%s", eventName(t.name), seq, randomHex(4))
		now := time.Now().UnixMilli()
		native := map[string]any{
			"CreatedDate":     now,
			"CreatedById":     orgUser,
			"EventUuid":       randomHex(16),
			"EventIdentifier": goavro.Union("string", id),
			"EventDate":       goavro.Union("long", now),
			"UserId":          goavro.Union("string", orgUser),
			"Username":        goavro.Union("string", "user@mock.example"),
			"SourceIp":        goavro.Union("string", fmt.Sprintf("10.0.%d.%d", seq%250, (seq/250)%250)),
			"Score":           goavro.Union("double", float64(seq%100)/10),
			"IsSuccess":       goavro.Union("boolean", seq%7 != 0),
			"Sequence":        nil,
		}
		payload, err := t.codec.BinaryFromNative(nil, native)
		if err != nil {
			return nil, err
		}
		t.events = append(t.events, storedEvent{seq: seq, id: id, payload: payload, schemaId: t.schemaId})
		t.ledger = append(t.ledger, id)
		t.nextSeq++
		ids = append(ids, id)
	}
	if t.retention > 0 && len(t.events) > t.retention {
		t.events = append([]storedEvent(nil), t.events[len(t.events)-t.retention:]...)
	}
	t.cond.Broadcast()
	return ids, nil
}

// PublishEvents generates n events on the named topic.
func (s *Server) PublishEvents(topicName string, n int) ([]string, error) {
	t, ok := s.topics[topicName]
	if !ok {
		return nil, fmt.Errorf("unknown topic %s", topicName)
	}
	return t.publish(n, s.opts.UserId)
}

func (s *Server) authorize(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	get := func(k string) string {
		if v := md.Get(k); len(v) > 0 {
			return v[0]
		}
		return ""
	}
	if !s.validToken(get("accesstoken")) || get("instanceurl") == "" || get("tenantid") != s.opts.OrgId {
		grpc.SetTrailer(ctx, metadata.Pairs("error-code", ErrorCodeAuth))
		return status.Error(codes.Unauthenticated, "invalid or expired session")
	}
	return nil
}

func (s *Server) GetTopic(ctx context.Context, req *proto.TopicRequest) (*proto.TopicInfo, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	t, ok := s.topics[req.GetTopicName()]
	if !ok {
		grpc.SetTrailer(ctx, metadata.Pairs("error-code", ErrorCodeTopic))
		return nil, status.Error(codes.NotFound, "topic not found")
	}
	return &proto.TopicInfo{TopicName: t.name, TenantGuid: s.opts.OrgId, CanSubscribe: true, SchemaId: t.schemaId, RpcId: randomHex(8)}, nil
}

func (s *Server) GetSchema(ctx context.Context, req *proto.SchemaRequest) (*proto.SchemaInfo, error) {
	if err := s.authorize(ctx); err != nil {
		return nil, err
	}
	for _, t := range s.topics {
		if t.schemaId == req.GetSchemaId() {
			return &proto.SchemaInfo{SchemaJson: t.schema, SchemaId: t.schemaId, RpcId: randomHex(8)}, nil
		}
	}
	return nil, status.Error(codes.NotFound, "schema not found")
}

func (s *Server) Subscribe(stream proto.PubSub_SubscribeServer) error {
	ctx := stream.Context()
	if err := s.authorize(ctx); err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	t, ok := s.topics[first.GetTopicName()]
	if !ok {
		stream.SetTrailer(metadata.Pairs("error-code", ErrorCodeTopic))
		return status.Error(codes.NotFound, "topic not found")
	}

	// Resolve the starting position: cursor is the last seq already consumed.
	t.mu.Lock()
	var cursor uint64
	oldest := t.nextSeq
	if len(t.events) > 0 {
		oldest = t.events[0].seq
	}
	switch first.GetReplayPreset() {
	case proto.ReplayPreset_LATEST:
		cursor = t.nextSeq - 1
	case proto.ReplayPreset_EARLIEST:
		cursor = oldest - 1
	case proto.ReplayPreset_CUSTOM:
		seq, valid := replaySeq(first.GetReplayId())
		// A replay ID older than the retention window (or malformed/in the future)
		// cannot be honoured.
		if !valid || seq+1 < oldest || seq >= t.nextSeq {
			t.mu.Unlock()
			stream.SetTrailer(metadata.Pairs("error-code", ErrorCodeReplayInvalid))
			return status.Error(codes.InvalidArgument, "replay ID is invalid or outside the retention window")
		}
		cursor = seq
	}
	t.mu.Unlock()

	var (
		pmu     sync.Mutex
		pending = int(first.GetNumRequested())
	)
	// Reader for subsequent FetchRequests.
	recvErr := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				recvErr <- err
				t.cond.Broadcast()
				return
			}
			if req.GetTopicName() != "" && req.GetTopicName() != t.name {
				recvErr <- status.Error(codes.InvalidArgument, "topic mismatch")
				t.cond.Broadcast()
				return
			}
			pmu.Lock()
			pending += int(req.GetNumRequested())
			pmu.Unlock()
			t.cond.Broadcast()
		}
	}()
	// Wake the delivery loop periodically for keepalives and cancellation.
	stopTick := make(chan struct{})
	defer close(stopTick)
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopTick:
				return
			case <-ticker.C:
				t.cond.Broadcast()
			}
		}
	}()

	delivered := 0
	lastSend := time.Now()
	for {
		select {
		case err := <-recvErr:
			if err == io.EOF {
				return nil
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if !s.validSession(ctx) {
			stream.SetTrailer(metadata.Pairs("error-code", ErrorCodeAuth))
			return status.Error(codes.Unauthenticated, "session expired")
		}

		t.mu.Lock()
		pmu.Lock()
		want := pending
		pmu.Unlock()
		if want > s.opts.MaxEventsPerResponse {
			want = s.opts.MaxEventsPerResponse
		}
		batch := make([]*proto.ConsumerEvent, 0, want)
		if want > 0 && len(t.events) > 0 {
			// Events are contiguous by seq, so index directly past the cursor.
			start := 0
			if cursor >= t.events[0].seq {
				start = int(cursor-t.events[0].seq) + 1
			}
			for _, ev := range t.events[min(start, len(t.events)):] {
				if len(batch) >= want {
					break
				}
				batch = append(batch, &proto.ConsumerEvent{
					Event:    &proto.ProducerEvent{Id: ev.id, SchemaId: ev.schemaId, Payload: ev.payload},
					ReplayId: replayBytes(ev.seq),
				})
			}
		}
		head := t.nextSeq - 1
		idle := len(batch) == 0 && time.Since(lastSend) >= s.opts.KeepaliveInterval
		if len(batch) == 0 && !idle {
			t.cond.Wait()
			t.mu.Unlock()
			continue
		}
		t.mu.Unlock()

		resp := &proto.FetchResponse{RpcId: randomHex(8)}
		if len(batch) > 0 {
			resp.Events = batch
			last, _ := replaySeq(batch[len(batch)-1].ReplayId)
			cursor = last
			resp.LatestReplayId = replayBytes(last)
		} else {
			// Keepalive. The subscription has no undelivered events only when
			// the client has outstanding demand; then the head is safe to report.
			pmu.Lock()
			hasDemand := pending > 0
			pmu.Unlock()
			if hasDemand {
				cursor = head
			}
			resp.LatestReplayId = replayBytes(cursor)
		}
		pmu.Lock()
		pending -= len(batch)
		resp.PendingNumRequested = int32(pending)
		pmu.Unlock()

		if err := stream.Send(resp); err != nil {
			return err
		}
		lastSend = time.Now()
		delivered += len(batch)

		if drop := s.GetFaults().DropStreamAfterEvents; drop > 0 && delivered >= drop {
			log.Debugf("mock: dropping subscription on %s after %d events", t.name, delivered)
			return status.Error(codes.Unavailable, "injected stream drop")
		}
	}
}

func (s *Server) validSession(ctx context.Context) bool {
	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get("accesstoken")
	return len(v) > 0 && s.validToken(v[0])
}

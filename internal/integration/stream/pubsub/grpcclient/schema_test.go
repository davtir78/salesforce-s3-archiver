package grpcclient

import (
	"context"
	"errors"
	"testing"

	"github.com/davtir78/salesforce-s3-archiver/internal/integration/stream/pubsub/proto"
	"google.golang.org/grpc"
)

// fakePubSub serves GetSchema from a function; other methods are not used.
type fakePubSub struct {
	proto.PubSubClient
	getSchema func() (*proto.SchemaInfo, error)
	calls     int
}

func (f *fakePubSub) GetSchema(ctx context.Context, in *proto.SchemaRequest, opts ...grpc.CallOption) (*proto.SchemaInfo, error) {
	f.calls++
	return f.getSchema()
}

func eventWithSchema(id string) *proto.ConsumerEvent {
	return &proto.ConsumerEvent{Event: &proto.ProducerEvent{SchemaId: id}}
}

// A failed schema request is transient (the subscriber reconnects); a schema
// that cannot be parsed is not, or the collector would retry it forever.
func TestSchemaErrorsAreClassified(t *testing.T) {
	fake := &fakePubSub{getSchema: func() (*proto.SchemaInfo, error) { return nil, errors.New("unavailable") }}
	c := &PubSubClient{pubSubClient: fake, schemaCache: map[string]*schemaEntry{}}

	_, _, err := c.DecodeEvent(context.Background(), eventWithSchema("s1"))
	var fetchErr *SchemaFetchError
	if !errors.As(err, &fetchErr) {
		t.Fatalf("a failed GetSchema call must be a SchemaFetchError, got %v", err)
	}

	fake.getSchema = func() (*proto.SchemaInfo, error) {
		return &proto.SchemaInfo{SchemaJson: `{"type": "record", "name": "Broken", "fields": [{"name": "x", "type": "no-such-type"}]}`}, nil
	}
	_, _, err = c.DecodeEvent(context.Background(), eventWithSchema("s1"))
	if err == nil {
		t.Fatal("an unparseable schema must fail")
	}
	if errors.As(err, &fetchErr) {
		t.Errorf("an unparseable schema must not be retried as a fetch error: %v", err)
	}
	if _, cached := c.schemaCache["s1"]; cached {
		t.Errorf("a schema that failed must not be cached")
	}
}

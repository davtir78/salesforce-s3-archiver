package grpcclient

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
)

const testSchema = `{
  "type": "record", "name": "LoginEventStream", "namespace": "com.sforce.eventbus",
  "fields": [
    {"name": "CreatedDate", "type": "long"},
    {"name": "CreatedById", "type": "string"},
    {"name": "EventIdentifier", "type": ["null", "string"], "default": null},
    {"name": "Score", "type": ["null", "double"], "default": null},
    {"name": "IsSuccess", "type": ["null", "boolean"], "default": null},
    {"name": "Missing", "type": ["null", "string"], "default": null},
    {"name": "Attributes", "type": {"type": "map", "values": "string"}},
    {"name": "Tags", "type": {"type": "array", "items": ["null", "string"]}},
    {"name": "Policy", "type": ["null", {"type": "record", "name": "PolicyInfo", "fields": [
      {"name": "Outcome", "type": {"type": "enum", "name": "Outcome", "symbols": ["Allow", "Block"]}},
      {"name": "Detail", "type": ["null", "string"], "default": null}
    ]}], "default": null},
    {"name": "Previous", "type": ["null", "PolicyInfo"], "default": null},
    {"name": "Hash", "type": {"type": "fixed", "name": "Hash4", "size": 4}},
    {"name": "Raw", "type": "bytes"},
    {"name": "At", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null},
    {"name": "Day", "type": {"type": "int", "logicalType": "date"}}
  ]
}`

func TestFlattenerRoundTrip(t *testing.T) {
	codec, err := goavro.NewCodec(testSchema)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 15, 1, 2, 3, 4_000_000, time.UTC)
	native := map[string]any{
		"CreatedDate":     int64(1757900000000),
		"CreatedById":     "005xx",
		"EventIdentifier": goavro.Union("string", "evt-1"),
		"Score":           goavro.Union("double", 42.5),
		"IsSuccess":       goavro.Union("boolean", false),
		"Missing":         nil,
		// A real map whose only key looks like an Avro type name must not be unwrapped.
		"Attributes": map[string]any{"string": "not a union"},
		"Tags":       []any{goavro.Union("string", "a"), nil},
		"Policy": goavro.Union("com.sforce.eventbus.PolicyInfo", map[string]any{
			"Outcome": "Block",
			"Detail":  goavro.Union("string", "risky"),
		}),
		"Previous": nil,
		"Hash":     []byte{1, 2, 3, 4},
		"Raw":      []byte("raw"),
		"At":       goavro.Union("long.timestamp-millis", at),
		"Day":      at,
	}
	bin, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _, err := codec.NativeFromBinary(bin)
	if err != nil {
		t.Fatal(err)
	}

	f, err := NewFlattener(testSchema)
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.Flatten(decoded.(map[string]any))
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]any{
		"CreatedDate":     int64(1757900000000),
		"CreatedById":     "005xx",
		"EventIdentifier": "evt-1",
		"Score":           42.5,
		"IsSuccess":       false,
		"Missing":         nil,
		"Attributes":      map[string]any{"string": "not a union"},
		"Tags":            []any{"a", nil},
		"Policy":          map[string]any{"Outcome": "Block", "Detail": "risky"},
		"Previous":        nil,
		"Hash":            []byte{1, 2, 3, 4},
		"Raw":             []byte("raw"),
		"At":              at.UnixMilli(),
		"Day":             "2026-09-15",
	}
	for k, w := range want {
		if !reflect.DeepEqual(got[k], w) {
			t.Errorf("%s = %#v, want %#v", k, got[k], w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d fields, want %d (every schema field must be kept)", len(got), len(want))
	}
	if _, err := json.Marshal(got); err != nil {
		t.Errorf("flattened record must be JSON-encodable: %v", err)
	}
}

func TestFlattenerRejectsUnknownBranch(t *testing.T) {
	f, err := NewFlattener(`{"type":"record","name":"R","fields":[{"name":"x","type":["null","string"]}]}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Flatten(map[string]any{"x": map[string]any{"long": int64(1)}}); err == nil {
		t.Error("a union branch not declared in the schema must be an error, not guessed")
	}
}

func TestNewFlattenerInvalidSchema(t *testing.T) {
	for _, s := range []string{`not json`, `{"type":"record","name":"R","fields":[{"name":"x","type":{"type":"wat"}}]}`} {
		if _, err := NewFlattener(s); err == nil {
			t.Errorf("expected error for schema %s", s)
		}
	}
}

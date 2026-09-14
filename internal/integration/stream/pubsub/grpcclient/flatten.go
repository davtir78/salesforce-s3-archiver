package grpcclient

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// Flattener converts goavro native values into plain JSON-friendly values
// using the writer schema, so union wrappers are only unwrapped where the
// schema declares a union. Guessing from map keys would reshape real Avro maps
// that happen to have a single key such as "string".
type Flattener struct {
	root  *avroType
	named map[string]*avroType
}

type avroType struct {
	kind        string // primitive name, record, enum, array, map, fixed, union
	name        string // full name for named types
	logicalType string
	fields      []avroField
	items       *avroType
	values      *avroType
	branches    []*avroType
	ref         string // unresolved named reference
}

type avroField struct {
	name string
	typ  *avroType
}

// NewFlattener parses an Avro schema (JSON).
func NewFlattener(schemaJSON string) (*Flattener, error) {
	var raw any
	if err := json.Unmarshal([]byte(schemaJSON), &raw); err != nil {
		return nil, fmt.Errorf("parsing avro schema: %w", err)
	}
	f := &Flattener{named: map[string]*avroType{}}
	root, err := f.parse(raw, "")
	if err != nil {
		return nil, err
	}
	f.root = root
	return f, nil
}

var avroPrimitives = map[string]bool{
	"null": true, "boolean": true, "int": true, "long": true,
	"float": true, "double": true, "bytes": true, "string": true,
}

func fullName(name, namespace string) string {
	if name == "" || strings.Contains(name, ".") || namespace == "" {
		return name
	}
	return namespace + "." + name
}

func (f *Flattener) parse(raw any, namespace string) (*avroType, error) {
	switch t := raw.(type) {
	case string:
		if avroPrimitives[t] {
			return &avroType{kind: t}, nil
		}
		return &avroType{ref: fullName(t, namespace)}, nil
	case []any:
		u := &avroType{kind: "union"}
		for _, b := range t {
			bt, err := f.parse(b, namespace)
			if err != nil {
				return nil, err
			}
			u.branches = append(u.branches, bt)
		}
		return u, nil
	case map[string]any:
		kind, _ := t["type"].(string)
		logical, _ := t["logicalType"].(string)
		ns := namespace
		if n, ok := t["namespace"].(string); ok && n != "" {
			ns = n
		}
		switch kind {
		case "record", "error":
			name, _ := t["name"].(string)
			full := fullName(name, ns)
			if i := strings.LastIndex(full, "."); i >= 0 {
				ns = full[:i]
			}
			rt := &avroType{kind: "record", name: full}
			f.named[full] = rt
			fields, _ := t["fields"].([]any)
			for _, fr := range fields {
				fm, ok := fr.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("invalid field in record %s", full)
				}
				fname, _ := fm["name"].(string)
				ft, err := f.parse(fm["type"], ns)
				if err != nil {
					return nil, err
				}
				rt.fields = append(rt.fields, avroField{name: fname, typ: ft})
			}
			return rt, nil
		case "enum", "fixed":
			name, _ := t["name"].(string)
			nt := &avroType{kind: kind, name: fullName(name, ns), logicalType: logical}
			f.named[nt.name] = nt
			return nt, nil
		case "array":
			it, err := f.parse(t["items"], ns)
			if err != nil {
				return nil, err
			}
			return &avroType{kind: "array", items: it}, nil
		case "map":
			vt, err := f.parse(t["values"], ns)
			if err != nil {
				return nil, err
			}
			return &avroType{kind: "map", values: vt}, nil
		default:
			if avroPrimitives[kind] {
				return &avroType{kind: kind, logicalType: logical}, nil
			}
			return nil, fmt.Errorf("unsupported avro type %v", t["type"])
		}
	default:
		return nil, fmt.Errorf("invalid avro schema node %T", raw)
	}
}

func (f *Flattener) resolve(t *avroType) *avroType {
	if t.ref == "" {
		return t
	}
	if n, ok := f.named[t.ref]; ok {
		return n
	}
	// Unqualified reference to a type in another namespace.
	for name, n := range f.named {
		if strings.HasSuffix(name, "."+t.ref) {
			return n
		}
	}
	return t
}

// branchName is the key goavro uses for a union branch.
func (t *avroType) branchName() string {
	switch t.kind {
	case "record", "enum", "fixed":
		return t.name
	}
	if t.logicalType != "" {
		return t.kind + "." + t.logicalType
	}
	return t.kind
}

// Flatten converts a decoded record.
func (f *Flattener) Flatten(native map[string]any) (map[string]any, error) {
	out, err := f.value(f.root, native)
	if err != nil {
		return nil, err
	}
	m, ok := out.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("schema root is not a record")
	}
	return m, nil
}

func (f *Flattener) value(t *avroType, v any) (any, error) {
	t = f.resolve(t)
	if v == nil {
		return nil, nil
	}
	switch t.kind {
	case "union":
		m, ok := v.(map[string]any)
		if !ok || len(m) != 1 {
			return nil, fmt.Errorf("expected a union value, got %T", v)
		}
		for key, inner := range m {
			for _, b := range t.branches {
				if f.resolve(b).branchName() == key {
					return f.value(b, inner)
				}
			}
			return nil, fmt.Errorf("union branch %q not in schema", key)
		}
		return nil, fmt.Errorf("empty union value")
	case "record":
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected record %s, got %T", t.name, v)
		}
		out := make(map[string]any, len(m))
		for _, field := range t.fields {
			fv, present := m[field.name]
			if !present {
				continue
			}
			conv, err := f.value(field.typ, fv)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", t.name, field.name, err)
			}
			out[field.name] = conv
		}
		return out, nil
	case "array":
		items, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("expected array, got %T", v)
		}
		out := make([]any, len(items))
		for i, item := range items {
			conv, err := f.value(t.items, item)
			if err != nil {
				return nil, err
			}
			out[i] = conv
		}
		return out, nil
	case "map":
		m, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected map, got %T", v)
		}
		out := make(map[string]any, len(m))
		for k, inner := range m {
			conv, err := f.value(t.values, inner)
			if err != nil {
				return nil, err
			}
			out[k] = conv
		}
		return out, nil
	}
	// Primitives, enums, fixed and logical types.
	switch x := v.(type) {
	case time.Time:
		if t.logicalType == "date" {
			return x.UTC().Format("2006-01-02"), nil
		}
		return x.UnixMilli(), nil
	case time.Duration:
		return x.Milliseconds(), nil
	case *big.Rat:
		return x.FloatString(18), nil
	}
	return v, nil
}

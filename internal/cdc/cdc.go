// Package cdc reads Debezium change events: the JSON envelope with "op",
// "before", "after" and "source", with or without the schema wrapper.
package cdc

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Op is the kind of change, as Debezium spells it.
type Op byte

const (
	OpCreate   Op = 'c'
	OpUpdate   Op = 'u'
	OpDelete   Op = 'd'
	OpRead     Op = 'r' // snapshot
	OpTruncate Op = 't'
)

func (o Op) String() string {
	switch o {
	case OpCreate:
		return "insert"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	case OpRead:
		return "read"
	case OpTruncate:
		return "truncate"
	}
	return "?"
}

// Field is one column of a row. Fields keep the order they had in the
// message, which is the table's column order.
type Field struct {
	Name  string
	Value json.RawMessage
}

// Event is one decoded change.
type Event struct {
	Op     Op
	Source string // db.schema.table, or the connector name
	Before []Field
	After  []Field
	Time   time.Time // when the change happened in the database, if known
}

// Parse decodes value as a Debezium change event. It reports false for
// anything else, so callers can fall back to showing the raw message.
func Parse(value []byte) (Event, bool) {
	top, err := object(value)
	if err != nil {
		return Event{}, false
	}
	payload := top
	if raw, ok := get(top, "payload"); ok {
		if _, hasSchema := get(top, "schema"); hasSchema {
			if payload, err = object(raw); err != nil {
				return Event{}, false
			}
		}
	}

	var op string
	raw, ok := get(payload, "op")
	if !ok || json.Unmarshal(raw, &op) != nil || len(op) != 1 || !strings.Contains("cudrt", op) {
		return Event{}, false
	}
	e := Event{Op: Op(op[0])}
	for _, side := range []struct {
		name string
		dst  *[]Field
	}{{"before", &e.Before}, {"after", &e.After}} {
		if raw, ok := get(payload, side.name); ok && !isNull(raw) {
			if *side.dst, err = object(raw); err != nil {
				return Event{}, false
			}
		}
	}

	var ms int64
	if raw, ok := get(payload, "source"); ok {
		if src, err := object(raw); err == nil {
			e.Source = sourceName(src)
			if raw, ok := get(src, "ts_ms"); ok && json.Unmarshal(raw, &ms) != nil {
				ms = 0
			}
		}
	}
	if raw, ok := get(payload, "ts_ms"); ms == 0 && ok && json.Unmarshal(raw, &ms) != nil {
		ms = 0
	}
	if ms > 0 {
		e.Time = time.UnixMilli(ms)
	}
	return e, true
}

func sourceName(src []Field) string {
	var parts []string
	for _, k := range []string{"db", "schema", "table"} {
		var s string
		if raw, ok := get(src, k); ok && json.Unmarshal(raw, &s) == nil && s != "" {
			parts = append(parts, s)
		}
	}
	if len(parts) == 0 {
		var s string
		if raw, ok := get(src, "name"); ok && json.Unmarshal(raw, &s) == nil {
			return s
		}
	}
	return strings.Join(parts, ".")
}

// Kind says how a field changed between before and after.
type Kind int

const (
	Same Kind = iota
	Changed
	Added   // only in after (inserts, new columns)
	Removed // only in before (deletes, dropped columns)
)

// Change is one field of an event with its old and new value.
type Change struct {
	Name          string
	Before, After json.RawMessage
	Kind          Kind
}

// Changes lines up before and after field by field, in column order.
func (e Event) Changes() []Change {
	var out []Change
	seen := map[string]bool{}
	for _, a := range e.After {
		seen[a.Name] = true
		b, ok := get(e.Before, a.Name)
		switch {
		case e.Before == nil || !ok:
			out = append(out, Change{a.Name, nil, a.Value, Added})
		case equal(b, a.Value):
			out = append(out, Change{a.Name, b, a.Value, Same})
		default:
			out = append(out, Change{a.Name, b, a.Value, Changed})
		}
	}
	for _, b := range e.Before {
		if !seen[b.Name] {
			out = append(out, Change{b.Name, b.Value, nil, Removed})
		}
	}
	return out
}

// Show renders a JSON value for display: strings without quotes, the rest
// as compact JSON.
func Show(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var b bytes.Buffer
	if json.Compact(&b, raw) == nil {
		return b.String()
	}
	return string(raw)
}

// object decodes a JSON object, keeping its keys in order.
func object(data []byte) ([]Field, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return nil, errors.New("not a JSON object")
	}
	fields := []Field{}
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return nil, err
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		fields = append(fields, Field{t.(string), v})
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	return fields, nil
}

func get(fields []Field, name string) (json.RawMessage, bool) {
	for _, f := range fields {
		if f.Name == name {
			return f.Value, true
		}
	}
	return nil, false
}

func isNull(raw json.RawMessage) bool { return string(bytes.TrimSpace(raw)) == "null" }

func equal(a, b json.RawMessage) bool {
	var x, y bytes.Buffer
	if json.Compact(&x, a) != nil || json.Compact(&y, b) != nil {
		return bytes.Equal(a, b)
	}
	return bytes.Equal(x.Bytes(), y.Bytes())
}

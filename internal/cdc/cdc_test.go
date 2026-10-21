package cdc

import (
	"testing"
	"time"
)

func TestParseWithSchema(t *testing.T) {
	msg := `{"schema":{"type":"struct"},"payload":{
		"before":{"id":1,"email":"ana@old.de","plan":"free"},
		"after":{"id":1,"email":"ana@new.de","plan":"free","city":"Paderborn"},
		"source":{"db":"inventory","schema":"public","table":"customers","ts_ms":1760000000000},
		"op":"u","ts_ms":1760000000500}}`
	e, ok := Parse([]byte(msg))
	if !ok {
		t.Fatal("not recognised as CDC")
	}
	if e.Op != OpUpdate || e.Source != "inventory.public.customers" || !e.Time.Equal(time.UnixMilli(1760000000000)) {
		t.Errorf("got %+v", e)
	}
	ch := e.Changes()
	want := []struct {
		name string
		kind Kind
	}{{"id", Same}, {"email", Changed}, {"plan", Same}, {"city", Added}}
	if len(ch) != len(want) {
		t.Fatalf("changes = %+v", ch)
	}
	for i, w := range want {
		if ch[i].Name != w.name || ch[i].Kind != w.kind {
			t.Errorf("change %d = %s/%v, want %s/%v", i, ch[i].Name, ch[i].Kind, w.name, w.kind)
		}
	}
	if Show(ch[1].Before) != "ana@old.de" || Show(ch[1].After) != "ana@new.de" {
		t.Errorf("show: %q → %q", Show(ch[1].Before), Show(ch[1].After))
	}
}

func TestParsePayloadOnlyDelete(t *testing.T) {
	e, ok := Parse([]byte(`{"before":{"id":7,"name":"lamp"},"after":null,"source":{"db":"shop","table":"products"},"op":"d","ts_ms":5}`))
	if !ok || e.Op != OpDelete || e.After != nil || e.Source != "shop.products" {
		t.Fatalf("got %+v, %v", e, ok)
	}
	for _, c := range e.Changes() {
		if c.Kind != Removed {
			t.Errorf("%s: kind %v, want Removed", c.Name, c.Kind)
		}
	}
	if !e.Time.Equal(time.UnixMilli(5)) {
		t.Errorf("time falls back to payload ts_ms, got %v", e.Time)
	}
}

func TestNotCDC(t *testing.T) {
	for _, msg := range []string{
		`hello`,
		`[1,2,3]`,
		`{"level":"info","msg":"started"}`,
		`{"op":"jump","after":{}}`,
		`{"schema":{},"payload":"x"}`,
	} {
		if _, ok := Parse([]byte(msg)); ok {
			t.Errorf("%s should not be CDC", msg)
		}
	}
}

func TestEqualIgnoresFormatting(t *testing.T) {
	e, _ := Parse([]byte(`{"op":"u","before":{"tags":["a", "b"]},"after":{"tags":["a","b"]}}`))
	if c := e.Changes(); len(c) != 1 || c[0].Kind != Same {
		t.Errorf("got %+v", c)
	}
}

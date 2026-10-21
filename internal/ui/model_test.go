package ui

import (
	"context"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/Dem1241/puddle/internal/broker"
	"github.com/Dem1241/puddle/internal/cdc"
)

type fakeSource struct {
	topics []broker.Topic
	ch     chan broker.Message
	tailed string
}

func (f *fakeSource) Topics(context.Context) ([]broker.Topic, error) { return f.topics, nil }

func (f *fakeSource) Tail(_ context.Context, topic string, _ int64) (<-chan broker.Message, error) {
	f.tailed = topic
	return f.ch, nil
}

func msg(offset int64, sec int, value string) broker.Message {
	m := broker.Message{Topic: "shop.public.orders", Offset: offset, Key: []byte(`{"id":5003}`), Value: []byte(value), Time: time.Unix(int64(1760000000+sec), 0)}
	m.Event, m.IsCDC = cdc.Parse(m.Value)
	return m
}

// run feeds msgs to the model and follows up on the commands it returns that
// finish quickly (fetching topics, opening a stream). Ticks and waiting for
// messages block, so they are dropped after a moment.
func run(t *testing.T, m Model, msgs ...tea.Msg) Model {
	t.Helper()
	for _, in := range msgs {
		next, cmd := m.Update(in)
		m = next.(Model)
		for cmd != nil {
			out := quick(cmd)
			if batch, ok := out.(tea.BatchMsg); ok {
				for _, c := range batch {
					if c != nil {
						if o := quick(c); o != nil {
							next, _ = m.Update(o)
							m = next.(Model)
						}
					}
				}
				break
			}
			switch out.(type) {
			case topicsMsg, openedMsg:
				next, cmd = m.Update(out)
				m = next.(Model)
			default:
				cmd = nil
			}
		}
	}
	return m
}

func quick(cmd tea.Cmd) tea.Msg {
	ch := make(chan tea.Msg, 1)
	go func() { ch <- cmd() }()
	select {
	case msg := <-ch:
		switch msg.(type) {
		case topicsMsg, openedMsg, tea.BatchMsg:
			return msg
		}
	case <-time.After(50 * time.Millisecond):
	}
	return nil
}

func key(s string) tea.KeyPressMsg {
	switch s {
	case "enter":
		return tea.KeyPressMsg{Code: tea.KeyEnter}
	case "esc":
		return tea.KeyPressMsg{Code: tea.KeyEscape}
	case "space":
		return tea.KeyPressMsg{Code: tea.KeySpace, Text: " "}
	case "up":
		return tea.KeyPressMsg{Code: tea.KeyUp}
	}
	return tea.KeyPressMsg{Code: rune(s[0]), Text: s}
}

func screenText(m Model) string { return ansi.Strip(m.View().Content) }

func TestTopicsAndStream(t *testing.T) {
	src := &fakeSource{
		topics: []broker.Topic{{Name: "app.logs", Partitions: 3, Messages: 12}, {Name: "shop.public.orders", Partitions: 3, Messages: 1234567}},
		ch:     make(chan broker.Message, 10),
	}
	m := run(t, New(src, "localhost:9092", 100, ""), tea.WindowSizeMsg{Width: 140, Height: 30})
	m = run(t, m, topicsMsg{src.topics, nil})

	s := screenText(m)
	for _, want := range []string{"☂ puddle", "localhost:9092", "2 topics", "app.logs", "shop.public.orders", "1.2M"} {
		if !strings.Contains(s, want) {
			t.Errorf("topics screen lacks %q:\n%s", want, s)
		}
	}
	if lines := strings.Split(s, "\n"); len(lines) != 30 {
		t.Errorf("screen has %d lines, want 30", len(lines))
	}

	// filter to the shop table and open it
	m = run(t, m, key("/"), key("s"), key("h"), key("o"), key("enter"))
	if s := screenText(m); strings.Contains(s, "app.logs") {
		t.Errorf("filter should hide app.logs:\n%s", s)
	}
	m = run(t, m, key("enter"))
	if src.tailed != "shop.public.orders" || m.screen != streamScreen {
		t.Fatalf("opened %q, screen %v", src.tailed, m.screen)
	}

	insert := msg(1, 1, `{"op":"c","after":{"id":5003,"status":"pending","total":269.7},"source":{"db":"shop","schema":"public","table":"orders"}}`)
	update := msg(2, 5, `{"op":"u","before":{"id":5003,"status":"pending","total":269.7},"after":{"id":5003,"status":"paid","total":269.7},"source":{"db":"shop","schema":"public","table":"orders"}}`)
	m = run(t, m, recordsMsg{m.stream, []broker.Message{update, insert}})

	s = screenText(m)
	for _, want := range []string{"● live", "2 messages", "insert", "id=5003 · status", "pending → paid", "partition 0 · offset 2"} {
		if !strings.Contains(s, want) {
			t.Errorf("stream screen lacks %q:\n%s", want, s)
		}
	}
	if strings.Index(s, "insert") > strings.Index(s, "id=5003 · status") {
		t.Errorf("messages should be in time order:\n%s", s)
	}

	// only changed fields
	m = run(t, m, key("c"))
	if s := screenText(m); strings.Contains(s, "269.7") || !strings.Contains(s, "2 unchanged fields hidden") {
		t.Errorf("changed-only view:\n%s", s)
	}

	// pausing keeps the selection while new messages arrive
	m = run(t, m, key("up"))
	if m.follow {
		t.Fatal("moving up should pause")
	}
	later := msg(3, 9, `{"level":"info","msg":"hello"}`)
	m = run(t, m, recordsMsg{m.stream, []broker.Message{later}})
	if s := screenText(m); !strings.Contains(s, "❚❚ paused") || !strings.Contains(s, "offset 1") {
		t.Errorf("paused selection moved:\n%s", s)
	}

	// esc goes back to the topic list
	m = run(t, m, key("esc"))
	if m.screen != topicsScreen || m.stream != nil {
		t.Errorf("esc should close the stream")
	}
}

func TestNarrowTerminalStacksPanes(t *testing.T) {
	src := &fakeSource{ch: make(chan broker.Message, 1)}
	m := run(t, New(src, "demo", 10, "shop.public.orders"), tea.WindowSizeMsg{Width: 80, Height: 24})
	next, _ := m.Update(openedMsg{topic: "shop.public.orders", ch: src.ch, cancel: func() {}})
	m = next.(Model)
	m = run(t, m, recordsMsg{m.stream, []broker.Message{msg(1, 1, `{"op":"d","before":{"id":1},"after":null}`)}})
	s := screenText(m)
	for _, line := range strings.Split(s, "\n") {
		if w := ansi.StringWidth(line); w > 80 {
			t.Errorf("line is %d wide: %q", w, line)
		}
	}
	if !strings.Contains(s, "delete") {
		t.Errorf("delete not shown:\n%s", s)
	}
}

func TestValueLines(t *testing.T) {
	j := ansi.Strip(strings.Join(valueLines([]byte(`{"a":1,"b":[true,null]}`), 40), "\n"))
	if !strings.Contains(j, `"a": 1`) || !strings.Contains(j, "true") {
		t.Errorf("json:\n%s", j)
	}
	if b := ansi.Strip(valueLines([]byte{0xff, 0x00, 0x01}, 40)[0]); b != "binary · 3 bytes" {
		t.Errorf("binary: %q", b)
	}
	if e := ansi.Strip(valueLines(nil, 40)[0]); !strings.Contains(e, "tombstone") {
		t.Errorf("empty: %q", e)
	}
	if k := keySummary([]byte(`{"schema":{},"payload":{"id":7,"region":"eu"}}`)); k != "id=7 region=eu" {
		t.Errorf("key: %q", k)
	}
}

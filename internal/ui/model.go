// Package ui is puddle's terminal interface: a topic list and a live view of
// one topic, with Debezium changes shown field by field.
package ui

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/Dem1241/puddle/internal/broker"
)

// Source is where topics and messages come from; *broker.Client is one.
type Source interface {
	Topics(ctx context.Context) ([]broker.Topic, error)
	Tail(ctx context.Context, topic string, last int64) (<-chan broker.Message, error)
}

type screen int

const (
	topicsScreen screen = iota
	streamScreen
)

// maxMessages caps what a stream keeps in memory; the oldest go first.
const maxMessages = 5000

type Model struct {
	src   Source
	label string // what we are connected to, for the header
	last  int64  // how many old records per partition to show when opening a topic

	width, height int
	screen        screen

	topics    []broker.Topic
	topicsErr error
	topicIdx  int // index into the visible (filtered) topics

	topic       string
	msgs        []broker.Message
	msgIdx      int // index into the visible messages
	follow      bool
	onlyChanged bool
	stream      <-chan broker.Message
	cancel      context.CancelFunc
	streamErr   error
	detailTop   int // scroll position of the detail pane

	filtering bool
	filter    string // for the current screen
	openFirst string // topic to open as soon as we start
}

// New creates the interface. If topic is set, it opens right away.
func New(src Source, label string, last int64, topic string) Model {
	return Model{src: src, label: label, last: last, follow: true, openFirst: topic}
}

type (
	topicsMsg struct {
		topics []broker.Topic
		err    error
	}
	tickMsg   struct{}
	openedMsg struct {
		topic  string
		ch     <-chan broker.Message
		cancel context.CancelFunc
		err    error
	}
	recordsMsg struct {
		ch   <-chan broker.Message
		msgs []broker.Message
	}
	closedMsg struct{ ch <-chan broker.Message }
)

func (m Model) Init() tea.Cmd {
	cmds := []tea.Cmd{fetchTopics(m.src), tick()}
	if m.openFirst != "" {
		cmds = append(cmds, openTopic(m.src, m.openFirst, m.last))
	}
	return tea.Batch(cmds...)
}

func fetchTopics(src Source) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		t, err := src.Topics(ctx)
		return topicsMsg{t, err}
	}
}

func tick() tea.Cmd {
	return tea.Tick(2*time.Second, func(time.Time) tea.Msg { return tickMsg{} })
}

func openTopic(src Source, topic string, last int64) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithCancel(context.Background())
		ch, err := src.Tail(ctx, topic, last)
		if err != nil {
			cancel()
		}
		return openedMsg{topic, ch, cancel, err}
	}
}

// wait reads the next messages: it blocks for one, then takes whatever else
// is already there, so a busy topic redraws once per batch.
func wait(ch <-chan broker.Message) tea.Cmd {
	return func() tea.Msg {
		first, ok := <-ch
		if !ok {
			return closedMsg{ch}
		}
		batch := []broker.Message{first}
		for len(batch) < 500 {
			select {
			case msg, ok := <-ch:
				if !ok {
					return recordsMsg{ch, batch}
				}
				batch = append(batch, msg)
			default:
				return recordsMsg{ch, batch}
			}
		}
		return recordsMsg{ch, batch}
	}
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height

	case tickMsg:
		if m.screen == topicsScreen {
			return m, tea.Batch(fetchTopics(m.src), tick())
		}
		return m, tick()

	case topicsMsg:
		m.topics, m.topicsErr = msg.topics, msg.err
		m.topicIdx = clamp(m.topicIdx, 0, len(m.visibleTopics())-1)

	case openedMsg:
		if msg.err != nil {
			m.streamErr = msg.err
			return m, nil
		}
		m.stopStream()
		m.screen, m.topic = streamScreen, msg.topic
		m.stream, m.cancel, m.streamErr = msg.ch, msg.cancel, nil
		m.msgs, m.msgIdx, m.detailTop = nil, 0, 0
		m.follow, m.filter, m.filtering = true, "", false
		return m, wait(msg.ch)

	case recordsMsg:
		if msg.ch != m.stream {
			return m, nil // from a topic we already left
		}
		// Partitions arrive interleaved; keep the list in time order, and keep
		// a paused selection on the same message while rows move around it.
		var selected *broker.Message
		if vis := m.visibleMsgs(); !m.follow && m.msgIdx >= 0 && m.msgIdx < len(vis) {
			selected = &vis[m.msgIdx]
		}
		for _, r := range msg.msgs {
			i := sort.Search(len(m.msgs), func(i int) bool { return when(m.msgs[i]).After(when(r)) })
			m.msgs = slices.Insert(m.msgs, i, r)
		}
		if over := len(m.msgs) - maxMessages; over > 0 {
			m.msgs = m.msgs[over:]
		}
		vis := m.visibleMsgs()
		m.msgIdx = len(vis) - 1
		if selected != nil {
			sel := *selected
			if i := slices.IndexFunc(vis, func(x broker.Message) bool {
				return x.Partition == sel.Partition && x.Offset == sel.Offset
			}); i >= 0 {
				m.msgIdx = i
			}
		}
		return m, wait(msg.ch)

	case closedMsg:
		if msg.ch == m.stream {
			m.stream = nil
		}

	case tea.KeyPressMsg:
		return m.key(msg)
	}
	return m, nil
}

func (m *Model) stopStream() {
	if m.cancel != nil {
		m.cancel()
	}
	m.stream, m.cancel = nil, nil
}

func (m Model) key(k tea.KeyPressMsg) (tea.Model, tea.Cmd) {
	s := k.String()
	if s == "ctrl+c" {
		m.stopStream()
		return m, tea.Quit
	}
	if m.filtering {
		switch s {
		case "esc":
			m.filtering, m.filter = false, ""
		case "enter":
			m.filtering = false
		case "backspace":
			if r := []rune(m.filter); len(r) > 0 {
				m.filter = string(r[:len(r)-1])
			}
		default:
			if k.Text != "" {
				m.filter += k.Text
			}
		}
		m.resetSelection()
		return m, nil
	}
	if s == "q" {
		m.stopStream()
		return m, tea.Quit
	}
	if s == "/" {
		m.filtering = true
		return m, nil
	}

	if m.screen == topicsScreen {
		n := len(m.visibleTopics())
		switch s {
		case "up", "k":
			m.topicIdx = clamp(m.topicIdx-1, 0, n-1)
		case "down", "j":
			m.topicIdx = clamp(m.topicIdx+1, 0, n-1)
		case "home", "g":
			m.topicIdx = 0
		case "end", "G":
			m.topicIdx = max(0, n-1)
		case "r":
			return m, fetchTopics(m.src)
		case "esc":
			m.filter = ""
		case "enter":
			if t := m.visibleTopics(); len(t) > 0 {
				return m, openTopic(m.src, t[m.topicIdx].Name, m.last)
			}
		}
		return m, nil
	}

	n := len(m.visibleMsgs())
	page := max(1, m.listHeight()-1)
	move := func(to int) {
		m.msgIdx = clamp(to, 0, n-1)
		m.follow = m.msgIdx == n-1
		m.detailTop = 0
	}
	switch s {
	case "up", "k":
		move(m.msgIdx - 1)
	case "down", "j":
		move(m.msgIdx + 1)
	case "pgup":
		move(m.msgIdx - page)
	case "pgdown":
		move(m.msgIdx + page)
	case "home", "g":
		move(0)
	case "end", "G":
		move(n - 1)
	case "space":
		m.follow = !m.follow
		if m.follow {
			move(n - 1)
		}
	case "c":
		m.onlyChanged = !m.onlyChanged
		m.detailTop = 0
	case "ctrl+d", "J":
		m.detailTop += 5
	case "ctrl+u", "K":
		m.detailTop = max(0, m.detailTop-5)
	case "esc":
		if m.filter != "" {
			m.filter = ""
			m.resetSelection()
			return m, nil
		}
		m.stopStream()
		m.screen, m.filter = topicsScreen, ""
		return m, fetchTopics(m.src)
	}
	return m, nil
}

// resetSelection keeps the selection valid after the filter changed.
func (m *Model) resetSelection() {
	if m.screen == topicsScreen {
		m.topicIdx = 0
		return
	}
	m.msgIdx = len(m.visibleMsgs()) - 1
	m.follow, m.detailTop = true, 0
}

func (m Model) visibleTopics() []broker.Topic {
	if m.filter == "" || m.screen != topicsScreen {
		return m.topics
	}
	var out []broker.Topic
	for _, t := range m.topics {
		if strings.Contains(strings.ToLower(t.Name), strings.ToLower(m.filter)) {
			out = append(out, t)
		}
	}
	return out
}

func (m Model) visibleMsgs() []broker.Message {
	if m.filter == "" || m.screen != streamScreen {
		return m.msgs
	}
	f := strings.ToLower(m.filter)
	var out []broker.Message
	for _, msg := range m.msgs {
		hay := strings.ToLower(string(msg.Key) + " " + string(msg.Value) + " " + msg.Event.Source)
		if strings.Contains(hay, f) {
			out = append(out, msg)
		}
	}
	return out
}

func (m Model) View() tea.View {
	v := tea.NewView("")
	v.AltScreen = true
	v.WindowTitle = "puddle"
	if m.width == 0 {
		return v
	}
	body := m.topicsView()
	if m.screen == streamScreen {
		body = m.streamView()
	}
	v.Content = lipgloss.JoinVertical(lipgloss.Left, m.header(), body, m.footer())
	return v
}

func (m Model) bodyHeight() int { return max(3, m.height-2) }

func (m Model) header() string {
	left := " " + sTitle.Render("☂ puddle") + sDim.Render(" · ") + sSub.Render(m.label)
	var right string
	if m.screen == topicsScreen {
		right = sSub.Render(fmt.Sprintf("%d topic%s", len(m.topics), plural(len(m.topics))))
	} else {
		left += sDim.Render(" · ") + sText.Render(m.topic)
		state := sAfter.Render("● live")
		if !m.follow {
			state = sDim.Render("❚❚ paused")
		}
		if m.stream == nil {
			state = sErr.Render("○ stopped")
		}
		right = state + sDim.Render(fmt.Sprintf("  %d messages", len(m.msgs)))
	}
	return spread(left, right+" ", m.width)
}

func (m Model) footer() string {
	if m.filtering {
		return " " + sPink.Render("/") + sText.Render(m.filter) + sPink.Render("▏") + "  " + hints("enter", "keep", "esc", "clear")
	}
	var h string
	if m.screen == topicsScreen {
		h = hints("↑↓", "move", "enter", "open", "/", "filter", "r", "refresh", "q", "quit")
	} else {
		changed := "changed only"
		if m.onlyChanged {
			changed = "all fields"
		}
		h = hints("↑↓", "move", "space", "pause", "c", changed, "ctrl+d/u", "scroll detail", "/", "filter", "esc", "back")
	}
	if m.filter != "" {
		h = sPink.Render("/") + sText.Render(m.filter) + "  " + h
	}
	if m.streamErr != nil && m.screen == topicsScreen {
		h = sErr.Render(m.streamErr.Error()) + "  " + h
	}
	return ansi.Truncate(" "+h, m.width, "…")
}

func hints(pairs ...string) string {
	var parts []string
	for i := 0; i+1 < len(pairs); i += 2 {
		parts = append(parts, sKey.Render(pairs[i])+" "+sDim.Render(pairs[i+1]))
	}
	return strings.Join(parts, "   ")
}

// spread puts left and right on one line of the given width.
func spread(left, right string, width int) string {
	gap := width - lipgloss.Width(left) - lipgloss.Width(right)
	if gap < 1 {
		return ansi.Truncate(left, width, "…")
	}
	return left + strings.Repeat(" ", gap) + right
}

func clamp(v, lo, hi int) int {
	if hi < lo {
		return lo
	}
	return min(max(v, lo), hi)
}

// when is the time a message is shown and sorted by: the database change
// time for CDC events, the record timestamp otherwise.
func when(msg broker.Message) time.Time {
	if msg.IsCDC && !msg.Event.Time.IsZero() {
		return msg.Event.Time
	}
	return msg.Time
}

package ui

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/Dem1241/puddle/internal/broker"
	"github.com/Dem1241/puddle/internal/cdc"
)

func (m Model) topicsView() string {
	h := m.bodyHeight()
	inner := m.width - 4
	var lines []string

	switch {
	case m.topicsErr != nil:
		lines = append(lines, "", sErr.Render("couldn't list topics"), sSub.Render(m.topicsErr.Error()))
	case len(m.topics) == 0:
		lines = append(lines, "", sDim.Render("no topics yet"))
	default:
		topics := m.visibleTopics()
		num := 12
		nameW := max(10, inner-2-2*num)
		lines = append(lines, sDim.Render("  "+pad("topic", nameW)+lpad("partitions", num)+lpad("messages", num)), "")
		rows := h - 2 - len(lines)
		start := clamp(m.topicIdx-rows/2, 0, max(0, len(topics)-rows))
		for i := start; i < len(topics) && i < start+rows; i++ {
			t := topics[i]
			name := ansi.Truncate(t.Name, nameW-1, "…")
			cols := lpad(fmt.Sprint(t.Partitions), num) + lpad(count(t.Messages), num)
			if i == m.topicIdx {
				lines = append(lines, sPink.Render("▌ ")+sTitle.Render(pad(name, nameW))+sText.Render(cols))
			} else {
				lines = append(lines, "  "+sText.Render(pad(name, nameW))+sSub.Render(cols))
			}
		}
		if len(topics) == 0 {
			lines = append(lines, sDim.Render("  nothing matches"))
		}
	}
	return sPaneOn.Width(m.width).Height(h).Render(strings.Join(fit(lines, h-2), "\n"))
}

// listHeight is how many message rows fit in the list pane.
func (m Model) listHeight() int {
	h := m.bodyHeight()
	if !m.sideBySide() {
		h = h * 45 / 100
	}
	return max(1, h-2)
}

func (m Model) sideBySide() bool { return m.width >= 100 }

func (m Model) streamView() string {
	h := m.bodyHeight()
	listW, listH, detW, detH := m.width, h*45/100, m.width, h-h*45/100
	if m.sideBySide() {
		listW = m.width * 45 / 100
		listH, detW, detH = h, m.width-listW, h
	}

	msgs := m.visibleMsgs()
	rows := listH - 2
	inner := listW - 4
	var lines []string
	if len(msgs) == 0 {
		lines = append(lines, sDim.Render("waiting for messages…"))
	}
	start := clamp(m.msgIdx-rows+1, 0, max(0, len(msgs)-rows))
	if !m.follow {
		start = clamp(m.msgIdx-rows/2, 0, max(0, len(msgs)-rows))
	}
	for i := start; i < len(msgs) && i < start+rows; i++ {
		lines = append(lines, row(msgs[i], i == m.msgIdx, inner))
	}
	list := sPaneOn.Width(listW).Height(listH).Render(strings.Join(fit(lines, rows), "\n"))

	var detail []string
	if m.msgIdx >= 0 && m.msgIdx < len(msgs) {
		detail = detailLines(msgs[m.msgIdx], detW-4, m.onlyChanged)
	}
	top := clamp(m.detailTop, 0, max(0, len(detail)-(detH-2)))
	det := sPane.Width(detW).Height(detH).Render(strings.Join(fit(detail[min(top, len(detail)):], detH-2), "\n"))

	if m.sideBySide() {
		return lipgloss.JoinHorizontal(lipgloss.Top, list, det)
	}
	return lipgloss.JoinVertical(lipgloss.Left, list, det)
}

// row is one line of the message list.
func row(msg broker.Message, selected bool, width int) string {
	var what string
	if msg.IsCDC {
		what = sText.Render(keySummary(msg.Key))
		if f := changedFields(msg.Event); f != "" {
			what += sDim.Render(" · ") + sSub.Render(f)
		}
	} else if len(msg.Value) == 0 {
		what = sText.Render(keySummary(msg.Key)) + sDim.Render(" · tombstone")
	} else {
		what = sSub.Render(oneLine(msg.Value))
	}
	marker := "  "
	if selected {
		marker = sPink.Render("▌ ")
	}
	line := marker + sDim.Render(when(msg).Format("15:04:05")) + " " + badge(msg.Event.Op, msg.IsCDC) + " " + what
	return ansi.Truncate(line, width, "…")
}

// detailLines renders one message for the detail pane.
func detailLines(msg broker.Message, width int, onlyChanged bool) []string {
	title := msg.Topic
	if msg.IsCDC && msg.Event.Source != "" {
		title = msg.Event.Source
	}
	lines := []string{
		badge(msg.Event.Op, msg.IsCDC) + " " + sTitle.Render(title),
		sDim.Render(fmt.Sprintf("partition %d · offset %d · %s", msg.Partition, msg.Offset, when(msg).Format("2006-01-02 15:04:05.000"))),
	}
	if len(msg.Key) > 0 {
		lines = append(lines, sDim.Render("key ")+sSub.Render(keySummary(msg.Key)))
	}
	lines = append(lines, "")

	if msg.IsCDC {
		lines = append(lines, changeLines(msg.Event, width, onlyChanged)...)
	} else {
		lines = append(lines, valueLines(msg.Value, width)...)
	}
	for i, l := range lines {
		lines[i] = ansi.Truncate(l, width, "…")
	}
	return lines
}

func changeLines(e cdc.Event, width int, onlyChanged bool) []string {
	changes := e.Changes()
	if len(changes) == 0 {
		return []string{sDim.Render("no row data (" + e.Op.String() + ")")}
	}
	nameW := 0
	for _, c := range changes {
		nameW = max(nameW, utf8.RuneCountInString(c.Name))
	}
	nameW = min(nameW, 22)
	valW := max(8, width-nameW-2)

	var lines []string
	hidden := 0
	for _, c := range changes {
		name := pad(ansi.Truncate(c.Name, nameW, "…"), nameW) + "  "
		switch c.Kind {
		case cdc.Same:
			if onlyChanged {
				hidden++
				continue
			}
			lines = append(lines, sDim.Render(name+cut(cdc.Show(c.After), valW)))
		case cdc.Changed:
			before, after := cdc.Show(c.Before), cdc.Show(c.After)
			half := max(4, (valW-3)/2)
			if utf8.RuneCountInString(before)+utf8.RuneCountInString(after)+3 > valW {
				before, after = cut(before, half), cut(after, valW-3-min(half, utf8.RuneCountInString(before)))
			}
			lines = append(lines, sText.Render(name)+sBefore.Render(before)+sDim.Render(" → ")+sAfter.Render(after))
		case cdc.Added:
			style := sText
			if e.Op == cdc.OpUpdate {
				style = sAfter
			}
			lines = append(lines, sText.Render(name)+style.Render(cut(cdc.Show(c.After), valW)))
		case cdc.Removed:
			style := sSub
			if e.Op == cdc.OpUpdate {
				style = sBefore
			}
			lines = append(lines, sSub.Render(name)+style.Render(cut(cdc.Show(c.Before), valW)))
		}
	}
	if hidden > 0 {
		lines = append(lines, "", sDim.Render(fmt.Sprintf("%d unchanged field%s hidden · c shows them", hidden, plural(hidden))))
	}
	if e.Op == cdc.OpUpdate && hidden == 0 && onlyChanged {
		lines = append(lines, "", sDim.Render("every field changed"))
	}
	return lines
}

// valueLines shows a non-CDC value: highlighted JSON, plain text, or a note
// for binary data.
func valueLines(v []byte, width int) []string {
	if len(v) == 0 {
		return []string{sDim.Render("empty value (tombstone)")}
	}
	var buf bytes.Buffer
	if json.Indent(&buf, v, "", "  ") == nil {
		var lines []string
		for _, l := range strings.Split(buf.String(), "\n") {
			lines = append(lines, highlightJSON(l))
		}
		return lines
	}
	if !utf8.Valid(v) {
		return []string{sDim.Render(fmt.Sprintf("binary · %d bytes", len(v))), sSub.Render(fmt.Sprintf("% x", v[:min(len(v), width/3)]))}
	}
	var lines []string
	for _, l := range strings.Split(string(v), "\n") {
		for utf8.RuneCountInString(l) > width && width > 0 {
			r := []rune(l)
			lines = append(lines, sText.Render(string(r[:width])))
			l = string(r[width:])
		}
		lines = append(lines, sText.Render(l))
	}
	return lines
}

// highlightJSON colours one line of indented JSON.
func highlightJSON(line string) string {
	var b strings.Builder
	i := 0
	for i < len(line) {
		ch := line[i]
		switch {
		case ch == '"':
			j := i + 1
			for j < len(line) && line[j] != '"' {
				if line[j] == '\\' {
					j++
				}
				j++
			}
			j = min(j+1, len(line))
			tok := line[i:j]
			if strings.HasPrefix(strings.TrimSpace(line[j:]), ":") {
				b.WriteString(sJSONKey.Render(tok))
			} else {
				b.WriteString(sJSONString.Render(tok))
			}
			i = j
		case ch == '-' || (ch >= '0' && ch <= '9'):
			j := i + 1
			for j < len(line) && strings.IndexByte("0123456789.eE+-", line[j]) >= 0 {
				j++
			}
			b.WriteString(sJSONNumber.Render(line[i:j]))
			i = j
		case strings.HasPrefix(line[i:], "true"), strings.HasPrefix(line[i:], "null"):
			b.WriteString(sJSONLit.Render(line[i : i+4]))
			i += 4
		case strings.HasPrefix(line[i:], "false"):
			b.WriteString(sJSONLit.Render(line[i : i+5]))
			i += 5
		case ch == ' ':
			b.WriteByte(' ')
			i++
		default:
			b.WriteString(sDim.Render(string(ch)))
			i++
		}
	}
	return b.String()
}

// keySummary turns {"id":1004} into id=1004, and shows other keys as text.
func keySummary(key []byte) string {
	if len(key) == 0 {
		return ""
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(key, &obj); err == nil && len(obj) > 0 {
		// Debezium keys may carry a schema too; the fields are in payload.
		if p, ok := obj["payload"]; ok {
			if _, ok := obj["schema"]; ok {
				return keySummary(p)
			}
		}
		var parts []string
		for k, v := range obj {
			parts = append(parts, k+"="+cdc.Show(v))
		}
		slices.Sort(parts)
		return strings.Join(parts, " ")
	}
	return oneLine(key)
}

func oneLine(b []byte) string {
	if !utf8.Valid(b) {
		return fmt.Sprintf("binary · %d bytes", len(b))
	}
	var buf bytes.Buffer
	if json.Compact(&buf, b) == nil {
		return buf.String()
	}
	return strings.Join(strings.Fields(string(b)), " ")
}

func count(n int64) string {
	switch {
	case n >= 10_000_000:
		return fmt.Sprintf("%.0fM", float64(n)/1e6)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1e6)
	case n >= 100_000:
		return fmt.Sprintf("%.0fk", float64(n)/1e3)
	}
	return fmt.Sprint(n)
}

func pad(s string, w int) string {
	return s + strings.Repeat(" ", max(0, w-lipgloss.Width(s)))
}

func lpad(s string, w int) string {
	return strings.Repeat(" ", max(0, w-lipgloss.Width(s))) + s
}

func cut(s string, w int) string { return ansi.Truncate(s, w, "…") }

// fit makes lines exactly n long, so panes keep their size.
func fit(lines []string, n int) []string {
	if len(lines) > n {
		return lines[:n]
	}
	for len(lines) < n {
		lines = append(lines, "")
	}
	return lines
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// changedFields names what an update changed, like "plan, updated_at".
func changedFields(e cdc.Event) string {
	if e.Op != cdc.OpUpdate {
		return ""
	}
	var names []string
	for _, c := range e.Changes() {
		if c.Kind != cdc.Same {
			names = append(names, c.Name)
		}
	}
	return strings.Join(names, ", ")
}

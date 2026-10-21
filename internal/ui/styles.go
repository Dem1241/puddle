package ui

import (
	"charm.land/lipgloss/v2"

	"github.com/Dem1241/puddle/internal/cdc"
)

// Catppuccin Mocha. The terminal's own background shows through; only the
// badges get a background of their own.
var (
	pink     = lipgloss.Color("#f5c2e7")
	mauve    = lipgloss.Color("#cba6f7")
	text     = lipgloss.Color("#cdd6f4")
	subtext  = lipgloss.Color("#a6adc8")
	overlay  = lipgloss.Color("#6c7086")
	surface1 = lipgloss.Color("#45475a")
	base     = lipgloss.Color("#1e1e2e")
	green    = lipgloss.Color("#a6e3a1")
	yellow   = lipgloss.Color("#f9e2af")
	red      = lipgloss.Color("#f38ba8")
	blue     = lipgloss.Color("#89b4fa")
	peach    = lipgloss.Color("#fab387")
	teal     = lipgloss.Color("#94e2d5")
)

var (
	sText   = lipgloss.NewStyle().Foreground(text)
	sSub    = lipgloss.NewStyle().Foreground(subtext)
	sDim    = lipgloss.NewStyle().Foreground(overlay)
	sPink   = lipgloss.NewStyle().Foreground(pink)
	sTitle  = lipgloss.NewStyle().Foreground(pink).Bold(true)
	sErr    = lipgloss.NewStyle().Foreground(red)
	sKey    = lipgloss.NewStyle().Foreground(pink)
	sPane   = lipgloss.NewStyle().Border(lipgloss.RoundedBorder()).BorderForeground(surface1).Padding(0, 1)
	sPaneOn = sPane.BorderForeground(pink)
	sBefore = lipgloss.NewStyle().Foreground(red)
	sAfter  = lipgloss.NewStyle().Foreground(green)

	// JSON highlighting in the detail pane.
	sJSONKey    = lipgloss.NewStyle().Foreground(blue)
	sJSONString = lipgloss.NewStyle().Foreground(green)
	sJSONNumber = lipgloss.NewStyle().Foreground(peach)
	sJSONLit    = lipgloss.NewStyle().Foreground(mauve)
)

// badge is the coloured pill in front of every message.
func badge(op cdc.Op, isCDC bool) string {
	label, bg := "msg", surface1
	fg := text
	if isCDC {
		label = op.String()
		fg = base
		switch op {
		case cdc.OpCreate:
			bg = green
		case cdc.OpUpdate:
			bg = yellow
		case cdc.OpDelete:
			bg = red
		case cdc.OpRead:
			bg = blue
		default:
			bg = teal
		}
	}
	return lipgloss.NewStyle().Foreground(fg).Background(bg).Width(8).Align(lipgloss.Center).Render(label)
}

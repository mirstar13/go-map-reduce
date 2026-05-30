package main

import "github.com/charmbracelet/lipgloss"

var (
	purple = lipgloss.Color("#bb9af7")
	blue   = lipgloss.Color("#7aa2f7")
	green  = lipgloss.Color("#9ece6a")
	bg     = lipgloss.Color("#1a1b26")
	fg     = lipgloss.Color("#a9b1d6")

	activeBorderColor   = purple
	inactiveBorderColor = lipgloss.Color("#414868")

	baseStyle = lipgloss.NewStyle().
			Padding(1, 2).
			Foreground(fg)

	panelStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(inactiveBorderColor).
			Padding(1).
			MarginRight(1)

	focusedPanelStyle = panelStyle.Copy().
				BorderForeground(activeBorderColor).
				BorderTopBackground(activeBorderColor)
)

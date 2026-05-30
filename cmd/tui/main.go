package main

import (
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

func (m model) Init() tea.Cmd {
	return nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.quitting = true
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m model) View() string {
	if m.quitting {
		return "Bye!\n"
	}

	header := " MAPREDUCE MISSION CONTROL "

	// Top Row
	health := panelStyle.Render("Health: OK")
	progress := panelStyle.Render("Progress: 0%")
	topRow := lipgloss.JoinHorizontal(lipgloss.Top, health, progress)

	// Bottom Row
	jobs := panelStyle.Render("Job List...")
	if m.focused == jobsPanel {
		jobs = focusedPanelStyle.Render("Job List...")
	}
	logs := panelStyle.Render("Logs...")
	bottomRow := lipgloss.JoinHorizontal(lipgloss.Top, jobs, logs)

	return lipgloss.JoinVertical(lipgloss.Left, header, topRow, bottomRow)
}

func main() {
	p := tea.NewProgram(initialModel())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v", err)
		os.Exit(1)
	}
}

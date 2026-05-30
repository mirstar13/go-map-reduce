package main

import (
	"fmt"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
	"github.com/mirstar13/go-map-reduce/db"
)

type tickMsg time.Time

func doTick() tea.Cmd {
	return tea.Tick(time.Second*2, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

type jobsMsg []db.Job

func fetchJobs(c *client.Client) tea.Cmd {
	return func() tea.Msg {
		if c == nil {
			return jobsMsg{}
		}
		var jobs []db.Job
		// We'll use a placeholder for now to avoid blocking on networking
		// _ = c.Get("/jobs", &jobs)
		return jobsMsg(jobs)
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		doTick(),
		fetchJobs(m.client),
	)
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tickMsg:
		return m, tea.Batch(doTick(), fetchJobs(m.client))

	case jobsMsg:
		m.jobs = msg
		return m, nil

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
	jobs := panelStyle.Render(fmt.Sprintf("Jobs (%d)...", len(m.jobs)))
	if m.focused == jobsPanel {
		jobs = focusedPanelStyle.Render(fmt.Sprintf("Jobs (%d)...", len(m.jobs)))
	}
	logs := panelStyle.Render("Logs...")
	bottomRow := lipgloss.JoinHorizontal(lipgloss.Top, jobs, logs)

	return lipgloss.JoinVertical(lipgloss.Left, header, topRow, bottomRow)
}

func main() {
	// Initialize a client (using default values for now)
	c := client.New("http://localhost:8080/api/v1", "")

	p := tea.NewProgram(initialModel(c))
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v", err)
		os.Exit(1)
	}
}

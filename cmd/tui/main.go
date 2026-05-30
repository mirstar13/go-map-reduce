package main

import (
	"fmt"
	"os"
	"time"

	"github.com/charmbracelet/bubbles/list"
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
type errorMsg error

func fetchJobs(c *client.Client) tea.Cmd {
	return func() tea.Msg {
		if c == nil {
			return jobsMsg{}
		}
		var jobs []db.Job
		err := c.Get("/jobs", &jobs)
		if err != nil {
			return errorMsg(err)
		}
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
	var cmd tea.Cmd
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.jobList.SetSize(msg.Width/2-4, msg.Height/2-4)
		m.progress.Width = msg.Width/2 - 8

	case tickMsg:
		return m, tea.Batch(doTick(), fetchJobs(m.client))

	case jobsMsg:
		m.jobs = msg
		items := make([]list.Item, len(msg))
		for i, j := range msg {
			items[i] = item{job: j}
		}
		m.jobList.SetItems(items)
		m.err = nil
		return m, nil

	case errorMsg:
		m.err = msg
		return m, nil

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.quitting = true
			return m, tea.Quit
		case "tab":
			m.focused = (m.focused + 1) % 3
			return m, nil
		}
	}

	// Only pass key messages to the list if it's focused
	if m.focused == jobsPanel {
		m.jobList, cmd = m.jobList.Update(msg)
		cmds = append(cmds, cmd)
	} else {
		// Non-key messages should still be passed (like WindowSizeMsg)
		if _, ok := msg.(tea.KeyMsg); !ok {
			m.jobList, cmd = m.jobList.Update(msg)
			cmds = append(cmds, cmd)
		}
	}

	return m, tea.Batch(cmds...)
}

func (m model) View() string {
	if m.quitting {
		return "Bye!\n"
	}

	header := lipgloss.NewStyle().
		Bold(true).
		Foreground(lipgloss.Color("#FFFFFF")).
		Background(purple).
		Padding(0, 1).
		Render(" MAPREDUCE MISSION CONTROL ")

	// Top Row
	healthText := "Health: OK"
	healthStyle := panelStyle
	if m.err != nil {
		healthText = fmt.Sprintf("Health: ERROR (%v)", m.err)
		healthStyle = panelStyle.Copy().BorderForeground(lipgloss.Color("#f7768e"))
	}
	if m.focused == healthPanel {
		healthStyle = focusedPanelStyle
	}
	health := healthStyle.Render(healthText)

	// Progress
	progVal := 0.0
	statusText := "No active job selected"
	if len(m.jobs) > 0 {
		idx := m.jobList.Index()
		if idx >= 0 && idx < len(m.jobs) {
			job := m.jobs[idx]
			statusText = fmt.Sprintf("Job %s: %s", job.JobID.String()[:8], job.Status)
			if job.Status == "completed" {
				progVal = 1.0
			} else if job.Status == "running" {
				progVal = 0.45 // Mock progress
			} else if job.Status == "failed" {
				progVal = 0.0
			}
		}
	}
	progress := panelStyle.Render(
		lipgloss.JoinVertical(lipgloss.Left,
			"Active Job Progress",
			statusText,
			m.progress.ViewAs(progVal),
		),
	)
	topRow := lipgloss.JoinHorizontal(lipgloss.Top, health, progress)

	// Bottom Row
	jobList := m.jobList.View()
	jobsStyle := panelStyle
	if m.focused == jobsPanel {
		jobsStyle = focusedPanelStyle
	}
	jobs := jobsStyle.Render(jobList)

	logsText := "Logs..."
	logsStyle := panelStyle
	if m.focused == logsPanel {
		logsStyle = focusedPanelStyle
	}
	logs := logsStyle.Render(logsText)
	bottomRow := lipgloss.JoinHorizontal(lipgloss.Top, jobs, logs)

	return lipgloss.JoinVertical(lipgloss.Left, header, topRow, bottomRow)
}

func main() {
	url := os.Getenv("UI_URL")
	if url == "" {
		url = "http://localhost:8081"
	}
	// Initialize a client
	c := client.New(url+"/api/v1", "")

	p := tea.NewProgram(initialModel(c), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v", err)
		os.Exit(1)
	}
}

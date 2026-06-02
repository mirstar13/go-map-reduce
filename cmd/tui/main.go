package main

import (
	"fmt"
	"os"
	"time"

	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
	"github.com/mirstar13/go-map-reduce/cmd/cli/config"
	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/services/ui/metrics"
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

func (m *model) Init() tea.Cmd {
	_, err := config.Load()
	if err == nil {
		m.isLoggedIn = true
	}
	if !m.isLoggedIn {
		return m.authModel.Init()
	}
	return tea.Batch(
		doTick(),
		fetchJobs(m.client),
		fetchMetrics(m.MetricsClient),
	)
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if !m.isLoggedIn {
		authModel, authCmd := m.authModel.Update(msg)
		m.authModel = authModel.(LoginModel)

		if msg, ok := msg.(tea.KeyMsg); ok && msg.String() == "enter" {
			// Basic login validation
			m.isLoggedIn = true
			return m, tea.Batch(doTick(), fetchJobs(m.client), fetchMetrics(m.MetricsClient))
		}

		return m, authCmd
	}

	var cmds []tea.Cmd

	// Update navigation model
	navModel, navCmd := m.nav.Update(msg)
	m.nav = navModel
	cmds = append(cmds, navCmd)

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.jobList.SetSize(msg.Width/2-4, msg.Height/2-4)
		m.progress.Width = msg.Width/2 - 8
		m.nav.list.SetSize(20, msg.Height-2)

	case tickMsg:
		return m, tea.Batch(append(cmds, doTick(), fetchJobs(m.client), fetchMetrics(m.MetricsClient))...)

	case metricsMsg:
		m.MetricsData = metrics.ClusterMetrics(msg)
		return m, tea.Batch(cmds...)

	case jobsMsg:
		m.jobs = msg
		items := make([]list.Item, len(msg))
		for i, j := range msg {
			items[i] = item{job: j}
		}
		m.jobList.SetItems(items)
		m.err = nil
		return m, tea.Batch(cmds...)

	case errorMsg:
		m.err = msg
		return m, tea.Batch(cmds...)

	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			m.quitting = true
			return m, tea.Quit
		case "tab":
			if m.activeView == createUserFormView {
				m.activeView = roleAssignmentFormView
			} else if m.activeView == roleAssignmentFormView {
				m.activeView = createUserFormView
			} else {
				m.focused = (m.focused + 1) % 3
			}
			return m, nil
		case "enter":
			if selected, ok := m.nav.list.SelectedItem().(navItem); ok {
				switch selected.title {
				case "Settings":
					m.activeView = jobFormView
				case "Admin":
					m.activeView = createUserFormView
				default:
					m.activeView = dashboardView
				}
			}
		}
	}

	// Only pass key messages to the list if it's focused
	if m.focused == taskInspectorPanel {
		var cmd tea.Cmd
		m.jobList, cmd = m.jobList.Update(msg)
		cmds = append(cmds, cmd)
	} else {
		// Non-key messages should still be passed (like WindowSizeMsg)
		if _, ok := msg.(tea.KeyMsg); !ok {
			var cmd tea.Cmd
			m.jobList, cmd = m.jobList.Update(msg)
			cmds = append(cmds, cmd)
		}
	}

	return m, tea.Batch(cmds...)
}

func (m *model) View() string {
	if m.quitting {
		return "Bye!\n"
	}

	if !m.isLoggedIn {
		return m.authModel.View()
	}

	nav := m.nav.View()

	var content string
	switch m.activeView {
	case jobFormView:
		content = m.form.View()
	case createUserFormView:
		content = m.userForm.View()
	case roleAssignmentFormView:
		content = m.roleForm.View()
	default:
		header := lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FFFFFF")).
			Background(purple).
			Padding(0, 1).
			Render(" MAPREDUCE MISSION CONTROL ")

		// 1. Global Status Panel
		statusText := fmt.Sprintf("Nodes: %d\nTasks: %d", m.MetricsData.ActiveNodes, m.MetricsData.ActiveTasks)
		statusStyle := globalStatusStyle
		if m.focused == globalStatusPanel {
			statusStyle = focusedPanelStyle
		}
		globalStatus := statusStyle.Render(lipgloss.JoinVertical(lipgloss.Left, "Global Status", statusText))

		// 2. Shuffle/Resource Panel
		resourceText := fmt.Sprintf("Shuffle: %v\nMemory: %v", m.MetricsData.ShuffleProgress, m.MetricsData.MemoryUsage)
		resourceStyle := shuffleResourceStyle
		if m.focused == shuffleResourcePanel {
			resourceStyle = focusedPanelStyle
		}
		shuffleResource := resourceStyle.Render(lipgloss.JoinVertical(lipgloss.Left, "Shuffle/Resource", resourceText))

		// 3. Task Inspector Panel
		jobList := m.jobList.View()
		taskStyle := taskInspectorStyle
		if m.focused == taskInspectorPanel {
			taskStyle = focusedPanelStyle
		}
		taskInspector := taskStyle.Render(lipgloss.JoinVertical(lipgloss.Left, "Task Inspector", jobList))

		content = lipgloss.JoinVertical(lipgloss.Left, header,
			lipgloss.JoinHorizontal(lipgloss.Top, globalStatus, shuffleResource),
			taskInspector,
		)
	}

	return lipgloss.JoinHorizontal(lipgloss.Top, nav, content)
}

func newClient(cfg *config.Config) *client.Client {
	return client.New(cfg.ServerURL, cfg.Token)
}

func main() {
	// Initialize a client
	cfg, _ := config.Load()
	c := newClient(cfg)
	mc := metrics.NewClient(cfg.ServerURL)

	p := tea.NewProgram(initialModel(c, mc), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		fmt.Printf("Error: %v", err)
		os.Exit(1)
	}
}

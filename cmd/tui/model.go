package main

import (
	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
	"github.com/mirstar13/go-map-reduce/db"
	"github.com/mirstar13/go-map-reduce/services/ui/metrics"
)

type panel int

const (
	globalStatusPanel panel = iota
	shuffleResourcePanel
	taskInspectorPanel
)

type activeView int

const (
	dashboardView activeView = iota
	jobFormView
	createUserFormView
	roleAssignmentFormView
)

type item struct {
	job db.Job
}

func (i item) Title() string       { return i.job.JobID.String() }
func (i item) Description() string { return i.job.Status }
func (i item) FilterValue() string { return i.job.JobID.String() }

type metricsMsg metrics.ClusterMetrics

type model struct {
	client        *client.Client
	MetricsClient metrics.Client
	MetricsData   metrics.ClusterMetrics
	focused       panel
	activeView    activeView
	quitting      bool
	jobs          []db.Job
	err           error

	authModel  LoginModel
	isLoggedIn bool

	jobList  list.Model
	progress progress.Model

	nav      navModel
	form     jobFormModel
	userForm createUserForm
	roleForm roleAssignmentForm
}

func initialModel(c *client.Client, mc metrics.Client) *model {
	l := list.New([]list.Item{}, list.NewDefaultDelegate(), 0, 0)
	l.Title = "Jobs"
	l.SetShowStatusBar(false)
	l.SetFilteringEnabled(false)

	p := progress.New(progress.WithDefaultGradient())

	return &model{
		client:        c,
		MetricsClient: mc,
		jobs:          []db.Job{},
		focused:       globalStatusPanel,
		jobList:       l,
		progress:      p,
		authModel:     NewLoginModel(),
		isLoggedIn:    false,
		nav:           NewNavModel(),
		form:          NewJobFormModel(),
		userForm:      NewCreateUserForm(),
		roleForm:      NewRoleAssignmentForm(),
	}
}

func fetchMetrics(c metrics.Client) tea.Cmd {
	return func() tea.Msg {
		m, err := c.GetClusterMetrics(nil)
		if err != nil {
			return nil
		}
		return metricsMsg(m)
	}
}

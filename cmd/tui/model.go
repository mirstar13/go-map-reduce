package main

import (
	"github.com/charmbracelet/bubbles/list"
	"github.com/charmbracelet/bubbles/progress"
	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
	"github.com/mirstar13/go-map-reduce/db"
)

type panel int

const (
	jobsPanel panel = iota
	healthPanel
	logsPanel
)

type item struct {
	job db.Job
}

func (i item) Title() string       { return i.job.JobID.String() }
func (i item) Description() string { return i.job.Status }
func (i item) FilterValue() string { return i.job.JobID.String() }

type model struct {
	client   *client.Client
	focused  panel
	quitting bool
	jobs     []db.Job
	err      error

	jobList  list.Model
	progress progress.Model
}

func initialModel(c *client.Client) model {
	l := list.New([]list.Item{}, list.NewDefaultDelegate(), 0, 0)
	l.Title = "Jobs"
	l.SetShowStatusBar(false)
	l.SetFilteringEnabled(false)

	p := progress.New(progress.WithDefaultGradient())

	return model{
		client:   c,
		jobs:     []db.Job{},
		focused:  jobsPanel,
		jobList:  l,
		progress: p,
	}
}

package main

import (
	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
	"github.com/mirstar13/go-map-reduce/db"
)

type panel int

const (
	jobsPanel panel = iota
	healthPanel
	logsPanel
)

type model struct {
	client   *client.Client
	focused  panel
	quitting bool
	jobs     []db.Job
}

func initialModel(c *client.Client) model {
	return model{
		client:  c,
		jobs:    []db.Job{},
		focused: jobsPanel,
	}
}

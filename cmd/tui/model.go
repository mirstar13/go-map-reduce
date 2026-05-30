package main

import (
	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
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
	jobs     []interface{}
}

func initialModel() model {
	return model{
		jobs:    []interface{}{},
		focused: jobsPanel,
	}
}

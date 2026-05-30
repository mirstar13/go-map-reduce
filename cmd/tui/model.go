package main

import (
	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
)

type model struct {
	client   *client.Client
	jobs     []interface{} // Placeholder for now
	quitting bool
}

func initialModel() model {
	return model{
		jobs: []interface{}{},
	}
}

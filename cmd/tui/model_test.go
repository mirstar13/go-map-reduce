package main

import (
	"testing"
	"github.com/mirstar13/go-map-reduce/cmd/cli/client"
	"github.com/stretchr/testify/assert"
)

func TestInitialModel(t *testing.T) {
	c := client.New("http://localhost:8080", "test-token")
	m := initialModel(c)
	assert.Equal(t, 0, len(m.jobs))
	assert.False(t, m.quitting)
	assert.NotNil(t, m.client)
	assert.Equal(t, jobsPanel, m.focused)
}

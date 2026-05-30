package main

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestInitialModel(t *testing.T) {
	m := initialModel()
	assert.Equal(t, 0, len(m.jobs))
	assert.False(t, m.quitting)
}

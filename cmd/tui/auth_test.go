package main

import (
	"testing"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/stretchr/testify/assert"
)

func TestLoginModel_Update(t *testing.T) {
	m := NewLoginModel()
	assert.Equal(t, 0, m.focused)

	// Test focus switching
	_, _ = m.Update(tea.KeyMsg{Type: tea.KeyTab})
	assert.Equal(t, 1, m.focused)
    
	_, _ = m.Update(tea.KeyMsg{Type: tea.KeyTab})
	assert.Equal(t, 2, m.focused)

	_, _ = m.Update(tea.KeyMsg{Type: tea.KeyTab})
	assert.Equal(t, 0, m.focused)
}

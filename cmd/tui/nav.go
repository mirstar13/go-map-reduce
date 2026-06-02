package main

import (
	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
)

type navItem struct {
	title string
	desc  string
}

func (i navItem) Title() string       { return i.title }
func (i navItem) Description() string { return i.desc }
func (i navItem) FilterValue() string { return i.title }

type navModel struct {
	list list.Model
}

func NewNavModel() navModel {
	items := []list.Item{
		navItem{title: "Jobs", desc: "Manage jobs"},
		navItem{title: "Metrics", desc: "View cluster metrics"},
		navItem{title: "Settings", desc: "Configure TUI"},
		navItem{title: "Admin", desc: "User/Role management"},
	}
	l := list.New(items, list.NewDefaultDelegate(), 0, 0)
	l.Title = "Navigation"
	l.SetShowStatusBar(false)
	return navModel{list: l}
}

func (m navModel) Init() tea.Cmd { return nil }

func (m navModel) Update(msg tea.Msg) (navModel, tea.Cmd) {
	var cmd tea.Cmd
	m.list, cmd = m.list.Update(msg)
	return m, cmd
}

func (m navModel) View() string {
	return m.list.View()
}

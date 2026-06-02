package main

import (
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type LoginModel struct {
	urlInput      textinput.Model
	usernameInput textinput.Model
	passwordInput textinput.Model
	focused       int
}

func NewLoginModel() LoginModel {
	urlInput := textinput.New()
	urlInput.Placeholder = "Server URL"
	urlInput.Focus()

	usernameInput := textinput.New()
	usernameInput.Placeholder = "Username"

	passwordInput := textinput.New()
	passwordInput.Placeholder = "Password"
	passwordInput.EchoMode = textinput.EchoPassword

	return LoginModel{
		urlInput:      urlInput,
		usernameInput: usernameInput,
		passwordInput: passwordInput,
		focused:       0,
	}
}

func (m LoginModel) Init() tea.Cmd {
	return textinput.Blink
}

func (m LoginModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "tab", "down":
			m.focused = (m.focused + 1) % 3
		case "up":
			m.focused = (m.focused - 1 + 3) % 3
		}
	}
	
	switch m.focused {
	case 0:
		m.urlInput.Focus()
		m.usernameInput.Blur()
		m.passwordInput.Blur()
		m.urlInput, cmd = m.urlInput.Update(msg)
	case 1:
		m.urlInput.Blur()
		m.usernameInput.Focus()
		m.passwordInput.Blur()
		m.usernameInput, cmd = m.usernameInput.Update(msg)
	case 2:
		m.urlInput.Blur()
		m.usernameInput.Blur()
		m.passwordInput.Focus()
		m.passwordInput, cmd = m.passwordInput.Update(msg)
	}
	return m, cmd
}

func (m LoginModel) View() string {
	return lipgloss.JoinVertical(lipgloss.Left,
		"Login",
		m.urlInput.View(),
		m.usernameInput.View(),
		m.passwordInput.View(),
	)
}

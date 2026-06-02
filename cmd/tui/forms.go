package main

import (
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

type jobFormModel struct {
	textInput textinput.Model
}

func NewJobFormModel() jobFormModel {
	ti := textinput.New()
	ti.Placeholder = "Job Name"
	ti.Focus()
	ti.CharLimit = 156
	ti.Width = 30
	return jobFormModel{textInput: ti}
}

func (m jobFormModel) Init() tea.Cmd { return textinput.Blink }

func (m jobFormModel) Update(msg tea.Msg) (jobFormModel, tea.Cmd) {
	var cmd tea.Cmd
	m.textInput, cmd = m.textInput.Update(msg)
	return m, cmd
}

func (m jobFormModel) View() string {
	return m.textInput.View()
}

type createUserForm struct {
	usernameInput textinput.Model
	passwordInput textinput.Model
}

func NewCreateUserForm() createUserForm {
	u := textinput.New()
	u.Placeholder = "Username"
	u.Focus()
	p := textinput.New()
	p.Placeholder = "Password"
	return createUserForm{usernameInput: u, passwordInput: p}
}

func (m createUserForm) View() string {
	return m.usernameInput.View() + "\n" + m.passwordInput.View()
}

type roleAssignmentForm struct {
	usernameInput textinput.Model
	roleInput     textinput.Model
}

func NewRoleAssignmentForm() roleAssignmentForm {
	u := textinput.New()
	u.Placeholder = "Username"
	u.Focus()
	r := textinput.New()
	r.Placeholder = "Role"
	return roleAssignmentForm{usernameInput: u, roleInput: r}
}

func (m roleAssignmentForm) View() string {
	return m.usernameInput.View() + "\n" + m.roleInput.View()
}

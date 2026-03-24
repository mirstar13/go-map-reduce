//go:build linux || darwin

package plugin

import "os/exec"

// pluginCmd creates an exec.Cmd for running a plugin binary.
func pluginCmd(path string) *exec.Cmd {
	return exec.Command(path)
}

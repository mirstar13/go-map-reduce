//go:build windows

package plugin

import "os/exec"

// pluginCmd creates an exec.Cmd for running a plugin binary on Windows.
func pluginCmd(path string) *exec.Cmd {
	return exec.Command(path + ".exe")
}

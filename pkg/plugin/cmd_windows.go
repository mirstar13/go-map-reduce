//go:build windows

package plugin

import (
	"os/exec"
	"strings"
)

// pluginCmd creates an exec.Cmd for running a plugin binary on Windows.
func pluginCmd(path string) *exec.Cmd {
	if !strings.HasSuffix(path, ".exe") {
		path += ".exe"
	}
	return exec.Command(path)
}


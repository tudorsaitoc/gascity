//go:build !linux

package main

import "os/exec"

// Cgroup placement is Linux-only. Other platforms retain their existing
// process-group and managed-watchdog behavior.
func startManagedDoltCommand(cmd *exec.Cmd, _ string) error {
	return cmd.Start()
}

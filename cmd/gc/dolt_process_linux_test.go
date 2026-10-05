//go:build linux

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestManagedDoltCgroupDeclarationRejectsInvalidBoundary(t *testing.T) {
	for _, data := range []string{
		`null`,
		`{"city_path":"relative","cgroup_path":"/sys/fs/cgroup/beads.slice/beads-canonical.slice"}`,
		`{"city_path":"/city/../other","cgroup_path":"/sys/fs/cgroup/beads.slice/beads-canonical.slice"}`,
		`{"city_path":"/city","cgroup_path":"/sys/fs/cgroup/saitoc.slice/saitoc-workers.slice"}`,
		`{"city_path":"/city","cgroup_path":"/sys/fs/cgroup/beads.slice/beads-canonical.slice/../other"}`,
		`{"city_path":"/city","cgroup_path":"/sys/fs/cgroup/beads.slice/beads-canonical.slice","unknown":true}`,
		`{"city_path":"/city","cgroup_path":"/sys/fs/cgroup/beads.slice/beads-canonical.slice"} {}`,
	} {
		t.Run(data, func(t *testing.T) {
			if _, err := parseManagedDoltCgroupConfig([]byte(data)); err == nil {
				t.Fatal("invalid declaration accepted")
			}
		})
	}
}

func TestManagedDoltCgroupMatchesOnlyCanonicalCity(t *testing.T) {
	city := t.TempDir()
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(city, alias); err != nil {
		t.Fatal(err)
	}
	config := managedDoltCgroupConfig{CityPath: alias, CgroupPath: managedDoltCanonicalCgroup}
	cgroup, err := managedDoltCgroupForCity(city, config)
	if err != nil || cgroup != managedDoltCanonicalCgroup {
		t.Fatalf("canonical symlink city placement = %q, %v", cgroup, err)
	}
	cgroup, err = managedDoltCgroupForCity(t.TempDir(), config)
	if err != nil || cgroup != "" {
		t.Fatalf("unrelated city placement = %q, %v", cgroup, err)
	}
}

func TestManagedDoltMissingExplicitCgroupDeclarationPreventsSpawn(t *testing.T) {
	withManagedDoltTestMode(t, false)
	t.Setenv(managedDoltTestModeEnv, "")
	t.Setenv(managedDoltCgroupConfigEnv, filepath.Join(t.TempDir(), "missing.json"))
	cmd := exec.Command("/bin/echo", "must not execute")
	if err := startManagedDoltCommand(cmd, t.TempDir()); err == nil || !strings.Contains(err.Error(), "cgroup declaration") {
		t.Fatalf("missing explicit declaration error = %v", err)
	}
	if cmd.Process != nil {
		t.Fatal("child spawned despite missing explicit declaration")
	}
}

func TestManagedDoltTestLaunchIgnoresHostCgroupDeclaration(t *testing.T) {
	withManagedDoltTestMode(t, true)
	t.Setenv(managedDoltCgroupConfigEnv, "invalid")
	cmd := exec.Command("/bin/echo", "test scope retained")
	var output bytes.Buffer
	cmd.Stdout = &output
	if err := startManagedDoltCommand(cmd, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "test scope retained\n" {
		t.Fatalf("test command output = %q", output.String())
	}
}

func writeManagedDoltCgroupDeclaration(t *testing.T, city, cgroup string) string {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("trusted host declaration requires root-owned fixture")
	}
	data, err := json.Marshal(managedDoltCgroupConfig{CityPath: city, CgroupPath: cgroup})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "managed-dolt-cgroup.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(managedDoltCgroupConfigEnv, path)
	return path
}

func TestManagedDoltUntrustedCgroupDeclarationPreventsSpawn(t *testing.T) {
	withManagedDoltTestMode(t, false)
	t.Setenv(managedDoltTestModeEnv, "")
	city := t.TempDir()
	path := writeManagedDoltCgroupDeclaration(t, city, managedDoltCanonicalCgroup)
	if err := os.Chmod(path, 0o666); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/echo", "must not execute")
	if err := startManagedDoltCommand(cmd, city); err == nil || !strings.Contains(err.Error(), "root-owned regular file") {
		t.Fatalf("untrusted declaration error = %v", err)
	}
	if cmd.Process != nil {
		t.Fatal("child spawned despite untrusted declaration")
	}
}

func TestManagedDoltNoncanonicalLaunchDoesNotRequireDeclaredCgroup(t *testing.T) {
	withManagedDoltTestMode(t, false)
	t.Setenv(managedDoltTestModeEnv, "")
	writeManagedDoltCgroupDeclaration(t, t.TempDir(), managedDoltCanonicalCgroup+"/absent-adr44-test")
	cmd := exec.Command("/bin/echo", "local scope retained")
	cmd.SysProcAttr = managedDoltSQLServerSysProcAttr()
	var output bytes.Buffer
	cmd.Stdout = &output
	if err := startManagedDoltCommand(cmd, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "local scope retained\n" {
		t.Fatalf("noncanonical command output = %q", output.String())
	}
}

func TestManagedDoltUnavailableDeclaredCgroupPreventsEveryProductionSpawn(t *testing.T) {
	withManagedDoltTestMode(t, false)
	t.Setenv(managedDoltTestModeEnv, "")
	city := t.TempDir()
	writeManagedDoltCgroupDeclaration(t, city, managedDoltCanonicalCgroup+"/absent-adr44-test")
	logPath := filepath.Join(city, "dolt.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close() //nolint:errcheck
	// A marker executable detects any accidental launch before placement fails.
	binDir := t.TempDir()
	marker := filepath.Join(city, "spawned")
	if err := os.WriteFile(filepath.Join(binDir, "dolt"), []byte("#!/bin/sh\ntouch '"+marker+"'\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	configPath := filepath.Join(city, "config.yaml")
	for _, watchdog := range []string{"0", "1"} {
		t.Setenv(managedDoltScopeWatchdogEnv, watchdog)
		started, err := startManagedDoltSQLServer(city, configPath, logPath, logFile)
		if err == nil || !strings.Contains(err.Error(), "canonical Dolt cgroup") {
			terminateManagedDoltStartedProcess(started)
			t.Fatalf("watchdog=%s unavailable role error = %v", watchdog, err)
		}
	}
	if code := runManagedDoltScopeWatchdog([]string{configPath, logPath, city}, logFile, logFile); code != 1 {
		t.Fatalf("watchdog child launch exit = %d, want 1", code)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("Dolt executable ran despite unavailable declared role: %v", err)
	}
}

// This canary is deliberately absent from ordinary unit runs. TestMain clears
// GC_* host routing variables, so the parent supplies the opt-in and isolated
// declaration through ADR44_* and we install only that declaration here.
// The parent owns an empty child cgroup and city; this test never creates,
// migrates, or removes a cgroup, and unsupported clone3 kernels fail the opt-in.
func TestManagedDoltActualKernelCgroupPlacement(t *testing.T) {
	optIn, present := os.LookupEnv("ADR44_MANAGED_DOLT_KERNEL_CANARY")
	if !present {
		t.Skip("opt in with ADR44_MANAGED_DOLT_KERNEL_CANARY=1 and an owned cgroup fixture")
	}
	if optIn != "1" {
		t.Fatal("ADR44_MANAGED_DOLT_KERNEL_CANARY must be exactly 1")
	}
	if os.Geteuid() != 0 {
		t.Fatal("actual-kernel canary requires root")
	}
	cgroup := os.Getenv("ADR44_MANAGED_DOLT_CANARY_CGROUP")
	if !strings.HasPrefix(cgroup, managedDoltCanonicalCgroup+"/") || filepath.Clean(cgroup) != cgroup {
		t.Fatal("canary requires a parent-owned child of the canonical cgroup, never the canonical root")
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(cgroup, &stat); err != nil {
		t.Fatalf("inspect supplied cgroup: %v", err)
	}
	if stat.Type != unix.CGROUP2_SUPER_MAGIC {
		t.Fatal("actual-kernel canary requires cgroup v2")
	}
	procs, err := os.ReadFile(filepath.Join(cgroup, "cgroup.procs"))
	if err != nil || len(bytes.TrimSpace(procs)) != 0 {
		t.Fatalf("parent-owned canary cgroup must start empty: %q, %v", procs, err)
	}
	declaration := os.Getenv("ADR44_MANAGED_DOLT_CANARY_CONFIG")
	if declaration == "" {
		t.Fatal("ADR44_MANAGED_DOLT_CANARY_CONFIG must name the parent-supplied trusted declaration")
	}
	withManagedDoltTestMode(t, false)
	t.Setenv(managedDoltTestModeEnv, "")
	t.Setenv(managedDoltCgroupConfigEnv, declaration)
	config, declared, err := readManagedDoltCgroupConfig()
	if err != nil || !declared {
		t.Fatalf("trusted canary declaration required: declared=%v, error=%v", declared, err)
	}
	if config.CgroupPath != cgroup {
		t.Fatalf("declaration cgroup %q differs from owned fixture %q", config.CgroupPath, cgroup)
	}
	// The shell reports its own identity and placement before waiting for EOF.
	// No cgroup.procs write or post-spawn migration can make this pass.
	cmd := exec.Command("/bin/sh", "-c", `printf '%s\n' "$$"; cat "/proc/$$/cgroup"; IFS= read -r _ || :`)
	cmd.SysProcAttr = managedDoltSQLServerSysProcAttr()
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close() //nolint:errcheck
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdout.Close() //nolint:errcheck
	if err := startManagedDoltCommand(cmd, config.CityPath); err != nil {
		t.Fatalf("actual clone3 launch failed (no inherited-placement fallback permitted): %v", err)
	}
	t.Cleanup(func() {
		// EOF releases this helper through its own contract; no signal is needed.
		_ = stdin.Close()
		if err := cmd.Wait(); err != nil {
			t.Errorf("wait for actual-kernel helper: %v", err)
		}
	})
	type observation struct {
		pid    string
		cgroup string
		err    error
	}
	observed := make(chan observation, 1)
	go func() {
		reader := bufio.NewReader(stdout)
		pid, err := reader.ReadString('\n')
		if err != nil {
			observed <- observation{err: err}
			return
		}
		placement, err := reader.ReadString('\n')
		observed <- observation{pid: pid, cgroup: placement, err: err}
	}()
	var child observation
	select {
	case child = <-observed:
	case <-time.After(5 * time.Second):
		t.Fatal("child did not report its first userspace placement within 5s")
	}
	if child.err != nil {
		t.Fatalf("read actual child observation: %v", child.err)
	}
	if strings.TrimSpace(child.pid) != strconv.Itoa(cmd.Process.Pid) {
		t.Fatalf("returned PID %d does not identify actual executable %q", cmd.Process.Pid, child.pid)
	}
	wantPlacement := "0::" + strings.TrimPrefix(cgroup, "/sys/fs/cgroup") + "\n"
	if child.cgroup != wantPlacement {
		t.Fatalf("child born in %q, want %q", child.cgroup, wantPlacement)
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil || pgid != cmd.Process.Pid {
		t.Fatalf("managed Setpgid not preserved: pid=%d, pgid=%d, error=%v", cmd.Process.Pid, pgid, err)
	}
	t.Logf("actual clone3 child pid=%d pgid=%d first_cgroup=%q", cmd.Process.Pid, pgid, strings.TrimSpace(child.cgroup))
}

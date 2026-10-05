//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	managedDoltCgroupConfigPath = "/etc/gascity/managed-dolt-cgroup.json"
	managedDoltCgroupConfigEnv  = "GC_DOLT_CGROUP_CONFIG"
	managedDoltCanonicalCgroup  = "/sys/fs/cgroup/beads.slice/beads-canonical.slice"
)

// This host-admin declaration applies to exactly one city, not every local
// provider. The fixed path makes CLI recovery/restarts independent of the
// supervisor's environment. The override is only for isolated host canaries;
// it has the same root-owned, non-writable file requirement as the default.
// An absent default leaves undeclared installations unchanged. Once declared,
// placement requires cgroup v2, clone3 support and permission to launch there;
// no post-spawn migration or inherited-cgroup fallback is safe.
type managedDoltCgroupConfig struct {
	CityPath   string `json:"city_path"`
	CgroupPath string `json:"cgroup_path"`
}

func parseManagedDoltCgroupConfig(data []byte) (managedDoltCgroupConfig, error) {
	var config managedDoltCgroupConfig
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return config, fmt.Errorf("decode managed Dolt cgroup declaration: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return config, fmt.Errorf("managed Dolt cgroup declaration must contain one JSON object")
	}
	if !filepath.IsAbs(config.CityPath) || filepath.Clean(config.CityPath) != config.CityPath {
		return config, fmt.Errorf("managed Dolt cgroup city_path must be a clean absolute path")
	}
	if config.CgroupPath != managedDoltCanonicalCgroup && !strings.HasPrefix(config.CgroupPath, managedDoltCanonicalCgroup+"/") {
		return config, fmt.Errorf("managed Dolt cgroup_path must be in %s", managedDoltCanonicalCgroup)
	}
	if filepath.Clean(config.CgroupPath) != config.CgroupPath {
		return config, fmt.Errorf("managed Dolt cgroup_path must be a clean absolute path")
	}
	return config, nil
}

func readManagedDoltCgroupConfig() (managedDoltCgroupConfig, bool, error) {
	path, explicit := os.LookupEnv(managedDoltCgroupConfigEnv)
	if !explicit {
		path = managedDoltCgroupConfigPath
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return managedDoltCgroupConfig{}, false, fmt.Errorf("%s must be a clean absolute path", managedDoltCgroupConfigEnv)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		if !explicit && errors.Is(err, unix.ENOENT) {
			return managedDoltCgroupConfig{}, false, nil
		}
		return managedDoltCgroupConfig{}, false, fmt.Errorf("open managed Dolt cgroup declaration %s: %w", path, err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close() //nolint:errcheck
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return managedDoltCgroupConfig{}, false, fmt.Errorf("stat managed Dolt cgroup declaration: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Uid != 0 || stat.Mode&0o022 != 0 {
		return managedDoltCgroupConfig{}, false, fmt.Errorf("managed Dolt cgroup declaration must be a root-owned regular file without group/world write permission")
	}
	const maxDeclarationBytes = 4096
	data, err := io.ReadAll(io.LimitReader(file, maxDeclarationBytes+1))
	if err != nil {
		return managedDoltCgroupConfig{}, false, fmt.Errorf("read managed Dolt cgroup declaration: %w", err)
	}
	if len(data) > maxDeclarationBytes {
		return managedDoltCgroupConfig{}, false, fmt.Errorf("managed Dolt cgroup declaration exceeds %d bytes", maxDeclarationBytes)
	}
	config, err := parseManagedDoltCgroupConfig(data)
	return config, err == nil, err
}

func managedDoltCgroupForCity(cityPath string, config managedDoltCgroupConfig) (string, error) {
	city, err := filepath.EvalSymlinks(cityPath)
	if err != nil {
		return "", fmt.Errorf("resolve managed Dolt cgroup launch city: %w", err)
	}
	city, err = filepath.Abs(city)
	if err != nil {
		return "", err
	}
	canonicalCity, err := filepath.EvalSymlinks(config.CityPath)
	if err != nil {
		return "", fmt.Errorf("resolve declared canonical Dolt city: %w", err)
	}
	if city != canonicalCity {
		return "", nil
	}
	return config.CgroupPath, nil
}

// startManagedDoltCommand is the shared spawn boundary for the production
// watchdog and sql-server, including direct starts with the watchdog disabled.
// CgroupFD is consumed by clone3 before exec: the returned PID still identifies
// the actual managed child, and placement failure cannot leave a running child
// in the caller's worker role. Keep the descriptor open through Start only.
func startManagedDoltCommand(cmd *exec.Cmd, cityPath string) error {
	if managedDoltTestModeEnabled() {
		return cmd.Start()
	}
	config, declared, err := readManagedDoltCgroupConfig()
	if err != nil {
		return err
	}
	if !declared {
		return cmd.Start()
	}
	cgroup, err := managedDoltCgroupForCity(cityPath, config)
	if err != nil {
		return err
	}
	if cgroup == "" {
		return cmd.Start()
	}
	// Reject symlink aliases as well as non-cgroup directories: declaration
	// syntax alone cannot prove that a pathname still names the intended role.
	resolved, err := filepath.EvalSymlinks(cgroup)
	if err != nil {
		return fmt.Errorf("resolve canonical Dolt cgroup %s: %w", cgroup, err)
	}
	if resolved != cgroup {
		return fmt.Errorf("canonical Dolt cgroup must not contain symlinks: %s", cgroup)
	}
	fd, err := unix.Open(cgroup, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open canonical Dolt cgroup %s: %w", cgroup, err)
	}
	defer unix.Close(fd) //nolint:errcheck
	var stat unix.Statfs_t
	if err := unix.Fstatfs(fd, &stat); err != nil {
		return fmt.Errorf("inspect canonical Dolt cgroup %s: %w", cgroup, err)
	}
	if stat.Type != unix.CGROUP2_SUPER_MAGIC {
		return fmt.Errorf("canonical Dolt cgroup %s is not on cgroup v2", cgroup)
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.UseCgroupFD = true
	cmd.SysProcAttr.CgroupFD = fd
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("launch managed Dolt in canonical cgroup %s: %w", cgroup, err)
	}
	return nil
}

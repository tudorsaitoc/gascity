package scripts_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// buildRecipe extracts the command lines of the Makefile's `build` target.
func buildRecipe(t *testing.T, makefile string) string {
	t.Helper()
	recipe := regexp.MustCompile(`(?m)^build:\n((?:\t[^\n]+\n?)+)`).FindStringSubmatch(makefile)
	if len(recipe) != 2 {
		t.Fatal("Makefile has no build target with a command recipe")
	}
	return recipe[1]
}

func readMakefile(t *testing.T) string {
	t.Helper()
	makefile, err := os.ReadFile(filepath.Join(repoRoot(t), "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	return string(makefile)
}

// TestBuildTargetDisablesToolchainVCSStamping asserts that `make build` keeps
// the Go toolchain out of the provenance business.
//
// Go's buildvcs identifies a repository by looking for a `.git` *directory*.
// Inside a git worktree `.git` is a gitdir *file*, so the toolchain does not
// recognize the worktree as a repository root and keeps walking up the
// filesystem. Polecat worktrees live under the city directory, which is itself
// a git repo, so buildvcs finds the *city's* `.git` and stamps the city's
// commit and the city's dirtiness into gc — a pristine gascity checkout gets
// labeled with an unrelated repository's revision and a false `-dirty`
// (ga-u7fb).
//
// The Makefile's own VERSION/COMMIT/DIRTY variables run git in the build
// directory, which resolves the gitdir pointer correctly and is therefore
// right regardless of how the worktree is nested. Passing -buildvcs=false
// makes those variables the single source of truth instead of leaving two
// competing stamps in the binary, one of which can describe another project.
func TestBuildTargetDisablesToolchainVCSStamping(t *testing.T) {
	recipe := buildRecipe(t, readMakefile(t))
	if !strings.Contains(recipe, "build-production") {
		t.Fatalf("build recipe does not invoke the shared production entrypoint:\n%s", recipe)
	}
	script, err := os.ReadFile(filepath.Join(repoRoot(t), "scripts", "build-production"))
	if err != nil {
		t.Fatalf("read production build script: %v", err)
	}
	if !strings.Contains(string(script), "-buildvcs=false") {
		t.Fatalf("production build script must pass -buildvcs=false so the Go toolchain cannot stamp an "+
			"enclosing repository's commit into gc when built from a git worktree (ga-u7fb):\n%s", script)
	}
}

func TestBuildUsesMeasuredProductionEntrypoint(t *testing.T) {
	makefile := readMakefile(t)
	if !strings.Contains(makefile, "scripts/build-production") {
		t.Fatal("make build must delegate to scripts/build-production")
	}

	script, err := os.ReadFile(filepath.Join(repoRoot(t), "scripts", "build-production"))
	if err != nil {
		t.Fatalf("read production build script: %v", err)
	}
	for _, want := range []string{"GOCACHE", "GOTMPDIR", "flock", "GC_BUILD_CACHE_MAX_BYTES", "GC_BUILD_GOTMP_MAX_BYTES", "-trimpath", "-buildvcs=false", "flags_digest", "artifact_sha256", "max_rss_kib", "gotmp_peak_bytes", "cache_peak_bytes", "cache_bytes_after", "/dev/shm", "XDG_RUNTIME_DIR"} {
		if !strings.Contains(string(script), want) {
			t.Errorf("production build script must record/enforce %q", want)
		}
	}
}

func TestProductionBuildTestKeepsTimingReceipts(t *testing.T) {
	script, err := os.ReadFile(filepath.Join(repoRoot(t), "scripts", "build-production-test"))
	if err != nil {
		t.Fatalf("read production build test script: %v", err)
	}
	if strings.Contains(string(script), "mktemp -d)") {
		t.Fatal("production build timing receipts must not be created in an unbounded temporary directory")
	}
	for _, want := range []string{".cache/build-production-test", "GC_BUILD_RECEIPT_DIR"} {
		if !strings.Contains(string(script), want) {
			t.Errorf("production build test script must retain timing receipt evidence at %q", want)
		}
	}
}

// TestBuildStampsWorkingTreeDirtiness asserts that disabling buildvcs did not
// silently drop dirty detection along with the bogus stamp. `gc version
// --long`, the supervisor's /health build_id, and binary-drift detection all
// read the same injected commit string, so the `-dirty` marker has to come
// from somewhere — and with buildvcs off, that somewhere is git run in the
// build directory.
func TestBuildStampsWorkingTreeDirtiness(t *testing.T) {
	makefile := readMakefile(t)

	dirty := regexp.MustCompile(`(?m)^DIRTY\s*:?=\s*(.+)$`).FindStringSubmatch(makefile)
	if len(dirty) != 2 {
		t.Fatal("Makefile defines no DIRTY variable; buildvcs is off, so nothing would mark a dirty build")
	}
	if !strings.Contains(dirty[1], "git status --porcelain") {
		t.Fatalf("DIRTY must be derived from `git status --porcelain` in the build directory, got: %s", dirty[1])
	}
	if !strings.Contains(dirty[1], "-dirty") {
		t.Fatalf("DIRTY must expand to the -dirty suffix consumers already parse, got: %s", dirty[1])
	}

	ldflags := regexp.MustCompile(`(?ms)^LDFLAGS\s*:?=\s*(.*?)\n\n`).FindStringSubmatch(makefile)
	if len(ldflags) != 2 {
		t.Fatal("Makefile has no LDFLAGS assignment")
	}
	commitFlag := regexp.MustCompile(`-X main\.commit=(\S+)`).FindStringSubmatch(ldflags[1])
	if len(commitFlag) != 2 {
		t.Fatalf("LDFLAGS does not inject main.commit:\n%s", ldflags[1])
	}
	if !strings.Contains(commitFlag[1], "$(DIRTY)") {
		t.Fatalf("main.commit must carry $(DIRTY) so a modified tree is visible in `gc version --long`, got: %s", commitFlag[1])
	}
}

func TestBuildTimestampIsStableForACommit(t *testing.T) {
	makefile := readMakefile(t)

	buildTime := regexp.MustCompile(`(?m)^BUILD_TIME\s*:?=\s*(.+)$`).FindStringSubmatch(makefile)
	if len(buildTime) != 2 {
		t.Fatal("Makefile has no BUILD_TIME variable")
	}
	if strings.Contains(buildTime[1], "date -u") {
		t.Fatalf("BUILD_TIME must not use the wall clock because it changes the Go linker action on every production build: %s", buildTime[1])
	}
	if !strings.Contains(buildTime[1], "git show") {
		t.Fatalf("BUILD_TIME must derive from the commit so unchanged production builds share a linker action: %s", buildTime[1])
	}
}

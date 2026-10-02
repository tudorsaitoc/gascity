package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	convoycore "github.com/gastownhall/gascity/internal/convoy"
	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/graphroute"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/shellquote"
	"github.com/gastownhall/gascity/internal/sling"
	"github.com/gastownhall/gascity/internal/sourceworkflow"
)

// selectiveErrStore wraps a beads.Store and injects Create errors for selected
// beads. Used to simulate partial cook failures in batch operations.
type selectiveErrStore struct {
	*beads.MemStore
	failOnParentIDs map[string]error
	failOnCreate    func(beads.Bead) error
}

func (s *selectiveErrStore) Create(b beads.Bead) (beads.Bead, error) {
	if s.failOnCreate != nil {
		if err := s.failOnCreate(b); err != nil {
			return beads.Bead{}, err
		}
	}
	if err, ok := s.failOnParentIDs[b.ParentID]; ok {
		return beads.Bead{}, err
	}
	return s.MemStore.Create(b)
}

func (s *selectiveErrStore) TxSingle(message string, fn func(beads.Tx) error) error {
	return s.MemStore.TxSingle(message, func(tx beads.Tx) error {
		staged := tx.(*beads.MemStore)
		return fn(&selectiveErrStore{
			MemStore: staged, failOnParentIDs: s.failOnParentIDs, failOnCreate: s.failOnCreate,
		})
	})
}

type getErrStore struct {
	beads.Store
	err error
}

func (s *getErrStore) Get(_ string) (beads.Bead, error) {
	return beads.Bead{}, s.err
}

func seededStore(ids ...string) beads.Store {
	seed := make([]beads.Bead, 0, len(ids))
	for _, id := range ids {
		seed = append(seed, beads.Bead{
			ID:       id,
			Title:    id,
			Type:     "task",
			Status:   "open",
			Metadata: map[string]string{},
		})
	}
	return beads.NewMemStoreFrom(0, seed, nil)
}

// fakeRunnerRule maps a command substring to a canned response.
type fakeRunnerRule struct {
	prefix string
	out    string
	err    error
}

func newSlingTestStore() *beads.MemStore {
	// These are real canonical rows, not a Get fallback that invents missing
	// work. Tests with other identities or state must seed their own store.
	ids := []string{"BL-1", "BL-2", "BL-3", "BL-42", "BL-99", "HW-7", "HW-42", "FR-99", "MY-42", "my-1", "FE-123"}
	rows := make([]beads.Bead, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, beads.Bead{ID: id, Title: id, Type: "task", Status: "open"})
	}
	store := beads.NewMemStoreFrom(0, rows, nil)
	store.HonorExplicitIDs = true
	return store
}

// fakeRunner records the commands it receives and returns canned output.
// Rules are matched in order (first match wins), providing deterministic behavior.
type fakeRunner struct {
	calls []string
	dirs  []string
	envs  []map[string]string
	rules []fakeRunnerRule
}

func newFakeRunner() *fakeRunner { return &fakeRunner{} }

// on registers a rule: if a command contains prefix, return (out, err).
func (r *fakeRunner) on(prefix, out string, err error) {
	r.rules = append(r.rules, fakeRunnerRule{prefix: prefix, out: out, err: err})
}

func (r *fakeRunner) run(dir, command string, env map[string]string) (string, error) {
	r.calls = append(r.calls, command)
	r.dirs = append(r.dirs, dir)
	r.envs = append(r.envs, env)
	for _, rule := range r.rules {
		if strings.Contains(command, rule.prefix) {
			return rule.out, rule.err
		}
	}
	return "", nil
}

// testOpts constructs a slingOpts for testing with the given agent and bead.
func testOpts(a config.Agent, beadOrFormula string) slingOpts {
	return slingOpts{Target: a, BeadOrFormula: beadOrFormula}
}

// testDeps constructs a slingDeps for testing, returning the deps and
// stdout/stderr buffers for inspection. The config's FormulaLayers.City
// is automatically populated with common test formulas.
func testDeps(cfg *config.City, sp runtime.Provider, runner SlingRunner) (slingDeps, *bytes.Buffer, *bytes.Buffer) {
	if cfg != nil && len(cfg.FormulaLayers.City) == 0 {
		cfg.FormulaLayers.City = []string{sharedTestFormulaDir}
	}
	var stdout, stderr bytes.Buffer
	return slingDeps{
		CityName: "test-city",
		CityPath: sharedTestCityDir,
		Cfg:      cfg,
		SP:       sp,
		Runner:   runner,
		Store:    newSlingTestStore(),
		StoreRef: "city:test-city",
	}, &stdout, &stderr
}

func writeSlingTestCity(t *testing.T, cityDir, content string) {
	t.Helper()
	cfg, err := config.Parse([]byte(content))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(cityDir, ".gc"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := config.PersistRigSiteBindings(fsys.OSFS{}, cityDir, cfg.Rigs); err != nil {
		t.Fatal(err)
	}
	raw, err := cfg.MarshalForWrite()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func installSlingMemStores(t *testing.T, cityDir string, scopes map[string][]beads.Bead) {
	t.Helper()
	stores := make(map[string]beads.Store, len(scopes)+1)
	stores[cityDir] = beads.NewMemStore()
	for dir, rows := range scopes {
		store := beads.NewMemStoreFrom(0, rows, nil)
		store.HonorExplicitIDs = true
		stores[dir] = store
	}
	previous := slingOpenStoreAtForCity
	slingOpenStoreAtForCity = func(dir, city string) (beads.Store, error) {
		if city != cityDir {
			return nil, fmt.Errorf("unexpected sling city %q", city)
		}
		store, ok := stores[dir]
		if !ok {
			return nil, fmt.Errorf("unexpected sling scope %q", dir)
		}
		return store, nil
	}
	t.Cleanup(func() { slingOpenStoreAtForCity = previous })
}

//nolint:unused // retained for future sling cwd-sensitive scenarios
func chdirSlingTest(t *testing.T, dir string) {
	t.Helper()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(orig) })
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
}

func assertStoreRoutedTo(t *testing.T, store beads.Store, beadID, want string) {
	t.Helper()
	bead, err := store.Get(beadID)
	if err != nil {
		t.Fatalf("store.Get(%s): %v", beadID, err)
	}
	if bead.Metadata["gc.routed_to"] != want {
		t.Fatalf("%s gc.routed_to = %q, want %q", beadID, bead.Metadata["gc.routed_to"], want)
	}
}

// sharedTestFormulaDir is a package-level temp directory containing minimal
// formula TOML files for all formula names commonly used in sling tests.
var (
	sharedTestFixtureRoot string
	sharedTestFormulaDir  string
	sharedTestCityDir     string
)

func initSharedSlingTestFixtures(root string) {
	fixtureRoot, err := os.MkdirTemp(root, pidPrefixedTempPattern(testSharedFixtureDirPrefix))
	if err != nil {
		panic(err)
	}
	sharedTestFixtureRoot = fixtureRoot

	dir := filepath.Join(fixtureRoot, "formulas")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	for _, name := range []string{
		"code-review", "mol-feature", "mol-polecat-work", "mol-do-work",
		"mol-refinery-patrol", "review", "build", "test-formula",
		"bad-formula", "mol-polecat-pr", "custom-formula",
		"mol-digest", "mol-cleanup", "mol-db-health", "mol-health-check",
		"my-formula", "convoy-formula",
	} {
		content := fmt.Sprintf("formula = %q\nversion = 1\n\n[[steps]]\nid = \"work\"\ntitle = \"Work\"\n", name)
		_ = os.WriteFile(filepath.Join(dir, name+".toml"), []byte(content), 0o644)
	}
	sharedTestFormulaDir = dir

	cityDir := filepath.Join(fixtureRoot, "city")
	if err := os.MkdirAll(cityDir, 0o755); err != nil {
		panic(err)
	}
	sharedTestCityDir = cityDir
}

func testFormulaDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func gitCmd(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func newRepoWithOriginHead(t *testing.T, branch string) string {
	t.Helper()
	return newRepoWithOriginHeadAt(t, t.TempDir(), branch)
}

// newRepoWithOriginHeadAt git-inits a repo at dir (created if absent) with
// origin/HEAD pointing at branch. Use it when the repo must live under a
// specific parent — e.g. inside the city dir, since the API rig-create now
// contains rig paths to the city root.
func newRepoWithOriginHeadAt(t *testing.T, dir, branch string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	gitCmd(t, dir, "init")
	gitCmd(t, dir, "symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/"+branch)
	return dir
}

func findVarValue(vars map[string]string, key string) (string, bool) {
	v, ok := vars[key]
	return v, ok
}

func priorityPtr(v int) *int {
	return &v
}

func TestBuildSlingCommand(t *testing.T) {
	tests := []struct {
		template string
		beadID   string
		want     string
	}{
		{"bd update {} --set-metadata gc.routed_to=mayor", "BL-42", "bd update 'BL-42' --set-metadata gc.routed_to=mayor"},
		{"bd update {} --add-label=pool:hw/polecat", "XY-7", "bd update 'XY-7' --add-label=pool:hw/polecat"},
		{"custom {} script {}", "ID-1", "custom 'ID-1' script 'ID-1'"},
	}
	for _, tt := range tests {
		got := sling.BuildSlingCommand(tt.template, tt.beadID)
		if got != tt.want {
			t.Errorf("BuildSlingCommand(%q, %q) = %q, want %q", tt.template, tt.beadID, got, tt.want)
		}
	}
}

func TestSlingJSONFromResult(t *testing.T) {
	result := sling.SlingResult{
		Target:      "repo-a/polecat",
		Method:      "formula",
		BeadID:      "repo-a-1",
		FormulaName: "pack-review",
		WorkflowID:  "wf-1",
		ConvoyID:    "convoy-1",
		Routed:      1,
		NudgeAgent:  &config.Agent{Name: "polecat", Dir: "repo-a"},
	}

	got := slingJSONFromResult(result)
	if got.SchemaVersion != "1" || !got.Success {
		t.Fatalf("schema/success = %q/%v, want v1 success", got.SchemaVersion, got.Success)
	}
	if got.Target != "repo-a/polecat" || got.BeadID != "repo-a-1" || got.Formula != "pack-review" {
		t.Fatalf("payload = %+v, want target/bead/formula refs", got)
	}
	if !got.Routed || !got.Queued || got.WorkflowID != "wf-1" || got.ConvoyID != "convoy-1" {
		t.Fatalf("payload = %+v, want routed queued workflow convoy refs", got)
	}
}

func TestDoSlingBeadToFixedAgent(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for built-in routing: %v", len(runner.calls), runner.calls)
	}
	bead, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("store.Get(BL-42): %v", err)
	}
	if bead.Metadata["gc.routed_to"] != "mayor" {
		t.Errorf("gc.routed_to = %q, want mayor", bead.Metadata["gc.routed_to"])
	}
	if !strings.Contains(stdout.String(), "Slung BL-42") {
		t.Errorf("stdout = %q, want to contain 'Slung BL-42'", stdout.String())
	}
}

func TestDoSlingPinnedDefaultSlingQueryUsesBuiltInRouting(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{
		Name:              "mayor",
		MaxActiveSessions: intPtr(1),
		SlingQuery:        "bd update {} --set-metadata gc.routed_to=mayor",
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for pinned default sling_query: %v", len(runner.calls), runner.calls)
	}
	bead, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("store.Get(BL-42): %v", err)
	}
	if bead.Metadata["gc.routed_to"] != "mayor" {
		t.Errorf("gc.routed_to = %q, want mayor", bead.Metadata["gc.routed_to"])
	}
	if !strings.Contains(stdout.String(), "Slung BL-42") {
		t.Errorf("stdout = %q, want to contain 'Slung BL-42'", stdout.String())
	}
}

func TestShellSlingRunnerOverridesInheritedBDEnv(t *testing.T) {
	t.Setenv("GC_DOLT_HOST", "stale-host")
	t.Setenv("GC_DOLT_PORT", "9999")
	t.Setenv("BEADS_DOLT_SERVER_HOST", "stale-host")
	t.Setenv("BEADS_DOLT_SERVER_PORT", "9999")

	out, err := shellSlingRunner("", `printf '%s|%s|%s|%s' "$GC_DOLT_HOST" "$GC_DOLT_PORT" "$BEADS_DOLT_SERVER_HOST" "$BEADS_DOLT_SERVER_PORT"`, map[string]string{
		"GC_DOLT_HOST":           "rig-db.example.com",
		"GC_DOLT_PORT":           "3307",
		"BEADS_DOLT_SERVER_HOST": "rig-db.example.com",
		"BEADS_DOLT_SERVER_PORT": "3307",
	})
	if err != nil {
		t.Fatalf("shellSlingRunner: %v", err)
	}
	if got := strings.TrimSpace(out); got != "rig-db.example.com|3307|rig-db.example.com|3307" {
		t.Fatalf("shellSlingRunner env = %q", got)
	}
}

func TestShellSlingRunnerStripsInheritedSecrets(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "ghs_should_not_leak")
	t.Setenv("OPENAI_API_KEY", "sk-should-not-leak")

	out, err := shellSlingRunner("", `printf '%s|%s' "${GITHUB_TOKEN:-unset}" "${OPENAI_API_KEY:-unset}"`, nil)
	if err != nil {
		t.Fatalf("shellSlingRunner: %v", err)
	}
	if got := strings.TrimSpace(out); got != "unset|unset" {
		t.Fatalf("shellSlingRunner inherited secrets = %q, want unset|unset", got)
	}
}

func TestSourceWorkflowCleanupCommandQuotesUntrustedArgs(t *testing.T) {
	got := sourceWorkflowCleanupCommand("ga-1; touch /tmp/pwn", "rig:demo; rm -rf /")
	if got == "gc workflow delete-source ga-1; touch /tmp/pwn --store-ref rig:demo; rm -rf / --apply" {
		t.Fatalf("cleanup command left shell metacharacters unquoted: %q", got)
	}
	args := shellquote.Split(got)
	want := []string{"gc", "workflow", "delete-source", "ga-1; touch /tmp/pwn", "--store-ref", "rig:demo; rm -rf /", "--apply"}
	if len(args) != len(want) {
		t.Fatalf("cleanup command args = %#v, want %#v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("cleanup command arg[%d] = %q, want %q (command %q)", i, args[i], want[i], got)
		}
	}
}

func TestDoSlingBeadToPool(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{
		Name:              "polecat",
		Dir:               "hello-world",
		MinActiveSessions: intPtr(1), MaxActiveSessions: intPtr(3),
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "HW-7")
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for built-in routing: %v", len(runner.calls), runner.calls)
	}
	bead, err := deps.Store.Get("HW-7")
	if err != nil {
		t.Fatalf("store.Get(HW-7): %v", err)
	}
	if bead.Metadata["gc.routed_to"] != "hello-world/polecat" {
		t.Errorf("gc.routed_to = %q, want hello-world/polecat", bead.Metadata["gc.routed_to"])
	}
}

func TestDoSlingRefusesCrossStoreRoute(t *testing.T) {
	cityPath := t.TempDir()
	rigPath := filepath.Join(cityPath, "alpha")
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "alpha", Path: rigPath}},
		Agents: []config.Agent{{
			Name:              "polecat",
			Dir:               "alpha",
			MinActiveSessions: intPtr(0), MaxActiveSessions: intPtr(3),
		}},
	}
	store := newSlingTestStore()
	if _, err := store.Create(beads.Bead{ID: "HQ-1", Type: "task", Status: "open"}); err != nil {
		t.Fatalf("seed HQ-1: %v", err)
	}
	runner := newFakeRunner()
	deps, stdout, stderr := testDeps(cfg, runtime.NewFake(), runner.run)
	deps.CityPath = cityPath
	deps.Store = store
	deps.StoreRef = "city:test-city"
	opts := testOpts(cfg.Agents[0], "HQ-1")
	opts.Force = true

	if code := doSling(opts, deps, &fakeQuerier{bead: beads.Bead{ID: "HQ-1", Type: "task", Status: "open"}}, stdout, stderr); code == 0 {
		t.Fatalf("doSling returned 0, want cross-store refusal; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	msg := stderr.String()
	for _, want := range []string{"refusing cross-store route", "city:test-city", "rig:alpha", "alpha/polecat", "tr-6s7yx"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q: %s", want, msg)
		}
	}

	// Verify the bead's metadata was NOT mutated.
	bead, err := store.Get("HQ-1")
	if err != nil {
		t.Fatalf("store.Get(HQ-1): %v", err)
	}
	if bead.Metadata["gc.routed_to"] != "" {
		t.Errorf("guard did not block SetMetadata: gc.routed_to = %q", bead.Metadata["gc.routed_to"])
	}
}

func TestCliBeadRouterAllowsSameStoreRoute(t *testing.T) {
	cityPath := t.TempDir()
	rigPath := filepath.Join(cityPath, "alpha")
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "alpha", Path: rigPath}},
		Agents: []config.Agent{{
			Name:              "polecat",
			Dir:               "alpha",
			MinActiveSessions: intPtr(0), MaxActiveSessions: intPtr(3),
		}},
	}
	store := newSlingTestStore()
	if _, err := store.Create(beads.Bead{ID: "RIG-1", Type: "task", Status: "open"}); err != nil {
		t.Fatalf("seed RIG-1: %v", err)
	}
	deps := &slingDeps{
		CityName: "test-city",
		CityPath: cityPath,
		Cfg:      cfg,
		Store:    store,
		StoreRef: "rig:alpha",
	}
	router := cliBeadRouter{deps: deps}

	if err := router.Route(context.Background(), sling.RouteRequest{
		BeadID: "RIG-1",
		Target: "alpha/polecat",
	}); err != nil {
		t.Fatalf("same-store route should succeed, got: %v", err)
	}
	bead, err := store.Get("RIG-1")
	if err != nil {
		t.Fatalf("store.Get(RIG-1): %v", err)
	}
	if bead.Metadata["gc.routed_to"] != "alpha/polecat" {
		t.Errorf("gc.routed_to = %q, want alpha/polecat", bead.Metadata["gc.routed_to"])
	}
}

func TestCliBeadRouterKeepsNativeSelectionAfterProviderConfigDrift(t *testing.T) {
	cityPath := t.TempDir()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:              "mayor",
			MaxActiveSessions: intPtr(1),
			SlingQuery:        "must-not-execute {}",
		}},
	}
	store := newSlingTestStore()
	if _, err := store.Create(beads.Bead{ID: "HQ-2", Title: "original goal", Type: "task", Status: "open"}); err != nil {
		t.Fatalf("seed HQ-2: %v", err)
	}
	providerCalled := false
	deps := &slingDeps{
		CityName: "test-city",
		CityPath: cityPath,
		Cfg:      cfg,
		Store:    store,
		StoreRef: "city:test-city",
		Runner: func(_, _ string, _ map[string]string) (string, error) {
			providerCalled = true
			wrongGoal := "wrong provider goal"
			return "", store.Update("HQ-2", beads.UpdateOpts{Title: &wrongGoal})
		},
	}
	router := cliBeadRouter{deps: deps}

	if err := router.Route(context.Background(), sling.RouteRequest{
		BeadID: "HQ-2",
		Target: "mayor",
	}); err != nil {
		t.Fatalf("HQ->HQ route should succeed, got: %v", err)
	}
	bead, err := store.Get("HQ-2")
	if err != nil {
		t.Fatal(err)
	}
	if providerCalled || bead.Title != "original goal" || bead.Metadata["gc.routed_to"] != "mayor" {
		t.Fatalf("native selection adopted a late custom provider: called=%v bead=%+v", providerCalled, bead)
	}
}

func TestDoSlingFormulaToAgent(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "code-review")
	opts.IsFormula = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for built-in routing: %v", len(runner.calls), runner.calls)
	}
	root, err := deps.Store.Get("gc-1")
	if err != nil {
		t.Fatalf("store.Get(gc-1): %v", err)
	}
	if root.Metadata["gc.routed_to"] != "mayor" {
		t.Errorf("gc.routed_to = %q, want mayor", root.Metadata["gc.routed_to"])
	}
	if !strings.Contains(stdout.String(), "formula") && !strings.Contains(stdout.String(), "wisp root gc-1") {
		t.Errorf("stdout = %q, want mention of formula/wisp", stdout.String())
	}
}

func TestDoSlingFormulaWithTitle(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "code-review")
	opts.IsFormula = true
	opts.Title = "my-review"
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	// MolCook goes through the store; verify the bead was created with the title.
	b, err := deps.Store.Get("gc-1")
	if err != nil {
		t.Fatalf("store.Get(gc-1): %v", err)
	}
	if b.Title != "my-review" {
		t.Errorf("bead title = %q, want %q", b.Title, "my-review")
	}
}

func TestDoSlingSuspendedAgentWarns(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", Suspended: true, MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-1")
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0 (still routes)", code)
	}
	if !strings.Contains(stderr.String(), "suspended") {
		t.Errorf("stderr = %q, want suspended warning", stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0 for built-in routing", len(runner.calls))
	}
	assertStoreRoutedTo(t, deps.Store, "BL-1", "mayor")
}

func TestDoSlingSuspendedAgentForce(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", Suspended: true, MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-1")
	opts.Force = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0", code)
	}
	if strings.Contains(stderr.String(), "suspended") {
		t.Errorf("--force should suppress warning; stderr = %q", stderr.String())
	}
}

func TestDoSlingSuspendedRigWarns(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "myrig", Suspended: true}},
	}
	a := config.Agent{Name: "polecat", Dir: "myrig", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	// Rig-scoped agents read their rig's store; match it so the
	// cross-store route guard does not trip before the rig check.
	deps.StoreRef = "rig:myrig"
	opts := testOpts(a, "my-1")
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0 (still routes)", code)
	}
	if !strings.Contains(stderr.String(), `rig "myrig" is suspended`) {
		t.Errorf("stderr = %q, want suspended-rig warning", stderr.String())
	}
	if !strings.Contains(stderr.String(), "gc rig resume myrig") {
		t.Errorf("stderr = %q, want resume hint", stderr.String())
	}
	assertStoreRoutedTo(t, deps.Store, "my-1", "myrig/polecat")
}

func TestDoSlingSuspendedRigForce(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "myrig", Suspended: true}},
	}
	a := config.Agent{Name: "polecat", Dir: "myrig", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	// Rig-scoped agents read their rig's store; match it so the
	// cross-store route guard does not trip before the rig check.
	deps.StoreRef = "rig:myrig"
	opts := testOpts(a, "my-1")
	opts.Force = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0", code)
	}
	if strings.Contains(stderr.String(), "suspended") {
		t.Errorf("--force should suppress warning; stderr = %q", stderr.String())
	}
}

func TestSlingJSONWarningsSuspendedRig(t *testing.T) {
	warnings := slingJSONWarnings(sling.SlingResult{SuspendedRig: "myrig"})
	if !slices.Contains(warnings, "rig_suspended") {
		t.Errorf("warnings = %v, want to contain rig_suspended", warnings)
	}
}

func TestDoSlingMultiSessionMaxZeroWarns(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{
		Name:              "polecat",
		Dir:               "rig",
		MinActiveSessions: intPtr(0), MaxActiveSessions: intPtr(0),
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-1")
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0 (still routes)", code)
	}
	if !strings.Contains(stderr.String(), "session config") || !strings.Contains(stderr.String(), "max_active_sessions=0") {
		t.Errorf("stderr = %q, want session config max_active_sessions=0 warning", stderr.String())
	}
}

func TestDoSlingMultiSessionMaxZeroForce(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{
		Name:              "polecat",
		Dir:               "rig",
		MinActiveSessions: intPtr(0), MaxActiveSessions: intPtr(0),
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-1")
	opts.Force = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0", code)
	}
	if strings.Contains(stderr.String(), "max=0") {
		t.Errorf("--force should suppress warning; stderr = %q", stderr.String())
	}
}

func TestDoSlingRunnerError(t *testing.T) {
	for _, deliveryErr := range []error{errors.New("dispatch failed"), context.DeadlineExceeded} {
		t.Run(deliveryErr.Error(), func(t *testing.T) {
			a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1), SlingQuery: "custom-dispatch {}"}
			cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}, Agents: []config.Agent{a}}
			deps, stdout, stderr := testDeps(cfg, runtime.NewFake(), nil)
			deps.CityPath = t.TempDir()
			if err := os.WriteFile(filepath.Join(deps.CityPath, "host-admitted"), []byte("ready"), 0o600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("GC_SLING_ADMISSION_COMMAND", `if test -f "$GC_CITY_PATH/host-admitted"; then printf '{"allowed":true}'; else printf '{"allowed":false}'; fi`)
			work := beads.Bead{ID: "BL-1", Title: "original goal", Description: "original scope", AcceptanceCriteria: "original outcome", Type: "task", Status: "open"}
			deps.Store = beads.NewMemStoreFrom(0, []beads.Bead{work}, nil)
			deliveries := 0
			deps.Runner = func(_ string, _ string, _ map[string]string) (string, error) {
				deliveries++
				return "", deliveryErr
			}
			opts := testOpts(a, work.ID)
			opts.OnFormula, opts.NoConvoy = "code-review", true
			if code := doSling(opts, deps, deps.Store, stdout, stderr); code != 13 {
				t.Fatalf("delivery failure exit=%d, want13; stderr=%s", code, stderr.String())
			}
			source, err := deps.Store.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			effectID, rootID := source.Metadata[sling.DispatchEffectIDKey], source.Metadata["molecule_id"]
			if effectID == "" || rootID == "" || source.Metadata[sling.DispatchEffectStateKey] != "unknown" {
				t.Fatalf("failed delivery lost unresolved original effect: %+v", source)
			}
			if source.Title != work.Title || source.Description != work.Description || source.AcceptanceCriteria != work.AcceptanceCriteria || source.Assignee != work.Assignee {
				t.Fatalf("failed delivery changed original work: %+v", source)
			}
			root, err := deps.Store.Get(rootID)
			if err != nil {
				t.Fatal(err)
			}
			if root.Metadata["gc.attach_fence_pending"] != "true" || root.Metadata[sling.DispatchEffectIDKey] != effectID {
				t.Fatalf("failed delivery activated or lost original candidate: %+v", root)
			}
			before, err := deps.Store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, TierMode: beads.TierBoth})
			if err != nil {
				t.Fatal(err)
			}
			stdout.Reset()
			stderr.Reset()
			if code := doSling(opts, deps, deps.Store, stdout, stderr); code != 13 {
				t.Fatalf("ambiguous delivery retry exit=%d, want13; stderr=%s", code, stderr.String())
			}
			after, err := deps.Store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, TierMode: beads.TierBoth})
			if err != nil {
				t.Fatal(err)
			}
			if deliveries != 1 || !reflect.DeepEqual(after, before) {
				t.Fatalf("retry duplicated delivery or materialization: deliveries=%d before=%+v after=%+v", deliveries, before, after)
			}
		})
	}
}

func TestDoSlingFormulaInstantiationError(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "nonexistent")
	opts.IsFormula = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSling returned %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "not found") {
		t.Errorf("stderr = %q, want formula error", stderr.String())
	}
}

func TestDoSlingNudgeFixedAgent(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	sp.Calls = nil // clear start call
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.CityPath = t.TempDir()
	prev := startNudgePoller
	startNudgePoller = func(_, _, _ string) error { return nil }
	t.Cleanup(func() { startNudgePoller = prev })
	opts := testOpts(a, "BL-1")
	opts.Nudge = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	pending, _, dead, err := listQueuedNudges(deps.CityPath, "mayor", time.Now())
	if err != nil {
		t.Fatalf("listQueuedNudges: %v", err)
	}
	if len(pending) != 1 || len(dead) != 0 {
		t.Fatalf("pending=%d dead=%d, want 1/0", len(pending), len(dead))
	}
	if pending[0].Source != "sling" {
		t.Fatalf("source = %q, want sling", pending[0].Source)
	}
	if !strings.Contains(stdout.String(), "Queued nudge for mayor") {
		t.Errorf("stdout = %q, want queue confirmation", stdout.String())
	}
}

func TestDoSlingNudgeNoSession(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	// Don't start the session — agent has no running session.
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.CityPath = t.TempDir() // isolated path so poke doesn't hit real socket
	opts := testOpts(a, "BL-1")
	opts.Nudge = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0 (sling succeeds, poke attempted)", code)
	}
	if !strings.Contains(stderr.String(), "poke failed") {
		t.Errorf("stderr = %q, want 'poke failed' message (no controller socket in test)", stderr.String())
	}
	pending, _, dead, err := listQueuedNudges(deps.CityPath, "mayor", time.Now())
	if err != nil {
		t.Fatalf("listQueuedNudges: %v", err)
	}
	if len(pending) != 1 || len(dead) != 0 {
		t.Fatalf("pending=%d dead=%d, want 1/0", len(pending), len(dead))
	}
}

func TestDoSlingNudgeSuspended(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", Suspended: true, MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-1")
	opts.Nudge = true
	opts.Force = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0", code)
	}
	if !strings.Contains(stderr.String(), "cannot nudge") {
		t.Errorf("stderr = %q, want 'cannot nudge: suspended' warning", stderr.String())
	}
}

func TestDoSlingNudgePoolMember(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	// Start pool instance 2 (instance 1 not running).
	_ = sp.Start(context.Background(), "hw--polecat-2", runtime.Config{})
	sp.Calls = nil
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{
		Name:              "polecat",
		Dir:               "hw",
		MinActiveSessions: intPtr(1), MaxActiveSessions: intPtr(3),
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.CityPath = t.TempDir()
	prev := startNudgePoller
	startNudgePoller = func(_, _, _ string) error { return nil }
	t.Cleanup(func() { startNudgePoller = prev })
	opts := testOpts(a, "BL-1")
	opts.Nudge = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	for _, c := range sp.Calls {
		if c.Method == "Nudge" {
			t.Fatalf("expected queued sling reminder, got direct nudge calls: %+v", sp.Calls)
		}
	}
}

func TestDoSlingNudgePoolMemberUsesBeadDerivedSessionName(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	const sessionName = "gm-glz06f"
	if err := sp.Start(context.Background(), sessionName, runtime.Config{}); err != nil {
		t.Fatalf("Start(%q): %v", sessionName, err)
	}
	sp.WaitForIdleErrors[sessionName] = nil
	sp.Calls = nil

	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city", Provider: "claude"},
		Providers: builtinProviderAliasesForTest("claude"),
		Agents: []config.Agent{{
			Name: "polecat",
			Dir:  "hw",
		}},
	}
	a := cfg.Agents[0]

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.CityPath = t.TempDir()
	store := newSlingTestStore()
	deps.Store = store
	sessionBead, err := store.Create(beads.Bead{
		Title:  "pool session",
		Type:   sessionBeadType,
		Labels: []string{sessionBeadLabel},
		Metadata: map[string]string{
			"template":      "hw/polecat",
			"agent_name":    "hw/polecat",
			"provider_kind": "claude",
			"session_name":  sessionName,
			"pool_slot":     "7",
			"state":         "active",
		},
	})
	if err != nil {
		t.Fatalf("Create(session bead): %v", err)
	}

	opts := testOpts(a, "BL-1")
	opts.Nudge = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	for _, c := range sp.Calls {
		if (c.Method == "Nudge" || c.Method == "NudgeNow") && c.Name == sessionName {
			if !strings.Contains(stdout.String(), "Nudged hw/polecat") {
				t.Fatalf("stdout = %q, want delivered nudge confirmation", stdout.String())
			}
			if strings.Contains(stdout.String(), "No running sessions") || strings.Contains(stderr.String(), "poke failed") {
				t.Fatalf("stdout=%q stderr=%q, want no controller wake fallback", stdout.String(), stderr.String())
			}
			updated, err := store.Get(sessionBead.ID)
			if err != nil {
				t.Fatalf("Get(session bead): %v", err)
			}
			if got := updated.Metadata["last_nudge_delivered_at"]; got == "" {
				t.Fatalf("last_nudge_delivered_at = %q, want delivered nudge stamp", got)
			}
			return
		}
	}
	t.Fatalf("runtime calls = %+v, want Nudge/NudgeNow on bead-derived session %q", sp.Calls, sessionName)
}

func TestDoSlingNudgePoolBeadDerivedSession(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	sessionName := "workflows__ollama-claude-mc-session-test"
	if err := sp.Start(context.Background(), sessionName, runtime.Config{}); err != nil {
		t.Fatal(err)
	}
	sp.Calls = nil
	a := config.Agent{
		Name:        "ollama-claude",
		Dir:         "gascity",
		BindingName: "workflows",
	}
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents:    []config.Agent{a},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.CityPath = t.TempDir()
	if _, err := deps.Store.Create(beads.Bead{
		Title:  "gascity/workflows.ollama-claude-3",
		Type:   sessionBeadType,
		Labels: []string{sessionBeadLabel},
		Metadata: map[string]string{
			"template":     "gascity/workflows.ollama-claude",
			"session_name": sessionName,
			"pool_slot":    "3",
		},
	}); err != nil {
		t.Fatal(err)
	}
	prev := startNudgePoller
	startNudgePoller = func(_, _, _ string) error { return nil }
	t.Cleanup(func() { startNudgePoller = prev })

	doSlingNudge(&a, deps.CityName, deps.CityPath, cfg, sp, deps.Store, stdout, stderr)
	if strings.Contains(stdout.String(), "No running sessions") || strings.Contains(stderr.String(), "poke failed") {
		t.Fatalf("sling nudge missed live bead-derived pool session; stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if stderr.Len() > 0 {
		t.Fatalf("stderr = %q, want no warning", stderr.String())
	}
	var observedLiveSession bool
	for _, call := range sp.Calls {
		if call.Method == "IsRunning" && call.Name == sessionName {
			observedLiveSession = true
			break
		}
	}
	if !observedLiveSession {
		t.Fatalf("runtime calls = %#v, want IsRunning for bead-derived session %q", sp.Calls, sessionName)
	}
	if !strings.Contains(stdout.String(), "gascity/workflows.ollama-claude-3") {
		t.Fatalf("stdout = %q, want nudge output for bead-derived pool instance", stdout.String())
	}
}

func TestDoSlingNudgePoolUsesCityStoreForSessionBeads(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	sessionName := "workflows__codex-max-mc-session-test"
	if err := sp.Start(context.Background(), sessionName, runtime.Config{}); err != nil {
		t.Fatal(err)
	}
	sp.Calls = nil
	a := config.Agent{
		Name:        "codex-max",
		Dir:         "gascity",
		BindingName: "workflows",
	}
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents:    []config.Agent{a},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.CityPath = t.TempDir()
	writeSlingTestCity(t, deps.CityPath, "[workspace]\nname = \"test-city\"\n")
	cityStore := beads.NewMemStore()
	if _, err := cityStore.Create(beads.Bead{
		Title:  "gascity/workflows.codex-max-8",
		Type:   sessionBeadType,
		Labels: []string{sessionBeadLabel},
		Metadata: map[string]string{
			"template":     "gascity/workflows.codex-max",
			"session_name": sessionName,
			"pool_slot":    "8",
		},
	}); err != nil {
		t.Fatal(err)
	}
	prevOpen := slingOpenCityStore
	slingOpenCityStore = func(path string) (beads.Store, error) {
		if path != deps.CityPath {
			t.Fatalf("slingOpenCityStore(%q), want %q", path, deps.CityPath)
		}
		return cityStore, nil
	}
	t.Cleanup(func() { slingOpenCityStore = prevOpen })
	prevPoller := startNudgePoller
	startNudgePoller = func(_, _, _ string) error { return nil }
	t.Cleanup(func() { startNudgePoller = prevPoller })

	doSlingNudge(&a, deps.CityName, deps.CityPath, cfg, sp, deps.Store, stdout, stderr)
	if strings.Contains(stdout.String(), "No running sessions") || strings.Contains(stderr.String(), "poke failed") {
		t.Fatalf("sling nudge missed live city-store pool session; stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if stderr.Len() > 0 {
		t.Fatalf("stderr = %q, want no warning", stderr.String())
	}
	var observedLiveSession bool
	for _, call := range sp.Calls {
		if call.Method == "IsRunning" && call.Name == sessionName {
			observedLiveSession = true
			break
		}
	}
	if !observedLiveSession {
		t.Fatalf("runtime calls = %#v, want IsRunning for city-store session %q", sp.Calls, sessionName)
	}
	if !strings.Contains(stdout.String(), "gascity/workflows.codex-max-8") {
		t.Fatalf("stdout = %q, want nudge output for city-store pool instance", stdout.String())
	}
}

func TestDoSlingNudgePoolNoMembers(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	// No pool instances running.
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{
		Name:              "polecat",
		Dir:               "hw",
		MinActiveSessions: intPtr(1), MaxActiveSessions: intPtr(3),
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.CityPath = t.TempDir() // isolated path so poke doesn't hit real socket
	opts := testOpts(a, "BL-1")
	opts.Nudge = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0 (sling succeeds, poke attempted)", code)
	}
	if !strings.Contains(stderr.String(), "poke failed") {
		t.Errorf("stderr = %q, want 'poke failed' message (no controller socket in test)", stderr.String())
	}
}

// TestBuiltInSlingSlotSuffixedTargetNormalizesRoutedTo is the write-side guard
// for #2592: resolving and slinging a slot-suffixed pool target ("saitoc/polecat-2")
// must record the base pool qualified name in gc.routed_to, so the pool's
// exact-match work_query (keyed on the base template) can see the bead.
func TestBuiltInSlingSlotSuffixedTargetNormalizesRoutedTo(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	maxPolecats := 5
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "saitoc", Path: "/tmp/saitoc", Prefix: "gc"}},
		Agents: []config.Agent{
			{Name: "polecat", Dir: "saitoc", MaxActiveSessions: &maxPolecats},
		},
	}
	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	store := newSlingTestStore()
	deps.Store = store
	deps.StoreRef = "rig:saitoc"

	created, err := store.Create(beads.Bead{Title: "slot-routed work", Type: "task"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Resolve the slot-suffixed target the way the CLI does, then sling it.
	target, ok := resolveAgentIdentity(cfg, "saitoc/polecat-2", "")
	if !ok {
		t.Fatal("resolveAgentIdentity(saitoc/polecat-2) failed")
	}
	if target.QualifiedName() != "saitoc/polecat-2" {
		t.Fatalf("resolved target QualifiedName = %q, want saitoc/polecat-2", target.QualifiedName())
	}

	opts := testOpts(target, created.ID)
	code := doSling(opts, deps, &fakeQuerier{bead: created}, stdout, stderr)
	if code != 0 {
		t.Fatalf("doSling returned %d; stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	routed, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get routed bead: %v", err)
	}
	if got := routed.Metadata["gc.routed_to"]; got != "saitoc/polecat" {
		t.Fatalf("gc.routed_to = %q, want saitoc/polecat (slot suffix should be normalized away)", got)
	}
}

func TestBuiltInSlingSlotSuffixedTargetRefusesCrossStoreRoute(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	maxPolecats := 5
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "saitoc", Path: "/tmp/saitoc", Prefix: "gc"}},
		Agents: []config.Agent{
			{Name: "polecat", Dir: "saitoc", MaxActiveSessions: &maxPolecats},
		},
	}
	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	store := newSlingTestStore()
	deps.Store = store
	deps.StoreRef = "city:test-city"

	created, err := store.Create(beads.Bead{Title: "slot-routed work", Type: "task"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	target, ok := resolveAgentIdentity(cfg, "saitoc/polecat-2", "")
	if !ok {
		t.Fatal("resolveAgentIdentity(saitoc/polecat-2) failed")
	}

	opts := testOpts(target, created.ID)
	code := doSling(opts, deps, &fakeQuerier{bead: created}, stdout, stderr)
	if code == 0 {
		t.Fatalf("doSling returned 0, want cross-store refusal; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if got := stderr.String(); !strings.Contains(got, "refusing cross-store route") ||
		!strings.Contains(got, "city:test-city") ||
		!strings.Contains(got, "rig:saitoc") {
		t.Fatalf("stderr = %q, want cross-store refusal with city and rig stores", got)
	}
	routed, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get routed bead: %v", err)
	}
	if got := routed.Metadata["gc.routed_to"]; got != "" {
		t.Fatalf("gc.routed_to = %q, want unset after refusal", got)
	}
}

func TestBuiltInSlingPoolRouteContractUsesMetadataOnly(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	maxPolecats := 5
	maxRefinery := 1
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "saitoc", Path: "/tmp/saitoc", Prefix: "gc"}},
		Agents: []config.Agent{
			{Name: "polecat", Dir: "saitoc", MaxActiveSessions: &maxPolecats},
			{Name: "refinery", Dir: "saitoc", MaxActiveSessions: &maxRefinery},
		},
	}
	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	store := newSlingTestStore()
	deps.Store = store
	// The bead lives in the saitoc rig store (single physical store reused
	// below as both source and rig store for the scale_check probe), so
	// align StoreRef accordingly. Without this, the cross-store guard
	// added in tr-6s7yx refuses the HQ-store → rig-pool route.
	deps.StoreRef = "rig:saitoc"

	created, err := store.Create(beads.Bead{Title: "route contract work", Type: "task"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	opts := testOpts(cfg.Agents[0], created.ID)
	code := doSling(opts, deps, &fakeQuerier{bead: created}, stdout, stderr)
	if code != 0 {
		t.Fatalf("doSling returned %d; stderr=%s stdout=%s", code, stderr.String(), stdout.String())
	}
	routed, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get routed bead: %v", err)
	}
	if got := routed.Metadata["gc.routed_to"]; got != "saitoc/polecat" {
		t.Fatalf("gc.routed_to = %q, want saitoc/polecat", got)
	}
	for _, label := range routed.Labels {
		if strings.HasPrefix(label, "pool:") {
			t.Fatalf("built-in sling added legacy pool label %q; labels=%v", label, routed.Labels)
		}
	}

	counts, partials, errs := defaultScaleCheckCounts([]defaultScaleCheckTarget{
		defaultScaleCheckTargetForAgent(sharedTestCityDir, cfg, &cfg.Agents[0], nil, map[string]beads.Store{"saitoc": store}),
	})
	if len(errs) != 0 {
		t.Fatalf("defaultScaleCheckCounts errors: %v", errs)
	}
	if len(partials) != 0 {
		t.Fatalf("defaultScaleCheckCounts partials: %v", partials)
	}
	if got := counts["saitoc/polecat"]; got != 1 {
		t.Fatalf("polecat scale count after sling = %d, want 1", got)
	}

	inProgress := "in_progress"
	polecatSession := "pc-1"
	if err := store.Update(created.ID, beads.UpdateOpts{
		Status:   &inProgress,
		Assignee: &polecatSession,
	}); err != nil {
		t.Fatalf("claim update: %v", err)
	}
	claimed, err := store.Get(created.ID)
	if err != nil {
		t.Fatalf("Get claimed bead: %v", err)
	}
	states := ComputePoolDesiredStates(cfg, []beads.Bead{claimed}, sessionInfosFromBeads([]beads.Bead{{
		ID:     polecatSession,
		Status: "open",
		Type:   sessionBeadType,
		Metadata: map[string]string{
			"template":     "saitoc/polecat",
			"session_name": polecatSession,
		},
	}}), map[string]int{"saitoc/polecat": 0})
	if len(states) != 1 || len(states[0].Requests) != 1 {
		t.Fatalf("resume states = %#v, want one polecat resume request", states)
	}
	if req := states[0].Requests[0]; req.Tier != "resume" || req.WorkBeadID != created.ID || req.SessionBeadID != polecatSession {
		t.Fatalf("resume request = %#v, want claimed work preserved for polecat session", req)
	}

	open := "open"
	refinery := "saitoc/refinery"
	if err := store.Update(created.ID, beads.UpdateOpts{
		Status:   &open,
		Assignee: &refinery,
		Labels:   []string{"pool:saitoc/polecat"},
		Metadata: map[string]string{"gc.routed_to": refinery},
	}); err != nil {
		t.Fatalf("handoff update: %v", err)
	}
	counts, partials, errs = defaultScaleCheckCounts([]defaultScaleCheckTarget{
		defaultScaleCheckTargetForAgent(sharedTestCityDir, cfg, &cfg.Agents[0], nil, map[string]beads.Store{"saitoc": store}),
		defaultScaleCheckTargetForAgent(sharedTestCityDir, cfg, &cfg.Agents[1], nil, map[string]beads.Store{"saitoc": store}),
	})
	if len(errs) != 0 {
		t.Fatalf("post-handoff defaultScaleCheckCounts errors: %v", errs)
	}
	if len(partials) != 0 {
		t.Fatalf("post-handoff defaultScaleCheckCounts partials: %v", partials)
	}
	if got := counts["saitoc/polecat"]; got != 0 {
		t.Fatalf("polecat scale count after refinery handoff with stale pool label = %d, want 0", got)
	}
	// A pool-template assignee is not concrete ownership. Generic scale demand
	// stays strictly unassigned+routed; callers that want pool demand must clear
	// Assignee and set gc.routed_to.
	if got := counts["saitoc/refinery"]; got != 0 {
		t.Fatalf("refinery generic scale count for assigned handoff = %d, want 0", got)
	}
}

func TestCmdSlingUsesRigScopedStoreForBuiltInRouting(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_BEADS", "file")

	cityDir := t.TempDir()
	rigDir := filepath.Join(cityDir, "frontend")
	if err := os.MkdirAll(rigDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(rig): %v", err)
	}
	if err := ensureScopedFileStoreLayout(cityDir); err != nil {
		t.Fatalf("ensureScopedFileStoreLayout: %v", err)
	}
	if err := ensurePersistedScopeLocalFileStore(cityDir); err != nil {
		t.Fatalf("ensurePersistedScopeLocalFileStore(city): %v", err)
	}
	if err := ensurePersistedScopeLocalFileStore(rigDir); err != nil {
		t.Fatalf("ensurePersistedScopeLocalFileStore(rig): %v", err)
	}
	cityToml := `[workspace]
name = "demo"

[[rigs]]
name = "frontend"
path = "frontend"
prefix = "FE"

[[agent]]
name = "worker"
dir = "frontend"
`
	writeSlingTestCity(t, cityDir, cityToml)
	installSlingMemStores(t, cityDir, map[string][]beads.Bead{rigDir: nil})
	t.Chdir(cityDir)
	t.Setenv("GC_CITY_PATH", cityDir)

	var stdout, stderr bytes.Buffer
	code := cmdSling([]string{"frontend/worker", "ship feature"}, false, false, true, "", nil, "", true, false, false, "", false, false, false, "", "", &stdout, &stderr)
	if code != 0 {
		t.Fatalf("cmdSling returned %d, want 0; stderr: %s", code, stderr.String())
	}

	rigStore, err := slingOpenStoreAtForCity(rigDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(rig): %v", err)
	}
	rigBeads, err := rigStore.List(beads.ListQuery{AllowScan: true, Sort: beads.SortCreatedAsc})
	if err != nil {
		t.Fatalf("rigStore.List: %v", err)
	}
	if len(rigBeads) != 1 {
		t.Fatalf("rig store bead count = %d, want 1: %#v", len(rigBeads), rigBeads)
	}
	if rigBeads[0].Title != "ship feature" {
		t.Fatalf("rig bead title = %q, want %q", rigBeads[0].Title, "ship feature")
	}
	if rigBeads[0].Metadata["gc.routed_to"] != "frontend/worker" {
		t.Fatalf("rig bead gc.routed_to = %q, want %q", rigBeads[0].Metadata["gc.routed_to"], "frontend/worker")
	}

	cityStore, err := slingOpenStoreAtForCity(cityDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(city): %v", err)
	}
	cityBeads, err := cityStore.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatalf("cityStore.List: %v", err)
	}
	if len(cityBeads) != 0 {
		t.Fatalf("city store bead count = %d, want 0: %#v", len(cityBeads), cityBeads)
	}
}

func TestCmdSlingFileStoreRefusesNativeRouteWithoutEffects(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_BEADS", "file")
	cityDir := t.TempDir()
	rigDir := filepath.Join(cityDir, "frontend")
	if err := ensureScopedFileStoreLayout(cityDir); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{cityDir, rigDir} {
		if err := ensurePersistedScopeLocalFileStore(dir); err != nil {
			t.Fatal(err)
		}
	}
	writeSlingTestCity(t, cityDir, "[workspace]\nname = \"demo\"\n\n[[rigs]]\nname = \"frontend\"\npath = \"frontend\"\nprefix = \"FE\"\n\n[[agent]]\nname = \"worker\"\ndir = \"frontend\"\n")
	writeTestFileStoreBeads(t, rigDir, []beads.Bead{{
		ID: "FE-42", Title: "original goal", Description: "original scope", AcceptanceCriteria: "original outcome",
		Type: "task", Status: "open", Labels: []string{"approved"}, Metadata: map[string]string{"handoff.conflict_state": ""},
	}})
	store, err := slingOpenStoreAtForCity(rigDir, cityDir)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, TierMode: beads.TierBoth})
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(cityDir)
	t.Setenv("GC_CITY_PATH", cityDir)
	var stdout, stderr bytes.Buffer
	code := cmdSling([]string{"frontend/worker", "FE-42"}, false, false, true, "", nil, "", false, false, false, "", true, false, false, "", "", &stdout, &stderr)
	if code != 1 {
		t.Fatalf("unsupported native route exit=%d, want1; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	after, err := store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, TierMode: beads.TierBoth})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("unsupported backend changed original goal, route or receipt: before=%+v after=%+v", before, after)
	}
	assertStoreHasNoBeadTitle(t, cityDir, cityDir, "FE-42")
}

func TestCmdSlingDefaultFormulaDoesNotMaterializePoolSession(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_BEADS", "file")

	cityDir := t.TempDir()
	if err := ensureScopedFileStoreLayout(cityDir); err != nil {
		t.Fatalf("ensureScopedFileStoreLayout: %v", err)
	}
	if err := ensurePersistedScopeLocalFileStore(cityDir); err != nil {
		t.Fatalf("ensurePersistedScopeLocalFileStore(city): %v", err)
	}
	cityToml := builtinImportsTOML("core") + `[workspace]
name = "demo"

[beads]
provider = "file"

[[agent]]
name = "worker"
start_command = "true"
default_sling_formula = "mol-do-work"
min_active_sessions = 0
max_active_sessions = 1

[[named_session]]
template = "worker"
mode = "on_demand"
`
	writeSlingTestCity(t, cityDir, cityToml)
	writeBuiltinImportsLock(t, cityDir, "core")
	installSlingMemStores(t, cityDir, nil)
	t.Chdir(cityDir)
	t.Setenv("GC_CITY_PATH", cityDir)

	var stdout, stderr bytes.Buffer
	code := cmdSling([]string{"worker", "ship feature"}, false, false, true, "", nil, "", true, false, false, "", false, false, false, "", "", &stdout, &stderr)
	if code != 0 {
		t.Fatalf("cmdSling returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}

	store, err := slingOpenStoreAtForCity(cityDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(city): %v", err)
	}
	sessions, err := store.ListByLabel(sessionBeadLabel, 0)
	if err != nil {
		t.Fatalf("ListByLabel(%q): %v", sessionBeadLabel, err)
	}
	if len(sessions) != 0 {
		t.Fatalf("session bead count = %d, want 0 after sling; sessions=%#v", len(sessions), sessions)
	}

	// mol-do-work is a graph.v2 formula (#2941): the default-formula sling
	// creates a one-item input convoy plus a workflow root instead of
	// routing the bare work bead.
	all, err := store.List(beads.ListQuery{AllowScan: true, TierMode: beads.TierBoth})
	if err != nil {
		t.Fatalf("store.List: %v", err)
	}
	var root *beads.Bead
	for i := range all {
		if all[i].Metadata["gc.kind"] == "workflow" {
			root = &all[i]
			break
		}
	}
	if root == nil {
		t.Fatalf("no graph.v2 workflow root created; beads=%#v", all)
	}
	inputConvoyID := root.Metadata["gc.input_convoy_id"]
	if inputConvoyID == "" {
		t.Fatalf("workflow root %s missing gc.input_convoy_id: %v", root.ID, root.Metadata)
	}
	members, err := convoycore.Members(store, inputConvoyID, true)
	if err != nil {
		t.Fatalf("convoycore.Members(%s): %v", inputConvoyID, err)
	}
	if len(members) != 1 || members[0].Title != "ship feature" {
		t.Fatalf("input convoy members = %#v, want the slung work bead", members)
	}
	foundRoutedWorkflowBead := false
	for _, bead := range all {
		if bead.ID != root.ID && bead.Metadata["gc.root_bead_id"] != root.ID {
			continue
		}
		if bead.Assignee == "worker" {
			t.Fatalf("workflow bead %s Assignee = %q, want template only in gc.routed_to; metadata=%v", bead.ID, bead.Assignee, bead.Metadata)
		}
		if bead.Metadata["gc.routed_to"] == "worker" {
			foundRoutedWorkflowBead = true
			if bead.Assignee != "" {
				t.Fatalf("workflow bead %s Assignee = %q with gc.routed_to=worker, want unassigned pool work", bead.ID, bead.Assignee)
			}
		}
	}
	if !foundRoutedWorkflowBead {
		t.Fatalf("workflow rooted at %s had no gc.routed_to=worker bead; all=%#v", root.ID, all)
	}

	// Demand surfaces through routed pool metadata: the graph workflow makes
	// the reconciler desire a worker session even though sling itself
	// materialized nothing.
	cfg, _, err := config.LoadWithIncludes(fsys.OSFS{}, filepath.Join(cityDir, "city.toml"))
	if err != nil {
		t.Fatalf("LoadWithIncludes: %v", err)
	}
	var dsErr bytes.Buffer
	ds := buildDesiredState("demo", cityDir, time.Now().UTC(), cfg, runtime.NewFake(), store, &dsErr)
	desiredWorkers := 0
	for name := range ds.State {
		if strings.HasPrefix(name, "worker-") {
			desiredWorkers++
		}
	}
	if desiredWorkers != 1 {
		t.Fatalf("desired worker sessions = %d, want 1 from routed graph step demand; state=%v stderr=%s", desiredWorkers, ds.State, dsErr.String())
	}
}

// setupCmdSlingBeadExistsFixture writes a minimal city.toml with a single
// rig + worker agent and positions the test CWD inside the city. Used by
// the bead-existence tests below. Returns the city directory.
func setupCmdSlingBeadExistsFixture(t *testing.T) string {
	t.Helper()
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_BEADS", "file")

	cityDir := t.TempDir()
	rigDir := filepath.Join(cityDir, "frontend")
	if err := os.MkdirAll(rigDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(rig): %v", err)
	}
	if err := ensureScopedFileStoreLayout(cityDir); err != nil {
		t.Fatalf("ensureScopedFileStoreLayout: %v", err)
	}
	if err := ensurePersistedScopeLocalFileStore(cityDir); err != nil {
		t.Fatalf("ensurePersistedScopeLocalFileStore(city): %v", err)
	}
	if err := ensurePersistedScopeLocalFileStore(rigDir); err != nil {
		t.Fatalf("ensurePersistedScopeLocalFileStore(rig): %v", err)
	}
	cityToml := `[workspace]
name = "demo"

[[rigs]]
name = "frontend"
path = "frontend"
prefix = "FE"

[[agent]]
name = "worker"
dir = "frontend"
`
	writeSlingTestCity(t, cityDir, cityToml)
	installSlingMemStores(t, cityDir, map[string][]beads.Bead{rigDir: nil})
	t.Chdir(cityDir)
	t.Setenv("GC_CITY_PATH", cityDir)
	return cityDir
}

func TestCmdSlingInlineBeadBareTargetFromRigCwdUsesRigStore(t *testing.T) {
	cityDir := setupCmdSlingBeadExistsFixture(t)
	rigDir := filepath.Join(cityDir, "frontend")
	t.Chdir(rigDir)

	var stdout, stderr bytes.Buffer
	code := cmdSling([]string{"worker", "ship feature"}, false, false, true, "", nil, "", true, false, false, "", false, false, false, "", "", &stdout, &stderr)
	if code != 0 {
		t.Fatalf("cmdSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	store, err := slingOpenStoreAtForCity(rigDir, cityDir)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Title != "ship feature" || rows[0].Metadata["gc.routed_to"] != "frontend/worker" {
		t.Fatalf("rig work = %+v, want exact inline goal routed to frontend/worker", rows)
	}
	assertStoreHasNoBeadTitle(t, cityDir, cityDir, "ship feature")
}

func TestCmdSlingRefusesMissingBead(t *testing.T) {
	// A bead-ID-shaped argument that doesn't resolve in the store must
	// cause sling to error out — otherwise a fabricated / typo'd ID
	// would flow through and strand workers on a dead reference.
	cityDir := setupCmdSlingBeadExistsFixture(t)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"frontend/worker", "FE-ghost1"},
		false, false, false, // isFormula, doNudge, force=false
		"", nil, "",
		true, false, false, "",
		false, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code == 0 {
		t.Fatalf("cmdSling returned 0, want non-zero; stderr: %s", stderr.String())
	}
	store, err := slingOpenStoreAtForCity(filepath.Join(cityDir, "frontend"), cityDir)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, TierMode: beads.TierBoth})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("missing source created work or receipt: %+v", rows)
	}
}

func TestCmdSlingDryRunRefusesMissingBead(t *testing.T) {
	setupCmdSlingBeadExistsFixture(t)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"frontend/worker", "FE-ghost1"},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		false, false, true,
		"", "",
		&stdout, &stderr,
	)
	if code == 0 {
		t.Fatalf("cmdSling dry-run returned 0, want non-zero; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	got := stderr.String()
	if !strings.Contains(got, "FE-ghost1") {
		t.Errorf("stderr missing bead ID; got: %s", got)
	}
	if !strings.Contains(got, "not found") {
		t.Errorf("stderr missing missing-bead phrasing; got: %s", got)
	}
}

func TestCmdSlingDryRunPreviewsInlineText(t *testing.T) {
	cityDir := setupCmdSlingBeadExistsFixture(t)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"frontend/worker", "write docs"},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		false, false, true,
		"", "",
		&stdout, &stderr,
	)
	if code != 0 {
		t.Fatalf("cmdSling dry-run returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stderr.String(), "not found") {
		t.Fatalf("stderr = %s, want no missing-bead diagnostic", stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "write docs") {
		t.Fatalf("stdout = %s, want inline text in dry-run preview", out)
	}
	if strings.Contains(out, "Created ") {
		t.Fatalf("stdout = %s, want no bead creation during dry-run", out)
	}
	if !strings.Contains(out, "No side effects executed (--dry-run).") {
		t.Fatalf("stdout = %s, want dry-run footer", out)
	}

	rigStore, err := slingOpenStoreAtForCity(filepath.Join(cityDir, "frontend"), cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(rig): %v", err)
	}
	beadList, err := rigStore.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatalf("rigStore.List: %v", err)
	}
	if len(beadList) != 0 {
		t.Fatalf("rig store bead count = %d, want 0: %#v", len(beadList), beadList)
	}
}

// TestCmdSlingDryRunInlineTextHasNoFalsePositivePreCheck verifies that
// inline-text dry-runs print a "Would create new task bead" hint and
// suppress the Pre-check ✓ line (which would be vacuously true for a
// bead that does not exist yet).
func TestCmdSlingDryRunInlineTextHasNoFalsePositivePreCheck(t *testing.T) {
	cityDir := setupCmdSlingBeadExistsFixture(t)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"frontend/worker", "write docs"},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		false, false, true,
		"", "",
		&stdout, &stderr,
	)
	if code != 0 {
		t.Fatalf("cmdSling dry-run returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	out := stdout.String()
	if strings.Contains(out, "has no existing molecule/wisp children ✓") {
		t.Fatalf("dry-run stdout still emits false-positive Pre-check ✓ for inline text:\n%s", out)
	}
	if !strings.Contains(out, "Would create new task bead") {
		t.Fatalf("dry-run stdout missing inline-text creation hint:\n%s", out)
	}
	// Cook/route preview commands must use a placeholder rather than
	// the inline title: the live path creates a bead first and uses
	// the new ID, so showing "write docs" as the bead-id arg would
	// describe a command that wouldn't actually run.
	if strings.Contains(out, "--on=write docs") || strings.Contains(out, "--on='write docs'") {
		t.Fatalf("dry-run stdout uses inline title as bead ID in --on=...:\n%s", out)
	}
	if !strings.Contains(out, "<new-bead-id>") {
		t.Fatalf("dry-run stdout missing <new-bead-id> placeholder:\n%s", out)
	}
	// Pre-existing footer must still be present.
	if !strings.Contains(out, "No side effects executed (--dry-run).") {
		t.Fatalf("dry-run stdout missing dry-run footer:\n%s", out)
	}

	// Sanity: city/frontend stores must remain empty (no bead created).
	for _, dir := range []string{cityDir, filepath.Join(cityDir, "frontend")} {
		store, err := slingOpenStoreAtForCity(dir, cityDir)
		if err != nil {
			t.Fatalf("openStoreAtForCity(%s): %v", dir, err)
		}
		bs, err := store.List(beads.ListQuery{AllowScan: true})
		if err != nil {
			t.Fatalf("List(%s): %v", dir, err)
		}
		if len(bs) != 0 {
			t.Fatalf("store %s has %d beads after dry-run, want 0: %#v", dir, len(bs), bs)
		}
	}
}

func mustResolveInlineBeadAction(t *testing.T, cfg *config.City, beadOrFormula string, dryRun bool, store beads.Store) (bool, bool) {
	t.Helper()
	create, inlineText, err := resolveInlineBeadAction(cfg, beadOrFormula, dryRun, store)
	if err != nil {
		t.Fatalf("resolveInlineBeadAction: %v", err)
	}
	return create, inlineText
}

func TestResolveInlineBeadActionDryRunInlineTextDoesNotProbeStore(t *testing.T) {
	create, inlineText := mustResolveInlineBeadAction(t, &config.City{}, "write docs", true, nil)
	if create {
		t.Fatal("create = true, want false during dry-run")
	}
	if !inlineText {
		t.Fatal("inlineText = false, want true")
	}
}

func TestResolveInlineBeadActionWhitespaceInlineTextDoesNotProbeStore(t *testing.T) {
	create, inlineText := mustResolveInlineBeadAction(t, &config.City{}, "write docs", false, nil)
	if !create {
		t.Fatal("create = false, want true for whitespace inline text")
	}
	if inlineText {
		t.Fatal("inlineText = true, want false outside dry-run")
	}
}

func TestResolveInlineBeadActionSingleTokenInlineTextDoesNotProbeStore(t *testing.T) {
	create, inlineText := mustResolveInlineBeadAction(t, &config.City{}, "docs", false, nil)
	if !create {
		t.Fatal("create = false, want true for single-token inline text")
	}
	if inlineText {
		t.Fatal("inlineText = true, want false outside dry-run")
	}
}

func TestResolveInlineBeadActionBeadIDDoesNotProbeStore(t *testing.T) {
	create, inlineText := mustResolveInlineBeadAction(t, &config.City{}, "FE-123", false, nil)
	if create {
		t.Fatal("create = true, want false for bead ID")
	}
	if inlineText {
		t.Fatal("inlineText = true, want false")
	}
}

func TestResolveInlineBeadActionHyphenatedRigPrefixIsBeadID(t *testing.T) {
	// Bead IDs whose configured rig prefix contains a hyphen
	// (agent-diagnostics-hnn from rig "agent-diagnostics") must
	// classify as bead IDs, not inline text.
	cfg := &config.City{
		Rigs: []config.Rig{
			{Name: "agent-diagnostics", Path: "/tmp/agent-diag", Prefix: "agent-diagnostics"},
		},
	}

	create, inlineText := mustResolveInlineBeadAction(t, cfg, "agent-diagnostics-hnn", false, nil)
	if create {
		t.Fatal("create = true, want false for configured hyphenated bead ID")
	}
	if inlineText {
		t.Fatal("inlineText = true, want false outside dry-run")
	}

	create, inlineText = mustResolveInlineBeadAction(t, cfg, "agent-diagnostics-hnn", true, nil)
	if create {
		t.Fatal("create = true, want false during dry-run")
	}
	if inlineText {
		t.Fatal("inlineText = true, want false for configured bead ID even in dry-run")
	}
}

func TestResolveInlineBeadActionUnknownHyphenatedTextStillCreates(t *testing.T) {
	// Inline text shaped like "<unknown-prefix>-<word>" with no store must
	// still create an inline task bead. Only inputs that match a CONFIGURED
	// rig prefix are protected from the auto-create branch (without a store).
	cfg := &config.City{
		Rigs: []config.Rig{{Name: "fe", Path: "/fe", Prefix: "fe"}},
	}
	create, inlineText := mustResolveInlineBeadAction(t, cfg, "code-review-please", false, nil)
	if !create {
		t.Fatal("create = false, want true for non-configured hyphenated text")
	}
	if inlineText {
		t.Fatal("inlineText = true, want false outside dry-run")
	}
}

func TestResolveInlineBeadActionConfiguredAlphaSuffixIsBeadID(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test", Prefix: "HQ"},
		Rigs:      []config.Rig{{Name: "frontend", Path: "/tmp/frontend", Prefix: "FE"}},
	}

	create, inlineText := mustResolveInlineBeadAction(t, cfg, "FE-hello", false, nil)
	if create {
		t.Fatal("create = true, want false for configured bead ID with all-alpha suffix")
	}
	if inlineText {
		t.Fatal("inlineText = true, want false outside dry-run")
	}

	create, inlineText = mustResolveInlineBeadAction(t, cfg, "FE-a1pha", false, nil)
	if create {
		t.Fatal("create = true, want false for configured bead ID with digit")
	}
	if inlineText {
		t.Fatal("inlineText = true, want false for configured bead ID")
	}
}

func TestResolveInlineBeadActionMultiDashStoreHitIsBeadID(t *testing.T) {
	// A multi-dash ID that fails the suffix heuristic but exists in the store
	// must classify as a bead ID, not inline text.
	cfg := &config.City{
		Rigs: []config.Rig{{Name: "fo", Path: "/tmp/fo", Prefix: "fo"}},
	}
	store := seededStore("fo-spawn-storm")

	create, inlineText := mustResolveInlineBeadAction(t, cfg, "fo-spawn-storm", false, store)
	if create {
		t.Fatal("create = true, want false — bead exists in store")
	}
	if inlineText {
		t.Fatal("inlineText = true, want false outside dry-run")
	}

	create, inlineText = mustResolveInlineBeadAction(t, cfg, "fo-spawn-storm", true, store)
	if create {
		t.Fatal("create = true, want false during dry-run")
	}
	if inlineText {
		t.Fatal("inlineText = true, want false — bead exists in store")
	}
}

func TestResolveInlineBeadActionMultiDashStoreMissStillCreates(t *testing.T) {
	// A multi-dash ID absent from the store falls through to inline-text
	// creation — the caller will auto-create a bead from the text.
	cfg := &config.City{
		Rigs: []config.Rig{{Name: "fo", Path: "/tmp/fo", Prefix: "fo"}},
	}
	store := seededStore() // empty

	create, inlineText := mustResolveInlineBeadAction(t, cfg, "fo-typo-not-real", false, store)
	if !create {
		t.Fatal("create = false, want true for unknown multi-dash text")
	}
	if inlineText {
		t.Fatal("inlineText = true, want false outside dry-run")
	}
}

func TestResolveInlineBeadActionMultiDashStoreErrorSurfaces(t *testing.T) {
	cfg := &config.City{
		Rigs: []config.Rig{{Name: "fo", Path: "/tmp/fo", Prefix: "fo"}},
	}
	store := &getErrStore{Store: beads.NewMemStore(), err: fmt.Errorf("lookup failed")}

	_, _, err := resolveInlineBeadAction(cfg, "fo-spawn-storm", false, store)
	if err == nil {
		t.Fatal("resolveInlineBeadAction error = nil, want lookup failure")
	}
	if !strings.Contains(err.Error(), "lookup failed") {
		t.Fatalf("resolveInlineBeadAction error = %q, want lookup failure", err)
	}
}

func TestResolveInlineBeadActionMultilineTextErrors(t *testing.T) {
	// Reproduces ga-thcr5n: a newline-joined list of bead IDs passed as a
	// single sling argument (e.g. by the always-on deacon script) must not
	// be silently fabricated into one contentless junk bead. Fail loud.
	blob := strings.Join([]string{
		"ga-kn8yy6.1", "ga-zogqc1.2.5", "ga-zogqc1.2.4", "ga-zogqc1.2.3",
		"ga-a700wu", "ga-rzdqvl", "ga-7sy2ac", "ga-cd1895", "ga-fxudqs",
		"ga-9feee3", "ga-e3zcqv", "ga-gzmzej", "ga-uslskt",
	}, "\n")

	create, previewInlineText, err := resolveInlineBeadAction(&config.City{}, blob, false, nil)
	if err == nil {
		t.Fatal("resolveInlineBeadAction error = nil, want error for multi-line inline text")
	}
	if !strings.Contains(err.Error(), "13") {
		t.Fatalf("resolveInlineBeadAction error = %q, want line count 13", err)
	}
	if create {
		t.Fatal("create = true, want false for multi-line inline text")
	}
	if previewInlineText {
		t.Fatal("previewInlineText = true, want false for multi-line inline text")
	}
}

func TestResolveInlineBeadActionMultilineTextErrorsDuringDryRun(t *testing.T) {
	// The dry-run path must fail the same way — a dry-run preview of a junk
	// bead is still a lie about what gc sling would do.
	blob := "ga-abc12\nga-def34\nga-ghi56"

	create, previewInlineText, err := resolveInlineBeadAction(&config.City{}, blob, true, nil)
	if err == nil {
		t.Fatal("resolveInlineBeadAction error = nil, want error for multi-line inline text during dry-run")
	}
	if create {
		t.Fatal("create = true, want false for multi-line inline text during dry-run")
	}
	if previewInlineText {
		t.Fatal("previewInlineText = true, want false — must not preview a junk bead")
	}
}

func TestResolveInlineBeadActionSingleLineWithSpacesStillCreates(t *testing.T) {
	// Regression/over-correction guard: the newline guard must key ONLY on
	// "\n", not general whitespace. Ordinary space-separated inline text
	// must still create a bead.
	create, inlineText := mustResolveInlineBeadAction(t, &config.City{}, "write a README", false, nil)
	if !create {
		t.Fatal("create = false, want true for single-line inline text with spaces")
	}
	if inlineText {
		t.Fatal("inlineText = true, want false outside dry-run")
	}
}

func TestResolveInlineBeadActionSingleBeadIDNoNewlineDoesNotError(t *testing.T) {
	// Regression guard: a single bead ID (the common case) must not trip the
	// newline guard or any other new logic.
	create, inlineText := mustResolveInlineBeadAction(t, &config.City{}, "ga-abc12", false, nil)
	if create {
		t.Fatal("create = true, want false for a single bead ID")
	}
	if inlineText {
		t.Fatal("inlineText = true, want false for a single bead ID")
	}
}

// TestCmdSlingConfiguredPrefixAllAlphaCrossRigRouteRefused verifies that
// `gc sling` refuses a route whose source bead and target agent live in
// different stores. Cross-store routes silently wedge pools because the
// supervisor's scale_check is single-store (see tr-6s7yx).
//
// Previously this scenario was permitted with the assumption that
// "routing = metadata only, keep the bead in its source store." That
// assumption created the wedge; the guard now refuses the route up
// front.
func TestCmdSlingConfiguredPrefixAllAlphaCrossRigRouteRefused(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_BEADS", "file")

	cityDir := t.TempDir()
	t.Setenv("GC_CITY", cityDir)
	t.Setenv("GC_CITY_PATH", "")
	t.Setenv("GC_CITY_ROOT", "")
	t.Setenv("GC_RIG", "")
	t.Setenv("GC_RIG_ROOT", "")
	frontendDir := filepath.Join(cityDir, "frontend")
	ordersDir := filepath.Join(cityDir, "orders")
	for _, dir := range []string{frontendDir, ordersDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", dir, err)
		}
	}
	if err := ensureScopedFileStoreLayout(cityDir); err != nil {
		t.Fatalf("ensureScopedFileStoreLayout: %v", err)
	}
	for _, dir := range []string{cityDir, frontendDir, ordersDir} {
		if err := ensurePersistedScopeLocalFileStore(dir); err != nil {
			t.Fatalf("ensurePersistedScopeLocalFileStore(%s): %v", dir, err)
		}
	}
	writeTestFileStoreBeads(t, frontendDir, []beads.Bead{{
		ID:       "FE-abcde",
		Title:    "existing frontend work",
		Type:     "task",
		Status:   "open",
		Metadata: map[string]string{},
	}})
	cityToml := `[workspace]
name = "demo"

[[rigs]]
name = "frontend"
path = "frontend"
prefix = "FE"

[[rigs]]
name = "orders"
path = "orders"
prefix = "OD"

[[agent]]
name = "worker"
dir = "orders"
`
	writeSlingTestCity(t, cityDir, cityToml)
	t.Chdir(cityDir)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"orders/worker", "FE-abcde"},
		false, false, true,
		"", nil, "",
		true, false, false, "",
		true, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code == 0 {
		t.Fatalf("cmdSling returned 0, want non-zero refusal; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	for _, want := range []string{"refusing cross-store route", "tr-6s7yx"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr missing %q: %s", want, stderr.String())
		}
	}

	frontendStore, err := slingOpenStoreAtForCity(frontendDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(frontend): %v", err)
	}
	routed, err := frontendStore.Get("FE-abcde")
	if err != nil {
		t.Fatalf("frontendStore.Get(FE-abcde): %v", err)
	}
	if got := routed.Metadata["gc.routed_to"]; got != "" {
		t.Fatalf("refused route mutated metadata: gc.routed_to = %q, want empty", got)
	}

	ordersStore, err := slingOpenStoreAtForCity(ordersDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(orders): %v", err)
	}
	ordersBeads, err := ordersStore.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatalf("ordersStore.List: %v", err)
	}
	if len(ordersBeads) != 0 {
		t.Fatalf("orders store bead count = %d, want 0: %#v", len(ordersBeads), ordersBeads)
	}
}

// TestCmdSlingHyphenatedRigPrefixExistingBeadDoesNotOrphan verifies
// that an existing bead in a rig whose configured prefix contains a
// hyphen ("agent-diagnostics-hnn" in rig "agent-diagnostics") routes
// to the rig store without auto-creating a city orphan.
func TestCmdSlingHyphenatedRigPrefixExistingBeadDoesNotOrphan(t *testing.T) {
	beadID := "agent-diagnostics-hnn"
	cityDir, rigDir, _ := setupCmdSlingHyphenatedRigPrefixBeadFixture(t, beadID, "agent-diagnostics")

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"agent-diagnostics/worker", beadID},
		false, false, true,
		"", nil, "",
		true, false, false, "",
		true, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code != 0 {
		t.Fatalf("cmdSling returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	// The pre-fix bug printed a "Created gc-NNN — \"agent-diagnostics-hnn\""
	// line because the live path took the auto-create-text-bead branch.
	if strings.Contains(stdout.String(), "Created ") {
		t.Fatalf("orphan auto-create regression: stdout = %q", stdout.String())
	}

	assertHyphenatedRigBeadRoutedWithoutInlineOrphan(t, cityDir, rigDir, beadID, "agent-diagnostics/worker")
}

func TestCmdSlingHyphenatedRigPrefixMultiDashExistingBeadDoesNotOrphan(t *testing.T) {
	beadID := "agent-diagnostics-spawn-storm"
	cityDir, rigDir, _ := setupCmdSlingHyphenatedRigPrefixBeadFixture(t, beadID, "agent-diagnostics")

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"agent-diagnostics/worker", beadID},
		false, false, true,
		"", nil, "",
		true, false, false, "",
		true, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code != 0 {
		t.Fatalf("cmdSling returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "Created ") {
		t.Fatalf("orphan auto-create regression: stdout = %q", stdout.String())
	}

	assertHyphenatedRigBeadRoutedWithoutInlineOrphan(t, cityDir, rigDir, beadID, "agent-diagnostics/worker")
}

func TestCmdSlingOneArgHyphenatedPrefixMultiDashExistingBeadUsesDefaultTarget(t *testing.T) {
	beadID := "agent-diagnostics-spawn-storm"
	cityDir, rigDir, _ := setupCmdSlingHyphenatedRigPrefixBeadFixture(t, beadID, "agent-diagnostics")

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{beadID},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		false, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code != 0 {
		t.Fatalf("cmdSling returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "Created ") {
		t.Fatalf("orphan auto-create regression: stdout = %q", stdout.String())
	}

	assertHyphenatedRigBeadRoutedWithoutInlineOrphan(t, cityDir, rigDir, beadID, "agent-diagnostics/worker")
}

// TestCmdSlingCrossRigHyphenatedPrefixMultiDashRouteRefused verifies the
// cross-store guard fires for a hyphenated-prefix rig bead routed to an
// agent in a different rig (see tr-6s7yx). Refusal is up-front: no
// metadata mutation, no orphan creation in the target store.
func TestCmdSlingCrossRigHyphenatedPrefixMultiDashRouteRefused(t *testing.T) {
	beadID := "agent-diagnostics-spawn-storm"
	cityDir, rigDir, otherDir := setupCmdSlingHyphenatedRigPrefixBeadFixture(t, beadID, "other")

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"other/worker", beadID},
		false, false, true,
		"", nil, "",
		true, false, false, "",
		true, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code == 0 {
		t.Fatalf("cmdSling returned 0, want non-zero refusal; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	for _, want := range []string{"refusing cross-store route", "tr-6s7yx"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr missing %q: %s", want, stderr.String())
		}
	}

	assertHyphenatedRigBeadNotMutatedAndNoOrphan(t, cityDir, rigDir, beadID)
	assertStoreHasNoBeadTitle(t, cityDir, otherDir, beadID)
}

// assertHyphenatedRigBeadNotMutatedAndNoOrphan verifies a refused cross-rig
// route left the bead's metadata untouched in its source rig store and
// did not create an orphan in the city store. Mirrors
// assertHyphenatedRigBeadRoutedWithoutInlineOrphan but for the refusal
// path.
func assertHyphenatedRigBeadNotMutatedAndNoOrphan(t *testing.T, cityDir, rigDir, beadID string) {
	t.Helper()

	rigStore, err := slingOpenStoreAtForCity(rigDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(rig): %v", err)
	}
	routed, err := rigStore.Get(beadID)
	if err != nil {
		t.Fatalf("rigStore.Get(%s): %v", beadID, err)
	}
	if got := routed.Metadata["gc.routed_to"]; got != "" {
		t.Fatalf("refused route mutated rig bead: gc.routed_to = %q, want empty", got)
	}

	assertStoreHasNoBeadTitle(t, cityDir, cityDir, beadID)
}

func setupCmdSlingHyphenatedRigPrefixBeadFixture(t *testing.T, beadID, agentDir string) (cityDir, rigDir, otherDir string) {
	t.Helper()
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_BEADS", "file")

	cityDir = t.TempDir()
	t.Setenv("GC_CITY", cityDir)
	t.Setenv("GC_CITY_PATH", "")
	t.Setenv("GC_CITY_ROOT", "")
	t.Setenv("GC_RIG", "")
	t.Setenv("GC_RIG_ROOT", "")
	rigDir = filepath.Join(cityDir, "agent-diagnostics")
	otherDir = filepath.Join(cityDir, "other")
	for _, dir := range []string{rigDir, otherDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", dir, err)
		}
	}
	if err := ensureScopedFileStoreLayout(cityDir); err != nil {
		t.Fatalf("ensureScopedFileStoreLayout: %v", err)
	}
	for _, dir := range []string{cityDir, rigDir, otherDir} {
		if err := ensurePersistedScopeLocalFileStore(dir); err != nil {
			t.Fatalf("ensurePersistedScopeLocalFileStore(%s): %v", dir, err)
		}
	}
	installSlingMemStores(t, cityDir, map[string][]beads.Bead{
		rigDir:   {{ID: beadID, Title: "existing diagnostics work", Type: "task", Status: "open"}},
		otherDir: nil,
	})
	cityToml := fmt.Sprintf(`[workspace]
name = "demo"

[[rigs]]
name = "agent-diagnostics"
path = "agent-diagnostics"
prefix = "agent-diagnostics"
default_sling_target = "agent-diagnostics/worker"

[[rigs]]
name = "other"
path = "other"
prefix = "OT"

[[agent]]
name = "worker"
dir = %q
`, agentDir)
	writeSlingTestCity(t, cityDir, cityToml)
	t.Chdir(cityDir)
	return cityDir, rigDir, otherDir
}

func assertHyphenatedRigBeadRoutedWithoutInlineOrphan(t *testing.T, cityDir, rigDir, beadID, wantTarget string) {
	t.Helper()

	rigStore, err := slingOpenStoreAtForCity(rigDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(rig): %v", err)
	}
	routed, err := rigStore.Get(beadID)
	if err != nil {
		t.Fatalf("rigStore.Get(%s): %v", beadID, err)
	}
	if routed.Metadata["gc.routed_to"] != wantTarget {
		t.Fatalf("rig bead gc.routed_to = %q, want %s (routing must land on the existing bead, not an orphan)", routed.Metadata["gc.routed_to"], wantTarget)
	}

	// City store must NOT contain a stray bead from the auto-create path.
	assertStoreHasNoBeadTitle(t, cityDir, cityDir, beadID)
}

func assertStoreHasNoBeadTitle(t *testing.T, cityDir, storeDir, beadTitle string) {
	t.Helper()
	store, err := slingOpenStoreAtForCity(storeDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(%s): %v", storeDir, err)
	}
	storeBeads, err := store.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatalf("store.List(%s): %v", storeDir, err)
	}
	for _, b := range storeBeads {
		if b.Title == beadTitle {
			t.Fatalf("store %s has orphan bead %q (title %q): inline-text auto-create fired for a known-rig bead ID", storeDir, b.ID, b.Title)
		}
	}
}

func TestCmdSlingConfiguredPrefixAllAlphaExistingBeadUsesSelectedPrefixStore(t *testing.T) {
	cityDir, frontendDir := setupCmdSlingConfiguredPrefixAllAlphaFrontendFixture(t, false, true)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"frontend/worker", "FE-abcde"},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		true, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code != 0 {
		t.Fatalf("cmdSling returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "Created ") {
		t.Fatalf("stdout = %q, want existing bead route without inline creation", stdout.String())
	}

	frontendStore, err := slingOpenStoreAtForCity(frontendDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(frontend): %v", err)
	}
	routed, err := frontendStore.Get("FE-abcde")
	if err != nil {
		t.Fatalf("frontendStore.Get(FE-abcde): %v", err)
	}
	if routed.Metadata["gc.routed_to"] != "frontend/worker" {
		t.Fatalf("frontend bead gc.routed_to = %q, want frontend/worker", routed.Metadata["gc.routed_to"])
	}
	all, err := frontendStore.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatalf("frontendStore.List: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("frontend store bead count = %d, want 1: %#v", len(all), all)
	}
}

func TestCmdSlingOneArgConfiguredPrefixAllAlphaExistingBeadUsesDefaultTarget(t *testing.T) {
	cityDir, frontendDir := setupCmdSlingConfiguredPrefixAllAlphaFrontendFixture(t, true, true)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"FE-abcde"},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		true, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code != 0 {
		t.Fatalf("cmdSling returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "Created ") {
		t.Fatalf("stdout = %q, want existing bead route without inline creation", stdout.String())
	}

	frontendStore, err := slingOpenStoreAtForCity(frontendDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(frontend): %v", err)
	}
	routed, err := frontendStore.Get("FE-abcde")
	if err != nil {
		t.Fatalf("frontendStore.Get(FE-abcde): %v", err)
	}
	if routed.Metadata["gc.routed_to"] != "frontend/worker" {
		t.Fatalf("frontend bead gc.routed_to = %q, want frontend/worker", routed.Metadata["gc.routed_to"])
	}
}

func setupCmdSlingConfiguredPrefixAllAlphaFrontendFixture(t *testing.T, defaultTarget, seedExisting bool) (cityDir, frontendDir string) {
	t.Helper()
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_BEADS", "file")

	cityDir = t.TempDir()
	t.Setenv("GC_CITY", cityDir)
	t.Setenv("GC_CITY_PATH", "")
	t.Setenv("GC_CITY_ROOT", "")
	t.Setenv("GC_RIG", "")
	t.Setenv("GC_RIG_ROOT", "")
	frontendDir = filepath.Join(cityDir, "frontend")
	if err := os.MkdirAll(frontendDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(frontend): %v", err)
	}
	if err := ensureScopedFileStoreLayout(cityDir); err != nil {
		t.Fatalf("ensureScopedFileStoreLayout: %v", err)
	}
	for _, dir := range []string{cityDir, frontendDir} {
		if err := ensurePersistedScopeLocalFileStore(dir); err != nil {
			t.Fatalf("ensurePersistedScopeLocalFileStore(%s): %v", dir, err)
		}
	}
	var rows []beads.Bead
	if seedExisting {
		rows = []beads.Bead{{ID: "FE-abcde", Title: "existing frontend work", Type: "task", Status: "open"}}
	}
	installSlingMemStores(t, cityDir, map[string][]beads.Bead{frontendDir: rows})
	defaultTargetLine := ""
	if defaultTarget {
		defaultTargetLine = "default_sling_target = \"frontend/worker\"\n"
	}
	cityToml := `[workspace]
name = "demo"

[[rigs]]
name = "frontend"
path = "frontend"
prefix = "FE"
` + defaultTargetLine + `
[[agent]]
name = "worker"
dir = "frontend"
`
	writeSlingTestCity(t, cityDir, cityToml)
	t.Chdir(cityDir)
	return cityDir, frontendDir
}

func writeTestFileStoreBeads(t *testing.T, scopeRoot string, stored []beads.Bead) {
	t.Helper()
	data := struct {
		Seq   int          `json:"seq"`
		Beads []beads.Bead `json:"beads"`
	}{Seq: len(stored), Beads: stored}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Marshal file store beads: %v", err)
	}
	if err := os.WriteFile(filepath.Join(scopeRoot, ".gc", "beads.json"), append(raw, '\n'), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", filepath.Join(scopeRoot, ".gc", "beads.json"), err)
	}
}

func TestCmdSlingForceRefusesMissingBeadWithoutEffects(t *testing.T) {
	cityDir := setupCmdSlingBeadExistsFixture(t)
	rigDir := filepath.Join(cityDir, "frontend")
	store, err := slingOpenStoreAtForCity(rigDir, cityDir)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, TierMode: beads.TierBoth})
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"frontend/worker", "FE-ghost1"},
		false, false, true,
		"", nil, "",
		false, false, false, "",
		false, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code == 0 {
		t.Fatalf("missing source returned success; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	after, err := store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, TierMode: beads.TierBoth})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("forced missing source created work, convoy or receipt: before=%+v after=%+v", before, after)
	}
	assertStoreHasNoBeadTitle(t, cityDir, cityDir, "FE-ghost1")
}

func TestCmdSlingAcceptsExistingBead(t *testing.T) {
	cityDir := setupCmdSlingBeadExistsFixture(t)
	rigDir := filepath.Join(cityDir, "frontend")

	rigStore, err := slingOpenStoreAtForCity(rigDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(rig): %v", err)
	}
	seeded, err := rigStore.Create(beads.Bead{ID: "FE-123", Title: "real work", Description: "original scope", AcceptanceCriteria: "original outcome", Type: "task"})
	if err != nil {
		t.Fatalf("seeding bead: %v", err)
	}

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"frontend/worker", seeded.ID},
		false, false, false, // force=false; existence check should pass naturally
		"", nil, "",
		true, false, false, "",
		false, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code != 0 {
		t.Fatalf("existing exact source refused: exit=%d stderr=%s", code, stderr.String())
	}
	routed, err := rigStore.Get(seeded.ID)
	if err != nil {
		t.Fatal(err)
	}
	if routed.Metadata["gc.routed_to"] != "frontend/worker" || routed.Title != seeded.Title || routed.Description != seeded.Description || routed.AcceptanceCriteria != seeded.AcceptanceCriteria {
		t.Fatalf("route changed original goal or failed to select target: %+v", routed)
	}
}

func TestCmdSlingMultiDashBeadIDRoutesExistingBead(t *testing.T) {
	// gc sling target fo-spawn-storm must route the existing bead and must
	// not create a new inline bead, when "fo-spawn-storm" exists in the store.
	cityDir, rigDir := setupCmdSlingMultiDashBeadFixture(t, true)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"foundations/worker", "fo-spawn-storm"},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		false, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code != 0 {
		t.Fatalf("cmdSling returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "Created ") {
		t.Errorf("created new inline bead instead of routing existing one; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "found existing bead") {
		t.Errorf("stderr = %q, want existing-bead routing breadcrumb", stderr.String())
	}

	rigStore, err := slingOpenStoreAtForCity(rigDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(rig): %v", err)
	}
	routed, err := rigStore.Get("fo-spawn-storm")
	if err != nil {
		t.Fatalf("rigStore.Get(fo-spawn-storm): %v", err)
	}
	if routed.Metadata["gc.routed_to"] != "foundations/worker" {
		t.Fatalf("rig bead gc.routed_to = %q, want foundations/worker", routed.Metadata["gc.routed_to"])
	}
	all, err := rigStore.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatalf("rigStore.List: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("rig store bead count = %d, want 1: %#v", len(all), all)
	}
}

func TestCmdSlingOneArgMultiDashExistingBeadUsesDefaultTarget(t *testing.T) {
	cityDir, rigDir := setupCmdSlingMultiDashBeadFixture(t, true)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"fo-spawn-storm"},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		false, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code != 0 {
		t.Fatalf("cmdSling returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "Created ") {
		t.Fatalf("stdout = %q, want existing bead route without inline creation", stdout.String())
	}
	if !strings.Contains(stderr.String(), "found existing bead") {
		t.Errorf("stderr = %q, want existing-bead routing breadcrumb", stderr.String())
	}

	rigStore, err := slingOpenStoreAtForCity(rigDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(rig): %v", err)
	}
	routed, err := rigStore.Get("fo-spawn-storm")
	if err != nil {
		t.Fatalf("rigStore.Get(fo-spawn-storm): %v", err)
	}
	if routed.Metadata["gc.routed_to"] != "foundations/worker" {
		t.Fatalf("rig bead gc.routed_to = %q, want foundations/worker", routed.Metadata["gc.routed_to"])
	}
	all, err := rigStore.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatalf("rigStore.List: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("rig store bead count = %d, want 1: %#v", len(all), all)
	}
}

// TestCmdSlingCrossRigMultiDashRouteRefused verifies the cross-store
// guard fires for a multi-dash-prefix rig bead routed across rigs (see
// tr-6s7yx). The bead remains in its source rig with no metadata
// mutation; the target rig store stays empty.
func TestCmdSlingCrossRigMultiDashRouteRefused(t *testing.T) {
	cityDir, rigDir := setupCmdSlingMultiDashBeadFixture(t, false)
	ordersDir := filepath.Join(cityDir, "orders")
	if err := os.MkdirAll(ordersDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(orders): %v", err)
	}
	if err := ensurePersistedScopeLocalFileStore(ordersDir); err != nil {
		t.Fatalf("ensurePersistedScopeLocalFileStore(orders): %v", err)
	}
	cityToml := `[workspace]
name = "demo"

[[rigs]]
name = "foundations"
path = "foundations"
prefix = "fo"

[[rigs]]
name = "orders"
path = "orders"
prefix = "od"

[[agent]]
name = "worker"
dir = "orders"
`
	writeSlingTestCity(t, cityDir, cityToml)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"orders/worker", "fo-spawn-storm"},
		false, false, true,
		"", nil, "",
		true, false, false, "",
		true, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code == 0 {
		t.Fatalf("cmdSling returned 0, want non-zero refusal; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	for _, want := range []string{"refusing cross-store route", "tr-6s7yx"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr missing %q: %s", want, stderr.String())
		}
	}

	rigStore, err := slingOpenStoreAtForCity(rigDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(rig): %v", err)
	}
	routed, err := rigStore.Get("fo-spawn-storm")
	if err != nil {
		t.Fatalf("rigStore.Get(fo-spawn-storm): %v", err)
	}
	if got := routed.Metadata["gc.routed_to"]; got != "" {
		t.Fatalf("refused route mutated rig bead: gc.routed_to = %q, want empty", got)
	}
	all, err := rigStore.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatalf("rigStore.List: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("rig store bead count = %d, want 1: %#v", len(all), all)
	}

	ordersStore, err := slingOpenStoreAtForCity(ordersDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(orders): %v", err)
	}
	ordersBeads, err := ordersStore.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatalf("ordersStore.List: %v", err)
	}
	if len(ordersBeads) != 0 {
		t.Fatalf("orders store bead count = %d, want 0: %#v", len(ordersBeads), ordersBeads)
	}
}

func TestCmdSlingUnderscoredPrefixMultiDashExistingBeadUsesPrefixStore(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_BEADS", "file")

	cityDir := t.TempDir()
	t.Setenv("GC_CITY", cityDir)
	t.Setenv("GC_CITY_PATH", "")
	t.Setenv("GC_CITY_ROOT", "")
	t.Setenv("GC_RIG", "")
	t.Setenv("GC_RIG_ROOT", "")
	rigDir := filepath.Join(cityDir, "live-docs")
	if err := os.MkdirAll(rigDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(rig): %v", err)
	}
	if err := ensureScopedFileStoreLayout(cityDir); err != nil {
		t.Fatalf("ensureScopedFileStoreLayout: %v", err)
	}
	for _, dir := range []string{cityDir, rigDir} {
		if err := ensurePersistedScopeLocalFileStore(dir); err != nil {
			t.Fatalf("ensurePersistedScopeLocalFileStore(%s): %v", dir, err)
		}
	}
	const beadID = "live_docs-spawn-storm"
	installSlingMemStores(t, cityDir, map[string][]beads.Bead{
		rigDir: {{ID: beadID, Title: "spawn storm bead", Type: "task", Status: "open"}},
	})
	cityToml := `[workspace]
name = "demo"

[[rigs]]
name = "live_docs"
path = "live-docs"
prefix = "live_docs"

[[agent]]
name = "worker"
dir = "live_docs"
`
	writeSlingTestCity(t, cityDir, cityToml)
	t.Chdir(cityDir)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"live_docs/worker", beadID},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		false, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code != 0 {
		t.Fatalf("cmdSling returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "Created ") {
		t.Fatalf("stdout = %q, want existing bead route without inline creation", stdout.String())
	}

	rigStore, err := slingOpenStoreAtForCity(rigDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(rig): %v", err)
	}
	routed, err := rigStore.Get(beadID)
	if err != nil {
		t.Fatalf("rigStore.Get(%s): %v", beadID, err)
	}
	if routed.Metadata["gc.routed_to"] != "live_docs/worker" {
		t.Fatalf("rig bead gc.routed_to = %q, want live_docs/worker", routed.Metadata["gc.routed_to"])
	}

	assertStoreHasNoBeadTitle(t, cityDir, cityDir, beadID)
}

func setupCmdSlingMultiDashBeadFixture(t *testing.T, defaultTarget bool) (cityDir, rigDir string) {
	t.Helper()
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_BEADS", "file")

	cityDir = t.TempDir()
	t.Setenv("GC_CITY", cityDir)
	t.Setenv("GC_CITY_PATH", "")
	t.Setenv("GC_CITY_ROOT", "")
	t.Setenv("GC_RIG", "")
	t.Setenv("GC_RIG_ROOT", "")
	rigDir = filepath.Join(cityDir, "foundations")
	if err := os.MkdirAll(rigDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(rig): %v", err)
	}
	if err := ensureScopedFileStoreLayout(cityDir); err != nil {
		t.Fatalf("ensureScopedFileStoreLayout: %v", err)
	}
	for _, dir := range []string{cityDir, rigDir} {
		if err := ensurePersistedScopeLocalFileStore(dir); err != nil {
			t.Fatalf("ensurePersistedScopeLocalFileStore(%s): %v", dir, err)
		}
	}
	installSlingMemStores(t, cityDir, map[string][]beads.Bead{
		rigDir:                           {{ID: "fo-spawn-storm", Title: "spawn storm bead", Type: "task", Status: "open"}},
		filepath.Join(cityDir, "orders"): nil,
	})
	defaultTargetLine := ""
	if defaultTarget {
		defaultTargetLine = "default_sling_target = \"foundations/worker\"\n"
	}
	cityToml := `[workspace]
name = "demo"

[[rigs]]
name = "foundations"
path = "foundations"
prefix = "fo"
` + defaultTargetLine + `

[[agent]]
name = "worker"
dir = "foundations"
`
	writeSlingTestCity(t, cityDir, cityToml)
	t.Chdir(cityDir)
	return cityDir, rigDir
}

func TestCmdSlingRefusesMissingConfiguredFallbackBeadID(t *testing.T) {
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_BEADS", "file")

	cityDir := t.TempDir()
	t.Setenv("GC_CITY", cityDir)
	t.Setenv("GC_CITY_PATH", "")
	t.Setenv("GC_CITY_ROOT", "")
	t.Setenv("GC_RIG", "")
	t.Setenv("GC_RIG_ROOT", "")
	rigDir := filepath.Join(cityDir, "orders")
	if err := os.MkdirAll(rigDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(rig): %v", err)
	}
	if err := ensureScopedFileStoreLayout(cityDir); err != nil {
		t.Fatalf("ensureScopedFileStoreLayout: %v", err)
	}
	if err := ensurePersistedScopeLocalFileStore(cityDir); err != nil {
		t.Fatalf("ensurePersistedScopeLocalFileStore(city): %v", err)
	}
	if err := ensurePersistedScopeLocalFileStore(rigDir); err != nil {
		t.Fatalf("ensurePersistedScopeLocalFileStore(rig): %v", err)
	}
	cityToml := `[workspace]
name = "demo"

[[rigs]]
name = "orders"
path = "orders"
prefix = "od"

[[agent]]
name = "worker"
dir = "orders"
`
	writeSlingTestCity(t, cityDir, cityToml)
	t.Chdir(cityDir)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"orders/worker", "od-zzzz1"},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		false, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code == 0 {
		t.Fatalf("cmdSling returned 0, want non-zero; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "Created ") {
		t.Fatalf("stdout = %q, want missing bead error instead of inline creation", stdout.String())
	}
	if !strings.Contains(stderr.String(), "not found") {
		t.Fatalf("stderr = %q, want missing bead diagnostic", stderr.String())
	}
}

func TestCmdSlingRefusesMissingConfiguredPrefixAllAlphaBeadID(t *testing.T) {
	cityDir, _ := setupCmdSlingConfiguredPrefixAllAlphaFrontendFixture(t, false, false)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"frontend/worker", "FE-abcde"},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		true, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code == 0 {
		t.Fatalf("cmdSling returned 0, want non-zero; stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "Created ") {
		t.Fatalf("stdout = %q, want missing bead error instead of inline creation", stdout.String())
	}
	if !strings.Contains(stderr.String(), "not found") {
		t.Fatalf("stderr = %q, want missing bead diagnostic", stderr.String())
	}

	frontendStore, err := slingOpenStoreAtForCity(filepath.Join(cityDir, "frontend"), cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(frontend): %v", err)
	}
	all, err := frontendStore.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatalf("frontendStore.List: %v", err)
	}
	if len(all) != 0 {
		t.Fatalf("frontend store bead count = %d, want 0: %#v", len(all), all)
	}
}

func TestSlingStoreEnvUsesRigBdRuntimeForMixedProviderRig(t *testing.T) {
	cityDir := t.TempDir()
	wantPort := strconv.Itoa(writeReachableManagedDoltState(t, cityDir))
	rigDir := filepath.Join(cityDir, "repo")
	if err := os.MkdirAll(filepath.Join(rigDir, ".beads"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cityDir, "city.toml"), []byte(`[workspace]
name = "demo"

[beads]
provider = "file"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rigDir, ".beads", "config.yaml"), []byte(`issue_prefix: repo
gc.endpoint_origin: inherited_city
gc.endpoint_status: verified
dolt.auto-start: false
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rigDir, ".beads", "metadata.json"), []byte(`{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"repo"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.City{Rigs: []config.Rig{{Name: "repo", Path: rigDir}}}

	env, err := slingStoreEnvWithError(cfg, cityDir, rigDir)
	if err != nil {
		t.Fatalf("slingStoreEnvWithError() error = %v", err)
	}
	if got := env["GC_DOLT_PORT"]; got != wantPort {
		t.Fatalf("GC_DOLT_PORT = %q, want %q", got, wantPort)
	}
	if got := env["BEADS_DIR"]; got != filepath.Join(rigDir, ".beads") {
		t.Fatalf("BEADS_DIR = %q, want %q", got, filepath.Join(rigDir, ".beads"))
	}
	if got := env["GC_RIG"]; got != "repo" {
		t.Fatalf("GC_RIG = %q, want %q", got, "repo")
	}
}

func TestSlingStoreEnvWithError_RefusesAnUnregisteredBackend(t *testing.T) {
	t.Setenv("GC_BEADS", "bd")

	cityDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cityDir, ".beads"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cityDir, ".beads", "config.yaml"), []byte(`issue_prefix: city
gc.endpoint_origin: managed_city
gc.endpoint_status: verified
dolt.auto-start: false
`), 0o644); err != nil {
		t.Fatal(err)
	}
	rigDir := filepath.Join(cityDir, "rigs", "pg")
	writeUnregisteredBackendMetadata(t, rigDir)
	if err := os.WriteFile(filepath.Join(rigDir, ".beads", "config.yaml"), []byte(`issue_prefix: pg
gc.endpoint_origin: inherited_city
gc.endpoint_status: verified
dolt.auto-start: false
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.City{Rigs: []config.Rig{{Name: "pg", Path: rigDir}}}

	_, err := slingStoreEnvWithError(cfg, cityDir, rigDir)
	assertRefusesUnregisteredBackend(t, err)
}

func TestTargetType(t *testing.T) {
	fixed := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
	if got := targetType(&fixed); got != "agent" {
		t.Errorf("targetType(fixed) = %q, want %q", got, "agent")
	}

	pool := config.Agent{Name: "polecat", MinActiveSessions: intPtr(1), MaxActiveSessions: intPtr(3)}
	if got := targetType(&pool); got != "pool" {
		t.Errorf("targetType(pool) = %q, want %q", got, "pool")
	}
}

func TestNewSlingCmdArgs(t *testing.T) {
	cmd := newSlingCmd(&bytes.Buffer{}, &bytes.Buffer{})
	if cmd.Use != "sling [target] <bead-or-formula-or-text>" {
		t.Errorf("Use = %q", cmd.Use)
	}
	// Verify flags exist.
	for _, name := range []string{"formula", "nudge", "force", "title", "on", "no-formula"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Errorf("missing flag %q", name)
		}
	}
	// Verify -f shorthand for --formula.
	if f := cmd.Flags().ShorthandLookup("f"); f == nil || f.Name != "formula" {
		t.Error("missing -f shorthand for --formula")
	}
	// Verify -t shorthand for --title.
	if f := cmd.Flags().ShorthandLookup("t"); f == nil || f.Name != "title" {
		t.Error("missing -t shorthand for --title")
	}
}

// fakeQuerier implements BeadQuerier for testing pre-flight checks.
// Callers may wire a parent bead for hasLiveConvoyParent lookups by
// setting bead.ParentID and parent.ID to the same value.
type fakeQuerier struct {
	bead   beads.Bead
	parent *beads.Bead
	err    error
}

func (q *fakeQuerier) Get(id string) (beads.Bead, error) {
	if q.err != nil {
		return beads.Bead{}, q.err
	}
	if q.parent != nil && id == q.bead.ParentID {
		return *q.parent, nil
	}
	return q.bead, nil
}

// fakeChildQuerier implements BeadChildQuerier for testing batch dispatch.
type fakeChildQuerier struct {
	beadsByID   map[string]beads.Bead
	childrenOf  map[string][]beads.Bead
	getErr      error
	childrenErr error
}

func newFakeChildQuerier() *fakeChildQuerier {
	return &fakeChildQuerier{
		beadsByID:  make(map[string]beads.Bead),
		childrenOf: make(map[string][]beads.Bead),
	}
}

func (q *fakeChildQuerier) Get(id string) (beads.Bead, error) {
	if q.getErr != nil {
		return beads.Bead{}, q.getErr
	}
	b, ok := q.beadsByID[id]
	if !ok {
		return beads.Bead{}, beads.ErrNotFound
	}
	return b, nil
}

func (q *fakeChildQuerier) Children(parentID string, _ ...beads.QueryOpt) ([]beads.Bead, error) {
	if q.childrenErr != nil {
		return nil, q.childrenErr
	}
	return q.childrenOf[parentID], nil
}

func (q *fakeChildQuerier) List(query beads.ListQuery) ([]beads.Bead, error) {
	if query.ParentID == "" {
		return nil, beads.ErrQueryRequiresScan
	}
	children, err := q.Children(query.ParentID)
	if err != nil {
		return nil, err
	}
	normalized := make([]beads.Bead, len(children))
	copy(normalized, children)
	for i := range normalized {
		if normalized[i].ParentID == "" {
			normalized[i].ParentID = query.ParentID
		}
	}
	query.ParentID = ""
	return beads.ApplyListQuery(normalized, query), nil
}

func TestDoSlingNativeRefusesStaleGoalLabelsOwnerAndHold(t *testing.T) {
	for _, field := range []string{"title", "description", "acceptance", "labels", "owner", "hold"} {
		t.Run(field, func(t *testing.T) {
			t.Setenv("GC_SLING_ADMISSION_COMMAND", "")
			a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
			cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}, Agents: []config.Agent{a}}
			work := beads.Bead{
				ID: "BL-42", Title: "approved goal", Description: "approved scope", AcceptanceCriteria: "approved outcome",
				Type: "task", Status: "open", Labels: []string{"approved"}, Metadata: map[string]string{},
			}
			conditions := &beads.UpdateConditions{
				Status: &work.Status, Assignee: &work.Assignee, Title: &work.Title, Description: &work.Description,
				AcceptanceCriteria: &work.AcceptanceCriteria, Labels: &work.Labels,
				Metadata: map[string]string{"handoff.conflict_state": ""},
			}
			current := work
			switch field {
			case "title":
				current.Title = "new human goal"
			case "description":
				current.Description = "new human scope"
			case "acceptance":
				current.AcceptanceCriteria = "new human outcome"
			case "labels":
				current.Labels = []string{"review:required"}
			case "owner":
				current.Assignee = "human"
			case "hold":
				current.Metadata = map[string]string{"handoff.conflict_state": "hold"}
			}
			store := beads.NewMemStoreFrom(0, []beads.Bead{current}, nil)
			deps, stdout, stderr := testDeps(cfg, runtime.NewFake(), func(_ string, _ string, _ map[string]string) (string, error) {
				t.Fatal("stale native dispatch unexpectedly invoked a shell provider")
				return "", errors.New("unexpected shell provider")
			})
			deps.Store = store
			deps.CityPath = t.TempDir()
			opts := testOpts(a, work.ID)
			opts.Conditions = conditions
			opts.Force, opts.Reassign, opts.NoConvoy = true, true, true
			if code := doSling(opts, deps, store, stdout, stderr); code != 13 {
				t.Fatalf("exit=%d, want refused stale %s; stdout=%s stderr=%s", code, field, stdout.String(), stderr.String())
			}
			got, err := store.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, current) {
				t.Fatalf("stale %s route overwrote work: got=%+v want=%+v", field, got, current)
			}
			if strings.Contains(stdout.String(), "Slung ") {
				t.Fatalf("refused route reported success: %s", stdout.String())
			}
		})
	}
}

// --- Batch dispatch (doSlingBatch) tests ---

func TestDoSlingBatchConvoyExpandsChildren(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Status: "open"},
		{ID: "BL-2", Status: "open"},
		{ID: "BL-3", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-1")
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for built-in routing: %v", len(runner.calls), runner.calls)
	}
	assertStoreRoutedTo(t, deps.Store, "BL-1", "mayor")
	assertStoreRoutedTo(t, deps.Store, "BL-2", "mayor")
	assertStoreRoutedTo(t, deps.Store, "BL-3", "mayor")
	if !strings.Contains(stdout.String(), "Expanding convoy CVY-1") {
		t.Errorf("stdout = %q, want expansion header", stdout.String())
	}
	if !strings.Contains(stdout.String(), "Slung 3/3 children") {
		t.Errorf("stdout = %q, want summary line", stdout.String())
	}
}

func TestDoSlingBatchConvoyMixedStatus(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-2"] = beads.Bead{ID: "CVY-2", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-2"] = []beads.Bead{
		{ID: "BL-1", Status: "open"},
		{ID: "BL-2", Status: "closed"},
		{ID: "BL-3", Status: "open"},
		{ID: "BL-4", Status: "in_progress"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-2")
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for built-in routing: %v", len(runner.calls), runner.calls)
	}
	assertStoreRoutedTo(t, deps.Store, "BL-1", "mayor")
	assertStoreRoutedTo(t, deps.Store, "BL-3", "mayor")
	out := stdout.String()
	if !strings.Contains(out, "Expanding convoy CVY-2 (4 children, 2 open)") {
		t.Errorf("stdout = %q, want header with counts", out)
	}
	if !strings.Contains(out, "Skipped BL-2 (status: closed)") {
		t.Errorf("stdout = %q, want skipped BL-2", out)
	}
	if !strings.Contains(out, "Skipped BL-4 (status: in_progress)") {
		t.Errorf("stdout = %q, want skipped BL-4", out)
	}
	if !strings.Contains(out, "Slung 2/4 children") {
		t.Errorf("stdout = %q, want summary", out)
	}
}

func TestDoSlingBatchConvoyNoOpenChildren(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-3"] = beads.Bead{ID: "CVY-3", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-3"] = []beads.Bead{
		{ID: "BL-1", Status: "closed"},
		{ID: "BL-2", Status: "closed"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-3")
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSlingBatch returned %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "no open children") {
		t.Errorf("stderr = %q, want 'no open children'", stderr.String())
	}
}

func TestDoSlingBatchEpicErrors(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["EP-1"] = beads.Bead{ID: "EP-1", Type: "epic", Status: "open"}
	q.childrenOf["EP-1"] = []beads.Bead{
		{ID: "BL-10", Status: "open"},
		{ID: "BL-11", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "EP-1")
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSlingBatch returned %d, want 1; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
	if stdout.String() != "" {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "first-class support is for convoys only") {
		t.Errorf("stderr = %q, want convoy-only error", stderr.String())
	}
}

func TestDoSlingBatchRegularBeadPassthrough(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["BL-42"] = beads.Bead{ID: "BL-42", Type: "task", Status: "open"}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for built-in routing: %v", len(runner.calls), runner.calls)
	}
	assertStoreRoutedTo(t, deps.Store, "BL-42", "mayor")
	if !strings.Contains(stdout.String(), "Slung BL-42") {
		t.Errorf("stdout = %q, want direct sling output", stdout.String())
	}
	if strings.Contains(stdout.String(), "Expanding") {
		t.Errorf("stdout = %q, should not expand a regular bead", stdout.String())
	}
}

func TestDoSlingBatchFormulaPassthrough(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	// Even if the querier has a convoy, --formula bypasses container check.
	q.beadsByID["convoy-formula"] = beads.Bead{ID: "convoy-formula", Type: "convoy"}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "convoy-formula")
	opts.IsFormula = true
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	// Should have gone through formula path.
	if !strings.Contains(stdout.String(), "formula") {
		t.Errorf("stdout = %q, want formula output", stdout.String())
	}
}

func TestDoSlingBatchNilQuerier(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	code := doSlingBatch(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Slung BL-42") {
		t.Errorf("stdout = %q, want direct sling output", stdout.String())
	}
}

func TestDoSlingBatchGetFails(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.getErr = fmt.Errorf("bd not available")

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code == 0 {
		t.Fatalf("doSlingBatch returned 0, want lookup failure; stdout: %s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "bd not available") {
		t.Errorf("stderr = %q, want lookup failure", stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("runner calls = %#v, want none", runner.calls)
	}
}

func TestDoSlingBatchChildrenFails(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenErr = fmt.Errorf("storage error")

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-1")
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSlingBatch returned %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "listing children") {
		t.Errorf("stderr = %q, want children error", stderr.String())
	}
}

func TestDoSlingBatchPartialFailure(t *testing.T) {
	t.Setenv("GC_SLING_ADMISSION_COMMAND", "")
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}, Agents: []config.Agent{a}}
	store := beads.NewMemStoreFrom(0, []beads.Bead{
		{ID: "CVY-1", Title: "batch", Type: "convoy", Status: "open"},
		{ID: "BL-1", Title: "one", Type: "task", Status: "open", ParentID: "CVY-1"},
		{ID: "BL-2", Title: "held", Type: "task", Status: "open", ParentID: "CVY-1", Metadata: map[string]string{"handoff.conflict_state": "hold"}},
		{ID: "BL-3", Title: "three", Type: "task", Status: "open", ParentID: "CVY-1"},
	}, nil)
	before, err := store.Get("BL-2")
	if err != nil {
		t.Fatal(err)
	}
	deps, stdout, stderr := testDeps(cfg, runtime.NewFake(), func(_ string, _ string, _ map[string]string) (string, error) {
		t.Fatal("native batch unexpectedly invoked shell provider")
		return "", errors.New("unexpected shell provider")
	})
	deps.Store = store
	deps.CityPath = t.TempDir()
	opts := testOpts(a, "CVY-1")
	opts.NoConvoy = true
	if code := doSlingBatch(opts, deps, store, stdout, stderr); code != 13 {
		t.Fatalf("exit=%d stdout=%s stderr=%s, want partial failure", code, stdout.String(), stderr.String())
	}
	assertStoreRoutedTo(t, store, "BL-1", "mayor")
	assertStoreRoutedTo(t, store, "BL-3", "mayor")
	after, err := store.Get("BL-2")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("held child changed: got=%+v want=%+v", after, before)
	}
	if !strings.Contains(stderr.String(), "Failed BL-2") || !strings.Contains(stdout.String(), "Slung 2/3 children") {
		t.Fatalf("partial failure misreported: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func TestDoSlingBatchAllChildrenFail(t *testing.T) {
	t.Setenv("GC_SLING_ADMISSION_COMMAND", "")
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}, Agents: []config.Agent{a}}
	store := beads.NewMemStoreFrom(0, []beads.Bead{
		{ID: "CVY-1", Type: "convoy", Status: "open"},
		{ID: "BL-1", Type: "task", Status: "open", ParentID: "CVY-1", Assignee: "human"},
		{ID: "BL-2", Type: "task", Status: "open", ParentID: "CVY-1", Metadata: map[string]string{"handoff.conflict_state": "hold"}},
	}, nil)
	before, err := store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true})
	if err != nil {
		t.Fatal(err)
	}
	deps, stdout, stderr := testDeps(cfg, runtime.NewFake(), func(_ string, _ string, _ map[string]string) (string, error) {
		t.Fatal("refused batch unexpectedly invoked shell provider")
		return "", errors.New("unexpected shell provider")
	})
	deps.Store = store
	deps.CityPath = t.TempDir()
	opts := testOpts(a, "CVY-1")
	opts.Force = true
	opts.NoConvoy = true
	if code := doSlingBatch(opts, deps, store, stdout, stderr); code != 13 {
		t.Fatalf("exit=%d stdout=%s stderr=%s, want all children refused", code, stdout.String(), stderr.String())
	}
	after, err := store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("failed batch changed canonical store: got=%+v want=%+v", after, before)
	}
	if !strings.Contains(stdout.String(), "Slung 0/2 children") {
		t.Fatalf("failed batch misreported success: stdout=%s stderr=%s", stdout.String(), stderr.String())
	}
}

func TestDoSlingBatchNudgeOnceAfterAll(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	sp.Calls = nil
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Status: "open"},
		{ID: "BL-2", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.CityPath = t.TempDir()
	prev := startNudgePoller
	startNudgePoller = func(_, _, _ string) error { return nil }
	t.Cleanup(func() { startNudgePoller = prev })
	opts := testOpts(a, "CVY-1")
	opts.Nudge = true
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	for _, c := range sp.Calls {
		if c.Method == "Nudge" {
			t.Fatalf("expected queued sling reminder, got direct nudge calls: %+v", sp.Calls)
		}
	}
	pending, _, dead, err := listQueuedNudges(deps.CityPath, "mayor", time.Now())
	if err != nil {
		t.Fatalf("listQueuedNudges: %v", err)
	}
	if len(pending) != 1 || len(dead) != 0 {
		t.Fatalf("pending=%d dead=%d, want 1/0", len(pending), len(dead))
	}
}

func TestDoSlingBatchForceSkipsPerChildWarnings(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	// Children already assigned — would normally warn.
	q.beadsByID["BL-1"] = beads.Bead{ID: "BL-1", Status: "open", Assignee: "other"}
	q.beadsByID["BL-2"] = beads.Bead{ID: "BL-2", Status: "open", Assignee: "other"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Status: "open", Assignee: "other"},
		{ID: "BL-2", Status: "open", Assignee: "other"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-1")
	opts.Force = true
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "already assigned") {
		t.Errorf("--force should suppress per-child warnings; stderr = %q", stderr.String())
	}
}

// --- On-formula (--on) tests ---

func TestOnAndFormulaMutuallyExclusive(t *testing.T) {
	cmd := newSlingCmd(&bytes.Buffer{}, &bytes.Buffer{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{"mayor", "BL-1", "--formula", "--on=code-review"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for mutually exclusive --formula and --on")
	}
	if !strings.Contains(err.Error(), "if any flags in the group") {
		t.Errorf("err = %v, want mutual exclusion error", err)
	}
}

func TestOnFormulaAttachesAndRoutes(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "BL-42", Title: "Work", Type: "task", Status: "open"},
	}, nil)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "code-review"
	code := doSling(opts, deps, deps.Store, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for built-in routing: %v", len(runner.calls), runner.calls)
	}
	source, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("store.Get(BL-42): %v", err)
	}
	if source.Metadata["gc.routed_to"] != "mayor" {
		t.Errorf("gc.routed_to = %q, want mayor", source.Metadata["gc.routed_to"])
	}
	rootID := source.Metadata["molecule_id"]
	if rootID == "" {
		t.Fatal("source bead missing molecule_id")
	}
	// Verify wisp was created in the store without parenting it to the outer bead.
	b, err := deps.Store.Get(rootID)
	if err != nil {
		t.Fatalf("store.Get(%s): %v", rootID, err)
	}
	if b.ParentID != "" {
		t.Errorf("wisp ParentID = %q, want empty", b.ParentID)
	}
	if b.Ref != "code-review" {
		t.Errorf("wisp Ref = %q, want %q", b.Ref, "code-review")
	}
	// Attached wisps route the source bead, not the molecule root: the wisp
	// root must stay unrouted so it is not independently claimed. Moving
	// routed_to onto the molecule (gastownhall/gascity#2848) would orphan the
	// work, since the attached root is privatized out of Ready().
	if got := b.Metadata["gc.routed_to"]; got != "" {
		t.Errorf("wisp root gc.routed_to = %q, want empty (attached wisp routes the source, not the molecule)", got)
	}
}

func TestOnRootOnlyFormulaKeepsAttachedWispPrivate(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "root-only.toml"), []byte(`
formula = "root-only"
description = "Private attached root"
version = 1
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.City{
		Workspace:     config.Workspace{Name: "test-city"},
		FormulaLayers: config.FormulaLayers{City: []string{dir}},
	}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "BL-42", Title: "Work", Type: "task", Status: "open"},
	}, nil)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "root-only"
	code := doSling(opts, deps, deps.Store, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	source, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("store.Get(BL-42): %v", err)
	}
	if source.Metadata["gc.routed_to"] != "mayor" {
		t.Errorf("source gc.routed_to = %q, want mayor", source.Metadata["gc.routed_to"])
	}
	rootID := source.Metadata["molecule_id"]
	if rootID == "" {
		t.Fatal("source bead missing molecule_id")
	}
	root, err := deps.Store.Get(rootID)
	if err != nil {
		t.Fatalf("store.Get(%s): %v", rootID, err)
	}
	if root.Type != "molecule" {
		t.Fatalf("attached root type = %q, want molecule", root.Type)
	}
	if root.Metadata["gc.kind"] == "wisp" {
		t.Fatalf("attached root leaked gc.kind=wisp metadata: %+v", root.Metadata)
	}
	ready, err := deps.Store.Ready()
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	for _, bead := range ready {
		if bead.ID == rootID {
			t.Fatalf("attached wisp root %s appeared in Ready(): %+v", rootID, ready)
		}
	}
}

func TestFormulaRootOnlyRoutesRunnableWispRoot(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "root-only.toml"), []byte(`
formula = "root-only"
description = "Standalone root"
version = 1
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.City{
		Workspace:     config.Workspace{Name: "test-city"},
		FormulaLayers: config.FormulaLayers{City: []string{dir}},
	}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStore()
	opts := testOpts(a, "root-only")
	opts.IsFormula = true
	code := doSling(opts, deps, deps.Store, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	root, err := deps.Store.Get("gc-1")
	if err != nil {
		t.Fatalf("store.Get(gc-1): %v", err)
	}
	if root.Type != "task" {
		t.Fatalf("root type = %q, want task", root.Type)
	}
	if root.Metadata["gc.kind"] != "wisp" {
		t.Fatalf("root gc.kind = %q, want wisp", root.Metadata["gc.kind"])
	}
	if root.Metadata["gc.routed_to"] != "mayor" {
		t.Fatalf("root gc.routed_to = %q, want mayor", root.Metadata["gc.routed_to"])
	}
	ready, err := deps.Store.Ready()
	if err != nil {
		t.Fatalf("Ready: %v", err)
	}
	if len(ready) != 1 || ready[0].ID != root.ID {
		t.Fatalf("Ready() = %+v, want only routed root %s", ready, root.ID)
	}
}

func TestOnFormulaCopiesSourcePriorityToCreatedBeads(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "BL-42", Title: "Source", Type: "task", Status: "open", Priority: priorityPtr(4)},
	}, nil)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "code-review"
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}

	source, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("Get(BL-42): %v", err)
	}
	rootID := source.Metadata["molecule_id"]
	if rootID == "" {
		t.Fatal("source bead missing molecule_id")
	}

	root, err := deps.Store.Get(rootID)
	if err != nil {
		t.Fatalf("Get(%s): %v", rootID, err)
	}
	if root.Priority == nil || *root.Priority != 4 {
		t.Fatalf("workflow root priority = %v, want 4", root.Priority)
	}

	queue := []string{rootID}
	seenIDs := map[string]struct{}{rootID: {}}
	seenDescendants := false
	for len(queue) > 0 {
		parentID := queue[0]
		queue = queue[1:]

		children, err := deps.Store.Children(parentID)
		if err != nil {
			t.Fatalf("Children(%s): %v", parentID, err)
		}
		for _, child := range children {
			seenDescendants = true
			if child.Priority == nil || *child.Priority != 4 {
				t.Fatalf("workflow descendant %s priority = %v, want 4", child.ID, child.Priority)
			}
			seenIDs[child.ID] = struct{}{}
			queue = append(queue, child.ID)
		}
	}
	if !seenDescendants {
		t.Fatal("workflow root has no descendants")
	}

	all, err := deps.Store.List(beads.ListQuery{AllowScan: true, TierMode: beads.TierBoth})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, bead := range all {
		if bead.ID == "BL-42" {
			continue
		}
		if bead.Type == "convoy" && bead.Title == "sling-BL-42" {
			continue
		}
		if _, ok := seenIDs[bead.ID]; !ok {
			t.Fatalf("created bead %s was not reachable from workflow root %s", bead.ID, rootID)
		}
	}
}

func TestOnFormulaGraphWorkflowPreassignsNonLatchBeadsForFixedAgent(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	applyFeatureFlags(cfg)
	t.Cleanup(func() { applyFeatureFlags(&config.City{}) })
	cfg.FormulaLayers.City = []string{testFormulaDir(t)}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	graphFormula := `
formula = "graph-work"
version = 2
contract = "graph.v2"

[[steps]]
id = "step"
title = "Do work"
`
	if err := os.WriteFile(filepath.Join(cfg.FormulaLayers.City[0], "graph-work.toml"), []byte(graphFormula), 0o644); err != nil {
		t.Fatal(err)
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "BL-42", Title: "Work", Type: "task", Status: "open"},
	}, nil)
	config.InjectImplicitAgents(cfg)
	addTestControlDispatcherAgents(cfg, "", "frontend")
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "graph-work"
	opts.ScopeKind = "city"
	opts.ScopeRef = "test-city"
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("graph workflow runner calls = %d, want 0; calls=%v", len(runner.calls), runner.calls)
	}

	parent, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("get parent: %v", err)
	}
	if got := parent.Status; got != "open" {
		t.Fatalf("parent status = %q, want open", got)
	}
	inputConvoys, err := deps.Store.List(beads.ListQuery{Type: "convoy"})
	if err != nil {
		t.Fatalf("list input convoys: %v", err)
	}
	var inputConvoy beads.Bead
	for _, candidate := range inputConvoys {
		members, err := convoycore.Members(deps.Store, candidate.ID, true)
		if err != nil {
			t.Fatalf("members(%s): %v", candidate.ID, err)
		}
		if len(members) == 1 && members[0].ID == "BL-42" {
			inputConvoy = candidate
			break
		}
	}
	if inputConvoy.ID == "" {
		t.Fatalf("input convoy for BL-42 not found in %+v", inputConvoys)
	}
	roots, err := deps.Store.ListByMetadata(map[string]string{"gc.input_convoy_id": inputConvoy.ID, "gc.kind": "workflow"}, 1)
	if err != nil {
		t.Fatalf("list workflow roots: %v", err)
	}
	if len(roots) != 1 {
		t.Fatalf("workflow root count = %d, want 1", len(roots))
	}
	rootID := roots[0].ID
	if parent.Metadata["workflow_id"] != rootID || parent.Metadata[sling.DispatchEffectStateKey] != "routed" || parent.Title != "Work" {
		t.Fatalf("source did not retain original goal and select exact activated provider: %+v root=%s", parent, rootID)
	}

	root, err := deps.Store.Get(rootID)
	if err != nil {
		t.Fatalf("get workflow root: %v", err)
	}
	if got := root.Status; got != "in_progress" {
		t.Fatalf("root status = %q, want in_progress", got)
	}
	// The canonical delivery key makes the activated root claimable.
	if got := root.Metadata["gc.routed_to"]; got != "mayor" {
		t.Fatalf("root gc.routed_to = %q, want mayor", got)
	}
	if got := root.Metadata["gc.source_bead_id"]; got != "" {
		t.Fatalf("root gc.source_bead_id = %q, want empty", got)
	}
	if got := root.Metadata["gc.scope_kind"]; got != "city" {
		t.Fatalf("root gc.scope_kind = %q, want city", got)
	}
	if got := root.Metadata["gc.scope_ref"]; got != "test-city" {
		t.Fatalf("root gc.scope_ref = %q, want test-city", got)
	}
	if got := root.Metadata[sourceworkflow.SourceStoreRefMetadataKey]; got != "" {
		t.Fatalf("root %s = %q, want empty", sourceworkflow.SourceStoreRefMetadataKey, got)
	}
	if got := root.Metadata["gc.root_store_ref"]; got != "city:test-city" {
		t.Fatalf("root gc.root_store_ref = %q, want city:test-city", got)
	}
	all, err := deps.Store.List(beads.ListQuery{AllowScan: true, TierMode: beads.TierBoth})
	if err != nil {
		t.Fatalf("list workflow beads: %v", err)
	}
	assigned := 0
	for _, bead := range all {
		if bead.Metadata["gc.root_bead_id"] != rootID {
			continue
		}
		switch bead.Metadata["gc.kind"] {
		case "workflow", "scope":
			if bead.Assignee != "" {
				t.Fatalf("latch bead %s assignee = %q, want empty", bead.ID, bead.Assignee)
			}
		case "workflow-finalize":
			if bead.Assignee != "" {
				t.Fatalf("workflow-finalize assignee = %q, want empty routed control-dispatcher queue", bead.Assignee)
			}
			if got := bead.Metadata["gc.routed_to"]; got != config.ControlDispatcherAgentName {
				t.Fatalf("workflow-finalize gc.routed_to = %q, want %q", got, config.ControlDispatcherAgentName)
			}
			if bead.Metadata[graphroute.GraphExecutionRouteMetaKey] != "mayor" {
				t.Fatalf("workflow-finalize execution route = %q, want mayor", bead.Metadata[graphroute.GraphExecutionRouteMetaKey])
			}
			assigned++
		default:
			if bead.Assignee != "mayor" {
				t.Fatalf("workflow bead %s assignee = %q, want mayor", bead.ID, bead.Assignee)
			}
			if bead.Metadata["gc.routed_to"] != "mayor" {
				t.Fatalf("workflow bead %s gc.routed_to = %q, want mayor", bead.ID, bead.Metadata["gc.routed_to"])
			}
			assigned++
		}
	}
	if assigned == 0 {
		t.Fatalf("expected at least one assigned workflow bead; rows=%#v", all)
	}
}

func TestDoSlingGraphWorkflowConflictReturnsExit3(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	applyFeatureFlags(cfg)
	t.Cleanup(func() { applyFeatureFlags(&config.City{}) })
	cfg.FormulaLayers.City = []string{testFormulaDir(t)}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	graphFormula := `
formula = "graph-work"
version = 2
contract = "graph.v2"

[[steps]]
id = "step"
title = "Do work"
`
	if err := os.WriteFile(filepath.Join(cfg.FormulaLayers.City[0], "graph-work.toml"), []byte(graphFormula), 0o644); err != nil {
		t.Fatal(err)
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "BL-42", Title: "Work", Type: "task", Status: "open"},
		{
			ID:     "wf-existing",
			Title:  "Existing workflow",
			Type:   "task",
			Status: "in_progress",
			Metadata: map[string]string{
				"gc.kind":             "workflow",
				"gc.formula_contract": "graph.v2",
				"gc.source_bead_id":   "BL-42",
			},
		},
	}, nil)
	config.InjectImplicitAgents(cfg)
	addTestControlDispatcherAgents(cfg, "", "frontend")
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "graph-work"
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 3 {
		t.Fatalf("doSling returned %d, want 3 for legacy source workflow conflict; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "already has live workflow") {
		t.Fatalf("stderr = %q, want live workflow conflict", stderr.String())
	}
}

func TestBatchOnGraphWorkflowStartsWorkflowWithoutRoutingChild(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	applyFeatureFlags(cfg)
	t.Cleanup(func() { applyFeatureFlags(&config.City{}) })
	cfg.FormulaLayers.City = []string{testFormulaDir(t)}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	graphFormula := `
formula = "graph-work"
version = 2
contract = "graph.v2"

[[steps]]
id = "step"
title = "Do work"
`
	if err := os.WriteFile(filepath.Join(cfg.FormulaLayers.City[0], "graph-work.toml"), []byte(graphFormula), 0o644); err != nil {
		t.Fatal(err)
	}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-1"] = []beads.Bead{{ID: "BL-1", Status: "open"}}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "CVY-1", Title: "Batch", Type: "convoy", Status: "open"},
		{ID: "BL-1", Title: "Child", Type: "task", Status: "open"},
	}, nil)
	config.InjectImplicitAgents(cfg)
	addTestControlDispatcherAgents(cfg, "", "frontend")
	opts := testOpts(a, "CVY-1")
	opts.OnFormula = "graph-work"
	opts.ScopeKind = "city"
	opts.ScopeRef = "test-city"
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("graph workflow runner calls = %d, want 0; calls=%v", len(runner.calls), runner.calls)
	}
	child, err := deps.Store.Get("BL-1")
	if err != nil {
		t.Fatalf("Get(BL-1): %v", err)
	}
	if got := child.Metadata["workflow_id"]; got != "" {
		t.Fatalf("child workflow_id = %q, want empty; convoy is graph.v2 input", got)
	}
	roots, err := deps.Store.ListByMetadata(map[string]string{"gc.input_convoy_id": "CVY-1", "gc.kind": "workflow"}, 1)
	if err != nil {
		t.Fatalf("list workflow roots: %v", err)
	}
	if len(roots) != 1 {
		t.Fatalf("workflow root count = %d, want 1", len(roots))
	}
	out := stdout.String()
	if !strings.Contains(out, "Attached workflow") {
		t.Fatalf("stdout = %q, want attached workflow message", out)
	}
	if strings.Contains(out, "  Slung BL-1") {
		t.Fatalf("stdout = %q, want no direct child sling line for graph workflow", out)
	}
}

func TestBatchOnGraphWorkflowConflictLeavesExistingRootInPlace(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	applyFeatureFlags(cfg)
	t.Cleanup(func() { applyFeatureFlags(&config.City{}) })
	cfg.FormulaLayers.City = []string{testFormulaDir(t)}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	graphFormula := `
formula = "graph-work"
version = 2
contract = "graph.v2"

[[steps]]
id = "step"
title = "Do work"
`
	if err := os.WriteFile(filepath.Join(cfg.FormulaLayers.City[0], "graph-work.toml"), []byte(graphFormula), 0o644); err != nil {
		t.Fatal(err)
	}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-1"] = []beads.Bead{{ID: "BL-1", Status: "open"}}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "CVY-1", Title: "Batch", Type: "convoy", Status: "open"},
		{ID: "BL-1", Title: "Child", Type: "task", Status: "open"},
		{
			ID:     "wf-existing",
			Title:  "Existing workflow",
			Type:   "task",
			Status: "in_progress",
			Metadata: map[string]string{
				"gc.kind":             "workflow",
				"gc.formula_contract": "graph.v2",
				"gc.source_bead_id":   "BL-1",
			},
		},
	}, nil)
	config.InjectImplicitAgents(cfg)
	addTestControlDispatcherAgents(cfg, "", "frontend")
	opts := testOpts(a, "CVY-1")
	opts.OnFormula = "graph-work"
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0 under convoy-first graph.v2; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("graph workflow runner calls = %d, want 0; calls=%v", len(runner.calls), runner.calls)
	}
	child, err := deps.Store.Get("BL-1")
	if err != nil {
		t.Fatalf("Get(BL-1): %v", err)
	}
	if got := child.Metadata["workflow_id"]; got != "" {
		t.Fatalf("child workflow_id = %q, want empty; convoy is graph.v2 input", got)
	}
	if !strings.Contains(stdout.String(), "Attached workflow") {
		t.Fatalf("stdout = %q, want attached workflow", stdout.String())
	}
}

func TestWorkflowStoreRefForDir(t *testing.T) {
	cityPath := filepath.Join(string(filepath.Separator), "tmp", "bright-lights")
	cfg := &config.City{
		Rigs: []config.Rig{
			{Name: "alpha", Path: "rigs/alpha"},
			{Name: "beta", Path: filepath.Join(cityPath, "rigs", "beta")},
		},
	}

	tests := []struct {
		name     string
		storeDir string
		cityName string
		want     string
	}{
		{
			name:     "named city store",
			storeDir: cityPath,
			cityName: "bright-lights",
			want:     "city:bright-lights",
		},
		{
			name:     "unnamed city store uses canonical fallback",
			storeDir: cityPath,
			cityName: "",
			want:     "city:city",
		},
		{
			name:     "relative rig path resolves under city",
			storeDir: filepath.Join(cityPath, "rigs", "alpha"),
			cityName: "bright-lights",
			want:     "rig:alpha",
		},
		{
			name:     "absolute rig path matches directly",
			storeDir: filepath.Join(cityPath, "rigs", "beta"),
			cityName: "bright-lights",
			want:     "rig:beta",
		},
		{
			name:     "unknown store yields empty ref",
			storeDir: filepath.Join(cityPath, "other"),
			cityName: "bright-lights",
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workflowStoreRefForDir(tt.storeDir, cityPath, tt.cityName, cfg); got != tt.want {
				t.Fatalf("workflowStoreRefForDir(%q) = %q, want %q", tt.storeDir, got, tt.want)
			}
		})
	}
}

func TestResolveSlingStoreRootUsesCanonicalRigRoot(t *testing.T) {
	cityPath := filepath.Join(t.TempDir(), "city")
	cfg := &config.City{
		Rigs: []config.Rig{
			{Name: "alpha", Path: filepath.Join("rigs", "alpha"), Prefix: "al"},
			{Name: "beta", Path: filepath.Join("rigs", "beta"), Prefix: "be"},
		},
	}

	got := resolveSlingStoreRoot(cfg, cityPath, "plain text", config.Agent{Dir: "alpha"})
	want := filepath.Join(cityPath, "rigs", "alpha")
	if got != want {
		t.Fatalf("resolveSlingStoreRoot() = %q, want %q", got, want)
	}
}

func TestResolveSlingStoreRootPrefersBeadPrefixRig(t *testing.T) {
	cityPath := filepath.Join(t.TempDir(), "city")
	cfg := &config.City{
		Rigs: []config.Rig{
			{Name: "alpha", Path: filepath.Join("rigs", "alpha"), Prefix: "al"},
			{Name: "beta", Path: filepath.Join("rigs", "beta"), Prefix: "be"},
		},
	}

	got := resolveSlingStoreRoot(cfg, cityPath, "be-123", config.Agent{Dir: "alpha"})
	want := filepath.Join(cityPath, "rigs", "beta")
	if got != want {
		t.Fatalf("resolveSlingStoreRoot() = %q, want %q", got, want)
	}
}

func TestResolveSlingStoreRootUsesPrefixRigForConfiguredAllAlphaBeadID(t *testing.T) {
	cityPath := filepath.Join(t.TempDir(), "city")
	cfg := &config.City{
		Rigs: []config.Rig{
			{Name: "frontend", Path: filepath.Join("rigs", "frontend"), Prefix: "FE"},
			{Name: "orders", Path: filepath.Join("rigs", "orders"), Prefix: "od"},
		},
	}

	got := resolveSlingStoreRoot(cfg, cityPath, "FE-hello", config.Agent{Dir: "orders"})
	want := filepath.Join(cityPath, "rigs", "frontend")
	if got != want {
		t.Fatalf("resolveSlingStoreRoot() = %q, want %q", got, want)
	}
}

func TestResolveSlingStoreRootHonorsHyphenatedRigPrefix(t *testing.T) {
	// A rig whose configured prefix itself contains a hyphen must
	// receive its own beads — the longest configured prefix wins
	// over a shorter prefix that also matches the bead-ID head.
	cityPath := filepath.Join(t.TempDir(), "city")
	cfg := &config.City{
		Rigs: []config.Rig{
			{Name: "agent", Path: filepath.Join("rigs", "agent"), Prefix: "agent"},
			{Name: "agent-diagnostics", Path: filepath.Join("rigs", "agent-diag"), Prefix: "agent-diagnostics"},
		},
	}

	got := resolveSlingStoreRoot(cfg, cityPath, "agent-diagnostics-hnn", config.Agent{Dir: "agent"})
	want := filepath.Join(cityPath, "rigs", "agent-diag")
	if got != want {
		t.Fatalf("resolveSlingStoreRoot(agent-diagnostics-hnn) = %q, want %q (longest configured prefix should win)", got, want)
	}

	// Sanity check: a bead under the shorter "agent" prefix still resolves
	// to that rig.
	got = resolveSlingStoreRoot(cfg, cityPath, "agent-x1", config.Agent{Dir: "agent-diagnostics"})
	want = filepath.Join(cityPath, "rigs", "agent")
	if got != want {
		t.Fatalf("resolveSlingStoreRoot(agent-x1) = %q, want %q", got, want)
	}
}

func TestResolveSlingStoreRootUsesCityRootForHQPrefix(t *testing.T) {
	cityPath := filepath.Join(t.TempDir(), "city")
	cfg := &config.City{
		Workspace: config.Workspace{Name: "bright-lights", Prefix: "hq"},
		Rigs: []config.Rig{
			{Name: "alpha", Path: filepath.Join("rigs", "alpha"), Prefix: "al"},
		},
	}

	got := resolveSlingStoreRoot(cfg, cityPath, "hq-123", config.Agent{Dir: "alpha"})
	if got != cityPath {
		t.Fatalf("resolveSlingStoreRoot() = %q, want city root %q", got, cityPath)
	}
}

// TestResolveSlingStoreRootHQPrefixUsesBdProviderFromCityRootMetadata
// reproduces dr-h6ze end to end at the sling layer: an HQ-prefixed bead
// (e.g. "dr-h6ze") correctly resolves its store root to the city root, but
// the city's own root is a Dolt-backed HQ store even though [beads]
// provider declares "file" as the default for rigs. Source validation,
// gc.routed_to mutation, and convoy/nudge all open the store via this same
// (storeDir, cityPath) pair, so the provider resolved here is what every
// downstream sling step actually talks to -- it must be "bd", not the
// configured file default, or the bead is silently unroutable.
func TestResolveSlingStoreRootHQPrefixUsesBdProviderFromCityRootMetadata(t *testing.T) {
	cityPath := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cityPath, ".beads"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cityPath, "city.toml"), []byte(`[workspace]
name = "bright-lights"

[beads]
provider = "file"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cityPath, ".beads", "metadata.json"), []byte(`{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"gc"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	setScopedBeadsProviderForTest(t, "", "file")

	cfg := &config.City{
		Workspace: config.Workspace{Name: "bright-lights", Prefix: "hq"},
		Rigs: []config.Rig{
			{Name: "alpha", Path: filepath.Join(cityPath, "rigs", "alpha"), Prefix: "al"},
		},
	}

	storeDir := resolveSlingStoreRoot(cfg, cityPath, "hq-123", config.Agent{Dir: "alpha"})
	if storeDir != cityPath {
		t.Fatalf("resolveSlingStoreRoot() = %q, want city root %q", storeDir, cityPath)
	}
	if got := authoritativeBeadsProviderForScope(storeDir, cityPath); got != "bd" {
		t.Fatalf("authoritativeBeadsProviderForScope(HQ store root) = %q, want bd (on-disk store identity, not ambient GC_BEADS=file)", got)
	}

	// Regression guard: a normal rig store with no Dolt marker of its own
	// still resolves through its declared file/bd contract unaffected by
	// the city-root fix above.
	rigStoreDir := resolveSlingStoreRoot(cfg, cityPath, "al-1", config.Agent{Dir: "alpha"})
	wantRigDir := filepath.Join(cityPath, "rigs", "alpha")
	if rigStoreDir != wantRigDir {
		t.Fatalf("resolveSlingStoreRoot(rig bead) = %q, want %q", rigStoreDir, wantRigDir)
	}
	if got := authoritativeBeadsProviderForScope(rigStoreDir, cityPath); got != "file" {
		t.Fatalf("authoritativeBeadsProviderForScope(rig store root) = %q, want file (no on-disk marker, city default preserved)", got)
	}
}

func TestSlingSourceWorkflowStoreCandidatesUseAuthoritativeProviders(t *testing.T) {
	setScopedBeadsProviderForTest(t, "", "file")

	cityPath := t.TempDir()
	rigPath := filepath.Join(cityPath, "rigs", "local")
	if err := os.MkdirAll(filepath.Join(cityPath, ".beads"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cityPath, ".beads", "metadata.json"), []byte(`{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"gc"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(rigPath, ".beads"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rigPath, ".beads", "metadata.json"), []byte(`{"database":"dolt","backend":"dolt","dolt_mode":"server","dolt_database":"local"}`), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.City{Rigs: []config.Rig{{Name: "local", Path: rigPath}}}
	providers := make(map[string]string)
	stores, _, err := openSourceWorkflowStoresWithProvider(cfg, cityPath, "", func(scopeRoot string) string {
		return authoritativeBeadsProviderForScope(scopeRoot, cityPath)
	}, func(dir string) (beads.Store, error) {
		providers[dir] = authoritativeBeadsProviderForScope(dir, cityPath)
		return beads.NewMemStore(), nil
	})
	if err != nil {
		t.Fatalf("openSourceWorkflowStoresWith: %v", err)
	}
	if len(stores) != 2 {
		t.Fatalf("stores = %d, want city and rig candidates", len(stores))
	}
	if got := providers[cityPath]; got != "bd" {
		t.Fatalf("city candidate provider = %q, want bd despite ambient GC_BEADS=file", got)
	}
	if got := providers[rigPath]; got != "bd" {
		t.Fatalf("rig candidate provider = %q, want bd despite ambient GC_BEADS=file", got)
	}

	setScopedBeadsProviderForTest(t, "", "")
	remoteCity := t.TempDir()
	if err := os.WriteFile(filepath.Join(remoteCity, "city.toml"), []byte(`[workspace]
name = "remote"

[beads]
provider = "exec:/tmp/remote-beads"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	remoteProviders := make(map[string]string)
	_, _, err = openSourceWorkflowStoresWith(&config.City{}, remoteCity, "", func(dir string) (beads.Store, error) {
		remoteProviders[dir] = authoritativeBeadsProviderForScope(dir, remoteCity)
		return beads.NewMemStore(), nil
	})
	if err != nil {
		t.Fatalf("openSourceWorkflowStoresWith(remote): %v", err)
	}
	if got := remoteProviders[remoteCity]; got != "exec:/tmp/remote-beads" {
		t.Fatalf("remote candidate provider = %q, want custom exec provider unchanged", got)
	}
}

func TestSlingFormulaRepoDirUsesCanonicalRigRoot(t *testing.T) {
	cityPath := filepath.Join(t.TempDir(), "city")
	deps := slingDeps{
		CityPath: cityPath,
		Cfg: &config.City{
			Rigs: []config.Rig{{Name: "alpha", Path: filepath.Join("rigs", "alpha"), Prefix: "al"}},
		},
	}

	got := sling.SlingFormulaRepoDir("plain text", deps, config.Agent{Dir: "alpha"})
	want := filepath.Join(cityPath, "rigs", "alpha")
	if got != want {
		t.Fatalf("SlingFormulaRepoDir() = %q, want %q", got, want)
	}
}

func TestCLIDirectSessionResolverMaterializesNamedSessionAliasShadow(t *testing.T) {
	t.Setenv("GC_SESSION", "fake")

	store := beads.NewMemStore()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:              "claude",
			StartCommand:      "true",
			MinActiveSessions: intPtr(0),
			MaxActiveSessions: intPtr(1),
		}},
		NamedSessions: []config.NamedSession{{
			Template: "claude",
			Mode:     "on_demand",
		}},
	}

	id, ok, err := cliDirectSessionResolver(store, cfg.Workspace.Name, t.TempDir(), cfg, "claude", "")
	if err != nil {
		t.Fatalf("cliDirectSessionResolver: %v", err)
	}
	if !ok || id == "" {
		t.Fatalf("cliDirectSessionResolver did not materialize named-session alias shadow: id=%q ok=%v", id, ok)
	}
	bead, err := store.Get(id)
	if err != nil {
		t.Fatalf("store.Get(%s): %v", id, err)
	}
	if got := bead.Metadata[namedSessionMetadataKey]; got != "true" {
		t.Fatalf("configured_named_session = %q, want true", got)
	}
}

func TestResolveGraphDirectSessionBindingMaterializesNamedSessionAliasShadow(t *testing.T) {
	t.Setenv("GC_SESSION", "fake")

	store := beads.NewMemStore()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{{
			Name:              "claude",
			StartCommand:      "true",
			MinActiveSessions: intPtr(0),
			MaxActiveSessions: intPtr(1),
		}},
		NamedSessions: []config.NamedSession{{
			Template: "claude",
			Mode:     "on_demand",
		}},
	}

	binding, ok, err := graphroute.ResolveGraphDirectSessionBinding(store, cfg.Workspace.Name, cfg, "claude", "", cliGraphrouteDeps(t.TempDir()))
	if err != nil {
		t.Fatalf("ResolveGraphDirectSessionBinding: %v", err)
	}
	if !ok || binding.DirectSessionID == "" {
		t.Fatalf("ResolveGraphDirectSessionBinding did not materialize named-session alias shadow: binding=%+v ok=%v", binding, ok)
	}
	bead, err := store.Get(binding.DirectSessionID)
	if err != nil {
		t.Fatalf("store.Get(%s): %v", binding.DirectSessionID, err)
	}
	if got := bead.Metadata[namedSessionMetadataKey]; got != "true" {
		t.Fatalf("configured_named_session = %q, want true", got)
	}
}

func TestDoSlingRejectsScopeForPlainBeadRouting(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Agents: []config.Agent{
			{Name: "worker", Dir: "myrig"},
		},
		Rigs: []config.Rig{
			{Name: "myrig", Path: "/city/myrig"},
		},
	}
	a, ok := resolveAgentIdentity(cfg, "worker", "")
	if !ok {
		t.Fatal("resolveAgentIdentity(worker) failed")
	}
	sp := runtime.NewFake()
	deps, stdout, stderr := testDeps(cfg, sp, func(dir, command string, env map[string]string) (string, error) {
		t.Fatalf("runner should not be invoked, got dir=%q command=%q env=%v", dir, command, env)
		return "", nil
	})
	opts := testOpts(a, "MY-42")
	opts.ScopeKind = "city"
	opts.ScopeRef = "test-city"

	code := doSling(opts, deps, nil, stdout, stderr)

	if code == 0 {
		t.Fatalf("doSling returned %d, want non-zero", code)
	}
	if !strings.Contains(stderr.String(), "--scope-kind/--scope-ref require a formula-backed workflow launch") {
		t.Fatalf("stderr = %q, want scope validation message", stderr.String())
	}
}

func TestOnFormulaGraphWorkflowPokesOnce(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	applyFeatureFlags(cfg)
	t.Cleanup(func() { applyFeatureFlags(&config.City{}) })
	config.InjectImplicitAgents(cfg)
	addTestControlDispatcherAgents(cfg, "", "frontend")
	cfg.FormulaLayers.City = []string{testFormulaDir(t)}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	graphFormula := `
formula = "graph-work"
version = 2
contract = "graph.v2"

[[steps]]
id = "step"
title = "Do work"
`
	if err := os.WriteFile(filepath.Join(cfg.FormulaLayers.City[0], "graph-work.toml"), []byte(graphFormula), 0o644); err != nil {
		t.Fatal(err)
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "BL-42", Title: "Work", Type: "task", Status: "open"},
	}, nil)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "graph-work"

	oldPoke := slingPokeControlDispatcher
	defer func() { slingPokeControlDispatcher = oldPoke }()
	pokes := 0
	slingPokeControlDispatcher = func(string) error {
		pokes++
		return nil
	}

	code := doSling(opts, deps, nil, stdout, stderr)
	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if pokes != 1 {
		t.Fatalf("graph workflow pokes = %d, want 1", pokes)
	}
}

func TestOnFormulaWithTitle(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	opts.Title = "my-review"
	opts.OnFormula = "code-review"
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	// MolCookOn goes through the store; verify bead was created with title and
	// left unattached from the outer bead.
	b, err := deps.Store.Get("gc-1")
	if err != nil {
		t.Fatalf("store.Get(gc-1): %v", err)
	}
	if b.Title != "my-review" {
		t.Errorf("bead title = %q, want %q", b.Title, "my-review")
	}
	if b.ParentID != "" {
		t.Errorf("bead ParentID = %q, want empty", b.ParentID)
	}
}

func TestReloadControllerConfigUsesControllerReloadCommand(t *testing.T) {
	dir := shortSocketTempDir(t, "gc-reload-cmd-")
	gcDir := filepath.Join(dir, ".gc")
	if err := os.MkdirAll(gcDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", gcDir, err)
	}

	sockPath := filepath.Join(gcDir, "controller.sock")
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("Listen(unix, %q): %v", sockPath, err)
	}
	defer lis.Close() //nolint:errcheck

	cmdCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close() //nolint:errcheck
		buf := make([]byte, 64)
		n, err := conn.Read(buf)
		if err != nil {
			errCh <- err
			return
		}
		cmdCh <- string(buf[:n])
		_, _ = conn.Write([]byte("ok\n"))
	}()

	if err := reloadControllerConfig(dir); err != nil {
		t.Fatalf("reloadControllerConfig(): %v", err)
	}

	select {
	case cmd := <-cmdCh:
		if cmd != "reload\n" {
			t.Fatalf("controller command = %q, want %q", cmd, "reload\n")
		}
	case err := <-errCh:
		t.Fatalf("controller accept/read: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for controller reload command")
	}
}

func TestPokeSupervisorReturnsWithoutWaitingForReloadAck(t *testing.T) {
	t.Setenv("GC_HOME", shortSocketTempDir(t, "gc-home-"))
	t.Setenv("XDG_RUNTIME_DIR", shortSocketTempDir(t, "gc-run-"))
	sockPath := supervisorSocketPath()
	if err := os.MkdirAll(filepath.Dir(sockPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", filepath.Dir(sockPath), err)
	}

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("Listen(unix, %q): %v", sockPath, err)
	}
	defer lis.Close() //nolint:errcheck

	cmdCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := lis.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close() //nolint:errcheck
		buf := make([]byte, 64)
		n, err := conn.Read(buf)
		if err != nil {
			errCh <- err
			return
		}
		cmdCh <- string(buf[:n])
		time.Sleep(500 * time.Millisecond)
	}()

	start := time.Now()
	if err := pokeSupervisor(); err != nil {
		t.Fatalf("pokeSupervisor(): %v", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("pokeSupervisor() took %v, want it to return immediately after queueing reload", elapsed)
	}

	select {
	case cmd := <-cmdCh:
		if cmd != "reload\n" {
			t.Fatalf("supervisor command = %q, want %q", cmd, "reload\n")
		}
	case err := <-errCh:
		t.Fatalf("supervisor accept/read: %v", err)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for supervisor reload command")
	}
}

func TestOnFormulaCookError(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "nonexistent-formula"
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSling returned %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "not found") {
		t.Errorf("stderr = %q, want formula error", stderr.String())
	}
}

func TestOnFormulaCookMissingFormula(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "totally-missing"
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSling returned %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "not found") {
		t.Errorf("stderr = %q, want 'not found'", stderr.String())
	}
}

func TestFormulaSlingReportsAllMissingRequiredVarsAtOnce(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	dir := testFormulaDir(t)
	cfg.FormulaLayers.City = []string{dir}
	formulaBody := `
formula = "repro-required-vars"
version = 1

[vars.target_id]
description = "Bead being worked on"
required = true

[vars.workspace]
description = "Workspace path"
required = true

[[steps]]
id = "do-work"
title = "Do work for {{target_id}}"
description = "Target: {{target_id}}, workspace: {{workspace}}"
`
	if err := os.WriteFile(filepath.Join(dir, "repro.toml"), []byte(formulaBody), 0o644); err != nil {
		t.Fatal(err)
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "repro")
	opts.IsFormula = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSling returned %d, want 1; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	errText := stderr.String()
	if !strings.Contains(errText, `variable "target_id" is required`) {
		t.Fatalf("stderr = %q, want missing target_id reported", errText)
	}
	if !strings.Contains(errText, `variable "workspace" is required`) {
		t.Fatalf("stderr = %q, want missing workspace reported", errText)
	}
	if strings.Contains(errText, "bead title contains unresolved variable(s)") {
		t.Fatalf("stderr = %q, want consolidated required-var validation instead of title-only failure", errText)
	}
}

func TestFormulaSlingReportsRequiredAndResidualTitleVarsWhenSomeVarsProvided(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	dir := testFormulaDir(t)
	cfg.FormulaLayers.City = []string{dir}
	formulaBody := `
formula = "repro-mixed-vars"
version = 1

[vars.target_id]
description = "Bead being worked on"
required = true

[vars.workspace]
description = "Workspace path"
required = true

[[steps]]
id = "do-work"
title = "Do work for {{title}}"
description = "Target: {{target_id}}, workspace: {{workspace}}"
`
	if err := os.WriteFile(filepath.Join(dir, "repro-mixed-vars.toml"), []byte(formulaBody), 0o644); err != nil {
		t.Fatal(err)
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "repro-mixed-vars")
	opts.IsFormula = true
	opts.Vars = []string{"target_id=BL-42"}
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSling returned %d, want 1; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	errText := stderr.String()
	if !strings.Contains(errText, `variable "workspace" is required`) {
		t.Fatalf("stderr = %q, want missing workspace reported", errText)
	}
	if !strings.Contains(errText, `step "repro-mixed-vars.do-work": bead title contains unresolved variable(s) title`) {
		t.Fatalf("stderr = %q, want unresolved title variable reported", errText)
	}
}

func TestOnFormulaExistingMoleculeErrors(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	store := beads.NewMemStoreFrom(0, []beads.Bead{
		{ID: "BL-42", Title: "original goal", Description: "original scope", AcceptanceCriteria: "original outcome", Type: "task", Status: "open", Assignee: "mayor"},
		{ID: "MOL-1", Title: "existing provider", Type: "molecule", Status: "open", ParentID: "BL-42"},
	}, nil)
	before, err := store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, TierMode: beads.TierBoth})
	if err != nil {
		t.Fatal(err)
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = store
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "code-review"
	opts.Force = true
	code := doSling(opts, deps, store, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSling returned %d, want 1", code)
	}
	after, err := store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, TierMode: beads.TierBoth})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("existing provider conflict changed original work or created a duplicate: before=%+v after=%+v", before, after)
	}
	// No runner calls — should fail before routing.
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0 (should not route)", len(runner.calls))
	}
}

func TestOnFormulaMissingRequiredVarsBeforeExistingMolecule(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "agent-one", MaxActiveSessions: intPtr(1)}

	dir := testFormulaDir(t)
	cfg.FormulaLayers.City = []string{dir}
	formulaBody := `
formula = "requires-workspace"
version = 1

[vars.workspace]
description = "Workspace path"
required = true

[[steps]]
id = "do-work"
title = "Work in {{workspace}}"
`
	if err := os.WriteFile(filepath.Join(dir, "requires-workspace.toml"), []byte(formulaBody), 0o644); err != nil {
		t.Fatal(err)
	}

	q := newFakeChildQuerier()
	q.beadsByID["BL-42"] = beads.Bead{ID: "BL-42", Type: "task", Status: "open", Assignee: "other-agent"}
	q.childrenOf["BL-42"] = []beads.Bead{
		{ID: "MOL-1", Type: "molecule", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "requires-workspace"
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSling returned %d, want 1; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	errText := stderr.String()
	if !strings.Contains(errText, `variable "workspace" is required`) {
		t.Fatalf("stderr = %q, want missing workspace reported", errText)
	}
	if strings.Contains(errText, "already has attached molecule") {
		t.Fatalf("stderr = %q, missing required vars should not be masked by attachment conflict", errText)
	}
}

func TestOnFormulaExistingWispErrors(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	store := beads.NewMemStoreFrom(0, []beads.Bead{
		{ID: "BL-42", Title: "original goal", Type: "task", Status: "open", Assignee: "mayor", Metadata: map[string]string{"molecule_id": "MOL-5"}},
		{ID: "MOL-5", Title: "selected pending provider", Type: "molecule", Status: "open", Ephemeral: true, Metadata: map[string]string{"gc.attach_fence_pending": "true"}},
	}, nil)
	before, err := store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, TierMode: beads.TierBoth})
	if err != nil {
		t.Fatal(err)
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = store
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "code-review"
	opts.Force = true
	code := doSling(opts, deps, store, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSling returned %d, want 1", code)
	}
	after, err := store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, TierMode: beads.TierBoth})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("pending selected provider was burned or duplicated: before=%+v after=%+v", before, after)
	}
}

func TestOnFormulaReplacesOnlyUnheldUnassignedPredecessor(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(fmt.Sprintf("held=%t", held), func(t *testing.T) {
			a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
			cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
			work := beads.Bead{ID: "BL-42", Title: "original goal", Description: "original scope", AcceptanceCriteria: "original outcome", Type: "task", Status: "open"}
			old := beads.Bead{ID: "MOL-1", Title: "original provider", Type: "molecule", Status: "open", ParentID: work.ID}
			if held {
				old.Metadata = map[string]string{"handoff.conflict_state": "hold"}
			}
			store := beads.NewMemStoreFrom(0, []beads.Bead{work, old}, nil)
			before, err := store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, TierMode: beads.TierBoth})
			if err != nil {
				t.Fatal(err)
			}
			deps, stdout, stderr := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
			deps.Store = store
			opts := testOpts(a, work.ID)
			opts.OnFormula, opts.NoConvoy = "code-review", true
			code := doSling(opts, deps, store, stdout, stderr)
			if held {
				after, err := store.List(beads.ListQuery{AllowScan: true, IncludeClosed: true, TierMode: beads.TierBoth})
				if err != nil {
					t.Fatal(err)
				}
				if code != 13 || !reflect.DeepEqual(after, before) {
					t.Fatalf("held predecessor was blindly burned or duplicated: exit=%d before=%+v after=%+v stderr=%s", code, before, after, stderr.String())
				}
				return
			}
			if code != 0 {
				t.Fatalf("guarded replacement exit=%d; stderr=%s", code, stderr.String())
			}
			source, err := store.Get(work.ID)
			if err != nil {
				t.Fatal(err)
			}
			predecessor, err := store.Get(old.ID)
			if err != nil {
				t.Fatal(err)
			}
			if source.Title != work.Title || source.Description != work.Description || source.AcceptanceCriteria != work.AcceptanceCriteria || source.Metadata["molecule_id"] == "" || source.Metadata["molecule_id"] == old.ID || predecessor.Status != "closed" {
				t.Fatalf("replacement lost original goal or failed to retire exact predecessor: source=%+v predecessor=%+v", source, predecessor)
			}
			assertStoreRoutedTo(t, store, work.ID, "mayor")
		})
	}
}

func TestOnFormulaMetadataAttachmentSkipsIdempotentRetry(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(1, []beads.Bead{{ID: "BL-42", Title: "Work", Type: "task", Status: "open"}}, nil)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "code-review"

	if code := doSling(opts, deps, deps.Store, stdout, stderr); code != 0 {
		t.Fatalf("first doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	source, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("Get(BL-42): %v", err)
	}
	firstRootID := source.Metadata["molecule_id"]
	if firstRootID == "" {
		t.Fatal("first sling did not set molecule_id")
	}
	assertStoreRoutedTo(t, deps.Store, "BL-42", "mayor")

	stdout.Reset()
	stderr.Reset()
	if code := doSling(opts, deps, deps.Store, stdout, stderr); code != 0 {
		t.Fatalf("second doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty on idempotent retry", stderr.String())
	}
	if !strings.Contains(stdout.String(), "skipping (idempotent)") {
		t.Fatalf("stdout = %q, want idempotent skip", stdout.String())
	}

	firstRoot, err := deps.Store.Get(firstRootID)
	if err != nil {
		t.Fatalf("Get(%s): %v", firstRootID, err)
	}
	if firstRoot.Status != "open" {
		t.Fatalf("first root status = %q, want open", firstRoot.Status)
	}

	updatedSource, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("Get(BL-42) after retry: %v", err)
	}
	if updatedSource.Metadata["molecule_id"] != firstRootID {
		t.Fatalf("source molecule_id = %q, want %q", updatedSource.Metadata["molecule_id"], firstRootID)
	}
}

func TestOnFormulaSkipsClosedMolecule(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["BL-42"] = beads.Bead{ID: "BL-42", Type: "task", Status: "open", Assignee: "other-agent"}
	q.childrenOf["BL-42"] = []beads.Bead{
		{ID: "MOL-1", Type: "molecule", Status: "closed"}, // closed — should be skipped
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "code-review"
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0 (closed molecule should be skipped); stderr: %s", code, stderr.String())
	}
}

func TestOnFormulaCleanBead(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["BL-42"] = beads.Bead{ID: "BL-42", Type: "task", Status: "open"}
	q.childrenOf["BL-42"] = []beads.Bead{{ID: "STEP-1", Type: "step", Status: "open"}}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "code-review"
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for built-in routing: %v", len(runner.calls), runner.calls)
	}
	assertStoreRoutedTo(t, deps.Store, "BL-42", "mayor")
}

func TestOnFormulaNilQuerier(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "code-review"
	// nil querier → molecule check skipped, should succeed.
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
}

func TestOnFormulaOutput(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "code-review"
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	// MemStore generates IDs like "gc-1".
	if !strings.Contains(out, "Attached wisp gc-1 (formula \"code-review\") to BL-42") {
		t.Errorf("stdout = %q, want attach message", out)
	}
	if !strings.Contains(out, "Slung BL-42 (with formula \"code-review\")") {
		t.Errorf("stdout = %q, want slung with formula message", out)
	}
}

func TestOnFormulaTitleOverrideBypassesRootTitlePlaceholder(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "agent-one", MaxActiveSessions: intPtr(1)}

	dir := testFormulaDir(t)
	cfg.FormulaLayers.City = []string{dir}
	formulaBody := `
formula = "root-title-placeholder"
version = 1

[vars.title]
description = "Root title"

[[steps]]
id = "work"
title = "Work"
`
	if err := os.WriteFile(filepath.Join(dir, "root-title-placeholder.toml"), []byte(formulaBody), 0o644); err != nil {
		t.Fatal(err)
	}

	q := newFakeChildQuerier()
	q.beadsByID["BL-42"] = beads.Bead{ID: "BL-42", Type: "task", Status: "open"}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "root-title-placeholder"
	opts.Title = "Reviewed work"
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stderr.String(), "bead title contains unresolved variable") {
		t.Fatalf("stderr = %q, title override should satisfy root placeholder", stderr.String())
	}
}

func TestBatchOnConvoy(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Status: "open"},
		{ID: "BL-2", Status: "open"},
		{ID: "BL-3", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-1")
	opts.OnFormula = "code-review"
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	// MolCookOn goes through the store, verify 3 wisps were created.
	all, _ := deps.Store.ListOpen()
	molCount := 0
	for _, b := range all {
		if b.Type == "molecule" {
			molCount++
		}
	}
	if molCount != 3 {
		t.Errorf("got %d molecule beads in store, want 3", molCount)
	}
	out := stdout.String()
	// MemStore generates IDs gc-1, gc-2, ... Each molecule.Cook creates
	// 2 beads (root + step), so wisp root IDs are gc-1, gc-3, gc-5.
	if !strings.Contains(out, "Attached wisp gc-1") {
		t.Errorf("stdout = %q, want gc-1 attach", out)
	}
	if !strings.Contains(out, "Attached wisp gc-3") {
		t.Errorf("stdout = %q, want gc-3 attach", out)
	}
	if !strings.Contains(out, "Attached wisp gc-5") {
		t.Errorf("stdout = %q, want gc-5 attach", out)
	}
	if !strings.Contains(out, "Slung 3/3 children") {
		t.Errorf("stdout = %q, want summary", out)
	}
}

func TestBatchOnFormulaRequiredIssueVarsUseChildID(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "agent-one", MaxActiveSessions: intPtr(1)}

	dir := testFormulaDir(t)
	cfg.FormulaLayers.City = []string{dir}
	formulaBody := `
formula = "requires-issue"
version = 1

[vars.issue]
description = "Source bead ID"
required = true

[[steps]]
id = "work"
title = "Work {{issue}}"
`
	if err := os.WriteFile(filepath.Join(dir, "requires-issue.toml"), []byte(formulaBody), 0o644); err != nil {
		t.Fatal(err)
	}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Type: "task", Status: "open"},
		{ID: "BL-2", Type: "task", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-1")
	opts.OnFormula = "requires-issue"
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if strings.Contains(stderr.String(), `variable "issue" is required`) {
		t.Fatalf("stderr = %q, batch graph classification should not validate before child vars are available", stderr.String())
	}
}

func TestBatchOnFormulaMissingRequiredVarsBeforeExistingMolecule(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "agent-one", MaxActiveSessions: intPtr(1)}

	dir := testFormulaDir(t)
	cfg.FormulaLayers.City = []string{dir}
	formulaBody := `
formula = "batch-requires-workspace"
version = 1

[vars.workspace]
description = "Workspace path"
required = true

[[steps]]
id = "work"
title = "Work {{workspace}}"
`
	if err := os.WriteFile(filepath.Join(dir, "batch-requires-workspace.toml"), []byte(formulaBody), 0o644); err != nil {
		t.Fatal(err)
	}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Type: "task", Status: "open"},
	}
	q.childrenOf["BL-1"] = []beads.Bead{
		{ID: "MOL-1", Type: "molecule", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-1")
	opts.OnFormula = "batch-requires-workspace"
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSlingBatch returned %d, want 1; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	errText := stderr.String()
	if !strings.Contains(errText, `variable "workspace" is required`) {
		t.Fatalf("stderr = %q, want missing workspace reported", errText)
	}
	if strings.Contains(errText, "already has attached molecule") || strings.Contains(errText, "cannot use --on") {
		t.Fatalf("stderr = %q, missing required vars should not be masked by attachment conflict", errText)
	}
}

func TestBatchOnConvoyCopiesChildPriorityToCreatedBeads(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	store := beads.NewMemStoreFrom(0, []beads.Bead{
		{ID: "CVY-1", Title: "batch", Type: "convoy", Status: "open"},
		{ID: "BL-1", Title: "original child goal", Type: "task", Status: "open", ParentID: "CVY-1", Priority: priorityPtr(3)},
	}, nil)

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = store
	opts := testOpts(a, "CVY-1")
	opts.OnFormula = "code-review"
	code := doSlingBatch(opts, deps, store, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}

	all, err := deps.Store.ListOpen()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, bead := range all {
		if bead.ID == "CVY-1" {
			continue
		}
		if bead.Priority == nil || *bead.Priority != 3 {
			t.Fatalf("created bead %s priority = %v, want 3", bead.ID, bead.Priority)
		}
	}
}

func TestBatchOnFailFastMolecule(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Status: "open"},
		{ID: "BL-2", Status: "open", Assignee: "other-agent"},
	}
	// BL-2 has an existing molecule child AND is assigned — legitimate, should block.
	q.childrenOf["BL-2"] = []beads.Bead{
		{ID: "MOL-1", Type: "molecule", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-1")
	opts.OnFormula = "code-review"
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSlingBatch returned %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "cannot use --on") {
		t.Errorf("stderr = %q, want '--on' error", stderr.String())
	}
	if !strings.Contains(stderr.String(), "BL-2 (has molecule MOL-1)") {
		t.Errorf("stderr = %q, want BL-2 details", stderr.String())
	}
	// Nothing should be routed — fail-fast.
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0 (fail-fast)", len(runner.calls))
	}
}

func TestBatchAutoBurnStaleMolecules(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-1"] = []beads.Bead{{ID: "BL-1", Status: "open"}, {ID: "BL-2", Status: "open"}}
	q.childrenOf["BL-2"] = []beads.Bead{{ID: "MOL-1", Type: "molecule", Status: "open"}}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "CVY-1", Title: "Batch", Type: "convoy", Status: "open"},
		{ID: "BL-1", Title: "One", Type: "task", Status: "open"},
		{ID: "BL-2", Title: "Two", Type: "task", Status: "open"},
		{ID: "MOL-1", Type: "molecule", Status: "open"},
	}, nil)

	opts := testOpts(a, "CVY-1")
	opts.OnFormula = "code-review"
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0 (auto-burn should unblock); stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "Auto-burned stale molecule MOL-1") {
		t.Errorf("stderr = %q, want auto-burn message", stderr.String())
	}
	assertStoreRoutedTo(t, deps.Store, "BL-1", "mayor")
	assertStoreRoutedTo(t, deps.Store, "BL-2", "mayor")
}

func TestOnFormulaPoolAttachmentKeepsLegacyStepsPrivate(t *testing.T) {
	dir := testFormulaDir(t)
	content := `
formula = "multi-step"
version = 1

[[steps]]
id = "prep"
title = "Prep"

[[steps]]
id = "ship"
title = "Ship"
needs = ["prep"]
`
	if err := os.WriteFile(filepath.Join(dir, "multi-step.toml"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		FormulaLayers: config.FormulaLayers{
			City: []string{dir},
		},
	}
	a := config.Agent{Name: "polecat", Dir: "repo", MinActiveSessions: intPtr(0), MaxActiveSessions: intPtr(5)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(1, []beads.Bead{
		{ID: "BL-42", Title: "Work", Type: "task", Status: "open"},
	}, nil)

	opts := testOpts(a, "BL-42")
	opts.OnFormula = "multi-step"
	code := doSling(opts, deps, deps.Store, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}

	source, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("Get(BL-42): %v", err)
	}
	rootID := source.Metadata["molecule_id"]
	if rootID == "" {
		t.Fatal("source bead missing molecule_id")
	}

	root, err := deps.Store.Get(rootID)
	if err != nil {
		t.Fatalf("Get(%s): %v", rootID, err)
	}
	if root.ParentID != "" {
		t.Fatalf("root ParentID = %q, want empty", root.ParentID)
	}

	all, err := deps.Store.ListOpen()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, bead := range all {
		if bead.ID == "BL-42" || bead.ID == rootID {
			continue
		}
		if bead.Type == "convoy" && bead.Title == "sling-BL-42" {
			continue
		}
		if bead.ParentID == "BL-42" {
			t.Fatalf("internal bead %s ParentID = %q, want not outer bead", bead.ID, bead.ParentID)
		}
		if bead.Ref == "" {
			continue
		}
		if got := bead.Metadata["gc.routed_to"]; got != "" {
			t.Fatalf("internal legacy bead %s gc.routed_to = %q, want empty; attached v1 formulas should route only the source bead", bead.ID, got)
		}
	}
	assertStoreRoutedTo(t, deps.Store, "BL-42", a.QualifiedName())
}

func TestBatchSkipsClosedMolecules(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Status: "open", Assignee: "other-agent"},
	}
	// BL-1 has a closed molecule — should be skipped, not block dispatch.
	q.childrenOf["BL-1"] = []beads.Bead{
		{ID: "MOL-1", Type: "molecule", Status: "closed"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-1")
	opts.OnFormula = "code-review"
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0 (closed molecule should be skipped); stderr: %s", code, stderr.String())
	}
}

func TestBatchOnPartialCookFailure(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-1"] = []beads.Bead{{ID: "BL-1", Status: "open"}, {ID: "BL-2", Status: "open"}, {ID: "BL-3", Status: "open"}}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	createCount := 0
	deps.Store = &selectiveErrStore{
		MemStore: beads.NewMemStoreFrom(1, []beads.Bead{
			{ID: "BL-1", Title: "One", Type: "task", Status: "open"},
			{ID: "BL-2", Title: "Two", Type: "task", Status: "open"},
			{ID: "BL-3", Title: "Three", Type: "task", Status: "open"},
		}, nil),
		failOnCreate: func(b beads.Bead) error {
			if b.Type != "molecule" || b.ParentID != "" {
				return nil
			}
			createCount++
			if createCount == 2 {
				return fmt.Errorf("cook failed for BL-2")
			}
			return nil
		},
	}
	opts := testOpts(a, "CVY-1")
	opts.OnFormula = "code-review"
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSlingBatch returned %d, want 1 (partial failure)", code)
	}
	out := stdout.String()
	if !strings.Contains(out, "Slung BL-1") {
		t.Errorf("stdout = %q, want BL-1 routed", out)
	}
	if !strings.Contains(out, "Slung BL-3") {
		t.Errorf("stdout = %q, want BL-3 routed", out)
	}
	if !strings.Contains(stderr.String(), "Failed BL-2") {
		t.Errorf("stderr = %q, want BL-2 failure", stderr.String())
	}
	if !strings.Contains(out, "Slung 2/3 children") {
		t.Errorf("stdout = %q, want summary", out)
	}
	assertStoreRoutedTo(t, deps.Store, "BL-1", "mayor")
	assertStoreRoutedTo(t, deps.Store, "BL-3", "mayor")
}

func TestBatchOnNudgeOnce(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	sp.Calls = nil
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Status: "open"},
		{ID: "BL-2", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.CityPath = t.TempDir()
	prev := startNudgePoller
	startNudgePoller = func(_, _, _ string) error { return nil }
	t.Cleanup(func() { startNudgePoller = prev })
	opts := testOpts(a, "CVY-1")
	opts.Nudge = true
	opts.OnFormula = "code-review"
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	for _, c := range sp.Calls {
		if c.Method == "Nudge" {
			t.Fatalf("expected queued sling reminder, got direct nudge calls: %+v", sp.Calls)
		}
	}
	pending, _, dead, err := listQueuedNudges(deps.CityPath, "mayor", time.Now())
	if err != nil {
		t.Fatalf("listQueuedNudges: %v", err)
	}
	if len(pending) != 1 || len(dead) != 0 {
		t.Fatalf("pending=%d dead=%d, want 1/0", len(pending), len(dead))
	}
}

func TestBatchOnRegularPassthrough(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["BL-42"] = beads.Bead{ID: "BL-42", Type: "task", Status: "open"}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "code-review"
	// Non-container bead + --on → should fall through to doSling.
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	// MemStore generates IDs like "gc-1".
	if !strings.Contains(out, "Attached wisp gc-1") {
		t.Errorf("stdout = %q, want attach message", out)
	}
	if !strings.Contains(out, "Slung BL-42 (with formula") {
		t.Errorf("stdout = %q, want slung with formula", out)
	}
	if strings.Contains(out, "Expanding") {
		t.Errorf("stdout = %q, should not expand a regular bead", out)
	}
}

// --- Dry-run tests ---

func TestDryRunFlagExists(t *testing.T) {
	cmd := newSlingCmd(&bytes.Buffer{}, &bytes.Buffer{})
	f := cmd.Flags().Lookup("dry-run")
	if f == nil {
		t.Fatal("missing --dry-run flag")
	}
	if f.Shorthand != "n" {
		t.Errorf("--dry-run shorthand = %q, want %q", f.Shorthand, "n")
	}
}

func TestDryRunSingleBead(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
	q := &fakeQuerier{bead: beads.Bead{ID: "BL-42", Title: "Implement login page", Type: "task", Status: "open"}}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("BL-42")
	opts := testOpts(a, "BL-42")
	opts.DryRun = true
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	// Target section.
	if !strings.Contains(out, "Agent:       mayor (non-expanding template)") {
		t.Errorf("stdout missing agent info: %s", out)
	}
	if !strings.Contains(out, "Sling query: bd update {} --set-metadata gc.routed_to=mayor") {
		t.Errorf("stdout missing sling query: %s", out)
	}
	// Work section.
	if !strings.Contains(out, "BL-42") {
		t.Errorf("stdout missing bead ID: %s", out)
	}
	if !strings.Contains(out, "Implement login page") {
		t.Errorf("stdout missing bead title: %s", out)
	}
	// Route command.
	if !strings.Contains(out, "bd update 'BL-42' --set-metadata gc.routed_to=mayor") {
		t.Errorf("stdout missing route command: %s", out)
	}
	// Footer.
	if !strings.Contains(out, "No side effects executed (--dry-run).") {
		t.Errorf("stdout missing footer: %s", out)
	}
	// Zero mutations.
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0 (dry-run): %v", len(runner.calls), runner.calls)
	}
}

func TestDryRunSingleBeadExpandsSlingQuerySummary(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "frontend", Path: "/city/frontend"}},
	}
	a := config.Agent{
		Name:       "worker",
		Dir:        "frontend",
		SlingQuery: "custom-dispatch {} --route={{.Rig}}/{{.AgentBase}} --city={{.CityName}}",
	}
	q := &fakeQuerier{bead: beads.Bead{ID: "FR-42", Title: "Implement login page", Type: "task", Status: "open"}}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("FR-42")
	opts := testOpts(a, "FR-42")
	opts.DryRun = true
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "Sling query: custom-dispatch {} --route=frontend/worker --city=test-city") {
		t.Fatalf("stdout missing expanded sling query: %s", out)
	}
}

func TestDryRunFormula(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "code-review")
	opts.IsFormula = true
	opts.DryRun = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "Formula:") {
		t.Errorf("stdout missing Formula section: %s", out)
	}
	if !strings.Contains(out, "Name: code-review") {
		t.Errorf("stdout missing formula name: %s", out)
	}
	if !strings.Contains(out, "Would run: gc formula cook code-review") {
		t.Errorf("stdout missing cook command: %s", out)
	}
	if !strings.Contains(out, "'<wisp-root>'") {
		t.Errorf("stdout missing wisp-root placeholder: %s", out)
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

func TestDryRunOnFormula(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
	q := newFakeChildQuerier()
	q.beadsByID["BL-42"] = beads.Bead{ID: "BL-42", Type: "task", Status: "open"}
	q.childrenOf["BL-42"] = []beads.Bead{} // no molecule children

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("BL-42")
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "code-review"
	opts.DryRun = true
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "Attach formula:") {
		t.Errorf("stdout missing attach section: %s", out)
	}
	if !strings.Contains(out, "Would run: gc formula cook code-review --attach BL-42") {
		t.Errorf("stdout missing cook command: %s", out)
	}
	if !strings.Contains(out, "Pre-check: BL-42 has no existing molecule/wisp children") {
		t.Errorf("stdout missing pre-check: %s", out)
	}
	if !strings.Contains(out, "bd update 'BL-42' --set-metadata gc.routed_to=mayor") {
		t.Errorf("stdout missing route command: %s", out)
	}
	// code-review (sharedTestFormulaDir) is version=1, not graph.v2: legacy
	// attach deliberately leaves the wisp root unrouted (see the
	// design-intent comment on the finalize() call in slingFormula,
	// internal/sling/sling_core.go, citing #2848 and
	// TestOnFormulaAttachesAndRoutes), so the preview must not claim a
	// second routed bead here. See TestDryRunOnFormulaGraphV2 for the
	// graph.v2 case where the line is expected.
	if strings.Contains(out, "A wisp/workflow root is also cooked and routed to the agent.") {
		t.Errorf("stdout has wisp-root disclosure for a legacy (non-graph.v2) formula attach: %s", out)
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

// writeGraphV2FormulaForDryRunTest writes a minimal graph.v2-contract
// formula file, mirroring internal/sling's writeNamedGraphV2ConvoyFormula
// (unexported there, so duplicated here rather than reused across packages).
func writeGraphV2FormulaForDryRunTest(t *testing.T, dir, name string) {
	t.Helper()
	content := fmt.Sprintf(`
formula = %q
version = 2
contract = "graph.v2"

[[steps]]
id = "step"
title = "Do work"
`, name)
	if err := os.WriteFile(filepath.Join(dir, name+".formula.toml"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDryRunOnFormulaGraphV2(t *testing.T) {
	formulaDir := t.TempDir()
	writeGraphV2FormulaForDryRunTest(t, formulaDir, "graph-work")

	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace:     config.Workspace{Name: "test-city"},
		Daemon:        config.DaemonConfig{FormulaV2: boolPtr(true)},
		FormulaLayers: config.FormulaLayers{City: []string{formulaDir}},
	}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
	q := newFakeChildQuerier()
	q.beadsByID["BL-42"] = beads.Bead{ID: "BL-42", Type: "task", Status: "open"}
	q.childrenOf["BL-42"] = []beads.Bead{} // no molecule children

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("BL-42")
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "graph-work"
	opts.DryRun = true
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "Attach formula:") {
		t.Errorf("stdout missing attach section: %s", out)
	}
	if !strings.Contains(out, "A wisp/workflow root is also cooked and routed to the agent.") {
		t.Errorf("stdout missing wisp-root disclosure for a graph.v2 formula attach: %s", out)
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

func TestDryRunMultiSessionConfig(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{
		Name:              "polecat",
		Dir:               "hw",
		MinActiveSessions: intPtr(1), MaxActiveSessions: intPtr(3),
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("BL-42")
	opts := testOpts(a, "BL-42")
	opts.DryRun = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "Session config: hw/polecat (min=1 max=3)") {
		t.Errorf("stdout missing multi-session config info: %s", out)
	}
	if !strings.Contains(out, "bd update {} --set-metadata gc.routed_to=hw/polecat") {
		t.Errorf("stdout missing sling query: %s", out)
	}
	if !strings.Contains(out, "Multi-session configs share a routed work queue via gc.routed_to") {
		t.Errorf("stdout missing multi-session explanation: %s", out)
	}
	if strings.Contains(out, "Pool agents") || strings.Contains(out, "pool member") {
		t.Errorf("stdout contains stale pool terminology: %s", out)
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

func TestDryRunConvoy(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open", Title: "Sprint 12 tasks"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Title: "Login page", Status: "open"},
		{ID: "BL-2", Title: "Auth backend", Status: "closed"},
		{ID: "BL-3", Title: "Session mgmt", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-1")
	opts.DryRun = true
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	// Container explanation.
	if !strings.Contains(out, "convoy") {
		t.Errorf("stdout missing convoy type: %s", out)
	}
	// Children list.
	if !strings.Contains(out, "Children (3 total, 2 open)") {
		t.Errorf("stdout missing children summary: %s", out)
	}
	if !strings.Contains(out, "BL-1") || !strings.Contains(out, "would route") {
		t.Errorf("stdout missing BL-1 route: %s", out)
	}
	if !strings.Contains(out, "BL-2") {
		t.Errorf("stdout missing BL-2: %s", out)
	}
	if !strings.Contains(out, "skip") {
		t.Errorf("stdout missing skip indicator: %s", out)
	}
	// Route commands.
	if !strings.Contains(out, "bd update 'BL-1' --set-metadata gc.routed_to=mayor") {
		t.Errorf("stdout missing BL-1 route command: %s", out)
	}
	if !strings.Contains(out, "bd update 'BL-3' --set-metadata gc.routed_to=mayor") {
		t.Errorf("stdout missing BL-3 route command: %s", out)
	}
	if strings.Contains(out, "bd update 'BL-2' --set-metadata gc.routed_to=mayor") {
		t.Errorf("stdout should not route closed BL-2: %s", out)
	}
	// Zero mutations.
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

func TestDryRunConvoyUsesTracksMembers(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	store := beads.NewMemStoreFrom(0, []beads.Bead{
		{ID: "CVY-1", Type: "convoy", Status: "open", Title: "Sprint 12 tasks"},
		{ID: "EPIC-1", Type: "epic", Status: "open", Title: "Product work"},
		{ID: "BL-1", Type: "task", Status: "open", Title: "Login page", ParentID: "EPIC-1"},
		{ID: "BL-2", Type: "task", Status: "closed", Title: "Auth backend", ParentID: "EPIC-1"},
	}, []beads.Dep{
		{IssueID: "CVY-1", DependsOnID: "BL-1", Type: "tracks"},
		{IssueID: "CVY-1", DependsOnID: "BL-2", Type: "tracks"},
	})

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = store
	opts := testOpts(a, "CVY-1")
	opts.DryRun = true
	code := doSlingBatch(opts, deps, store, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "Children (2 total, 1 open)") {
		t.Fatalf("stdout missing tracks children summary: %s", out)
	}
	if !strings.Contains(out, "BL-1") || !strings.Contains(out, "would route") {
		t.Errorf("stdout missing tracked open child route preview: %s", out)
	}
	if !strings.Contains(out, "BL-2") || !strings.Contains(out, "skip") {
		t.Errorf("stdout missing tracked closed child skip preview: %s", out)
	}
	if !strings.Contains(out, "bd update 'BL-1' --set-metadata gc.routed_to=mayor") {
		t.Errorf("stdout missing tracked BL-1 route command: %s", out)
	}
	if strings.Contains(out, "bd update 'BL-2' --set-metadata gc.routed_to=mayor") {
		t.Errorf("stdout should not route closed tracked BL-2: %s", out)
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

// TestDryRunLeafTaskViaBatchDispatchOnFormula is a regression test for
// the dry-run mismatch where `gc sling --dry-run <pool> <leaf-task>
// --on <formula>` rendered the batch ("container with zero children")
// preview even though a real run would wrap-and-route the bead itself.
//
// The live CLI dispatches every sling invocation through
// doSlingBatchWithJSON (cmd_sling.go calls it unconditionally from the
// command handler). Inside, sling.DoSling decides single-vs-batch by
// bead type. For non-container types (task, bug, feature, etc.) it
// returns a single-bead result with ContainerType unset. The dry-run
// rendering then has to honor that signal — otherwise it falls into
// the batch preview and reports "Children (0 total, 0 open)" plus an
// empty route list, which is the opposite of what the real run does.
//
// The fix in cmd_sling.go gates the dryRunBatch dispatch on
// result.ContainerType being non-empty.
func TestDryRunLeafTaskViaBatchDispatchOnFormula(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{
		Name:              "polecat",
		Dir:               "hw",
		MinActiveSessions: intPtr(0), MaxActiveSessions: intPtr(3),
	}
	q := newFakeChildQuerier()
	q.beadsByID["BL-42"] = beads.Bead{ID: "BL-42", Type: "task", Status: "open", Title: "Implement login page"}
	q.childrenOf["BL-42"] = []beads.Bead{} // no molecule children

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("BL-42")
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "code-review"
	opts.DryRun = true
	// Mirror the live CLI: every sling invocation goes through
	// doSlingBatchWithJSON; the function decides single-vs-batch
	// internally from the bead type.
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	// Must render the single-bead preview, NOT the batch container preview.
	if strings.Contains(out, "Children (0 total, 0 open)") {
		t.Errorf("stdout rendered misleading container preview for a leaf task; got: %s", out)
	}
	if strings.Contains(out, "is a container bead that groups related work") {
		t.Errorf("stdout claimed leaf task is a container; got: %s", out)
	}
	if strings.Contains(out, "Attach formula (per open child):") {
		t.Errorf("stdout rendered per-child attach section for a leaf task; got: %s", out)
	}
	// Should render the single-bead attach + route preview.
	if !strings.Contains(out, "Attach formula:") {
		t.Errorf("stdout missing single-bead attach section: %s", out)
	}
	if !strings.Contains(out, "Would run: gc formula cook code-review --attach BL-42") {
		t.Errorf("stdout missing cook command: %s", out)
	}
	if !strings.Contains(out, "bd update 'BL-42' --set-metadata gc.routed_to=hw/polecat") {
		t.Errorf("stdout missing route command: %s", out)
	}
	if !strings.Contains(out, "No side effects executed (--dry-run).") {
		t.Errorf("stdout missing footer: %s", out)
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0 (dry-run): %v", len(runner.calls), runner.calls)
	}
}

func TestDryRunBatchOnFormula(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Status: "open"},
		{ID: "BL-2", Status: "closed"},
		{ID: "BL-3", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-1")
	opts.OnFormula = "code-review"
	opts.DryRun = true
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	// Per-child cook commands.
	if !strings.Contains(out, "gc formula cook code-review --attach BL-1") {
		t.Errorf("stdout missing BL-1 cook command: %s", out)
	}
	if !strings.Contains(out, "gc formula cook code-review --attach BL-3") {
		t.Errorf("stdout missing BL-3 cook command: %s", out)
	}
	if strings.Contains(out, "gc formula cook code-review --attach BL-2") {
		t.Errorf("stdout should not cook for closed BL-2: %s", out)
	}
	// Route commands.
	if !strings.Contains(out, "bd update 'BL-1' --set-metadata gc.routed_to=mayor") {
		t.Errorf("stdout missing BL-1 route: %s", out)
	}
	if !strings.Contains(out, "bd update 'BL-3' --set-metadata gc.routed_to=mayor") {
		t.Errorf("stdout missing BL-3 route: %s", out)
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

func TestDryRunNudgeRunning(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	sp.Calls = nil
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("BL-1")
	opts := testOpts(a, "BL-1")
	opts.Nudge = true
	opts.DryRun = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0", code)
	}
	out := stdout.String()
	if !strings.Contains(out, "Nudge:") {
		t.Errorf("stdout missing Nudge section: %s", out)
	}
	if !strings.Contains(out, "running") {
		t.Errorf("stdout missing running status: %s", out)
	}
	// No actual nudge should have been sent.
	for _, c := range sp.Calls {
		if c.Method == "Nudge" {
			t.Error("dry-run should not send an actual nudge")
		}
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

func TestDryRunNudgeNotRunning(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("BL-1")
	opts := testOpts(a, "BL-1")
	opts.Nudge = true
	opts.DryRun = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "no running session") {
		t.Errorf("stdout missing 'no running session': %s", out)
	}
}

func TestDryRunNoMutations(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("BL-42")
	opts := testOpts(a, "BL-42")
	opts.DryRun = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0", code)
	}
	if len(runner.calls) != 0 {
		t.Errorf("dry-run executed %d commands, want 0: %v", len(runner.calls), runner.calls)
	}
}

func TestDryRunSuspendedWarning(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", Suspended: true, MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("BL-1")
	opts := testOpts(a, "BL-1")
	opts.DryRun = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0", code)
	}
	// Suspended warning should still fire to stderr.
	if !strings.Contains(stderr.String(), "suspended") {
		t.Errorf("stderr = %q, want suspended warning", stderr.String())
	}
	// But no mutations.
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

func TestDryRunOnExistingMolecule(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["BL-42"] = beads.Bead{ID: "BL-42", Type: "task", Status: "open"}
	q.childrenOf["BL-42"] = []beads.Bead{
		{ID: "MOL-1", Type: "molecule", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("BL-42")
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "code-review"
	opts.DryRun = true
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 1 {
		t.Fatalf("dry-run returned %d, want 1 (existing molecule)", code)
	}
	if !strings.Contains(stderr.String(), "already has attached molecule MOL-1") {
		t.Errorf("stderr = %q, want molecule error", stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

func TestDryRunNilQuerier(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("BL-42")
	opts := testOpts(a, "BL-42")
	opts.DryRun = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	// Should still show bead ID even without querier details.
	if !strings.Contains(out, "BL-42") {
		t.Errorf("stdout missing bead ID: %s", out)
	}
	if !strings.Contains(out, "No side effects executed (--dry-run).") {
		t.Errorf("stdout missing footer: %s", out)
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

// --- Idempotency detection (checkBeadState + integration) tests ---

func TestCheckBeadStateIdempotentFixedAgent(t *testing.T) {
	q := &fakeQuerier{
		bead:   beads.Bead{ID: "BL-42", ParentID: "CVY-1", Metadata: map[string]string{"gc.routed_to": "mayor"}},
		parent: &beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"},
	}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	result := checkBeadState(q, "BL-42", a)
	if !result.Idempotent {
		t.Error("expected Idempotent=true for matching gc.routed_to")
	}
	if len(result.Warnings) != 0 {
		t.Errorf("expected no warnings, got %v", result.Warnings)
	}
}

func TestCheckBeadStateIdempotentPool(t *testing.T) {
	q := &fakeQuerier{
		bead:   beads.Bead{ID: "BL-42", ParentID: "CVY-1", Metadata: map[string]string{"gc.routed_to": "hw/polecat"}},
		parent: &beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"},
	}
	a := config.Agent{Name: "polecat", Dir: "hw", MinActiveSessions: intPtr(1), MaxActiveSessions: intPtr(3)}

	result := checkBeadState(q, "BL-42", a)
	if !result.Idempotent {
		t.Error("expected Idempotent=true for matching gc.routed_to")
	}
	if len(result.Warnings) != 0 {
		t.Errorf("expected no warnings, got %v", result.Warnings)
	}
}

func TestCheckBeadStateIdempotentPoolMultiLabels(t *testing.T) {
	q := &fakeQuerier{
		bead: beads.Bead{
			ID:       "BL-42",
			ParentID: "CVY-1",
			Labels:   []string{"priority:high", "sprint:3"},
			Metadata: map[string]string{"gc.routed_to": "hw/polecat"},
		},
		parent: &beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"},
	}
	a := config.Agent{Name: "polecat", Dir: "hw", MinActiveSessions: intPtr(1), MaxActiveSessions: intPtr(3)}

	result := checkBeadState(q, "BL-42", a)
	if !result.Idempotent {
		t.Error("expected Idempotent=true for matching gc.routed_to among other labels")
	}
}

func TestCheckBeadStateCustomQueryNoIdempotency(t *testing.T) {
	q := &fakeQuerier{bead: beads.Bead{ID: "BL-42", Assignee: "mayor"}}
	a := config.Agent{Name: "mayor", SlingQuery: "custom-script {} --route"}

	result := checkBeadState(q, "BL-42", a)
	if result.Idempotent {
		t.Error("expected Idempotent=false for custom sling_query (can't detect)")
	}
	if len(result.Warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(result.Warnings), result.Warnings)
	}
	if !strings.Contains(result.Warnings[0], "already assigned") {
		t.Errorf("expected assignee warning, got %q", result.Warnings[0])
	}
}

func TestCheckBeadStateDifferentAssignee(t *testing.T) {
	q := &fakeQuerier{bead: beads.Bead{ID: "BL-42", Assignee: "other-agent"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	result := checkBeadState(q, "BL-42", a)
	if result.Idempotent {
		t.Error("expected Idempotent=false for different assignee")
	}
	if len(result.Warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(result.Warnings), result.Warnings)
	}
	if !strings.Contains(result.Warnings[0], "already assigned to \"other-agent\"") {
		t.Errorf("expected assignee warning, got %q", result.Warnings[0])
	}
}

func TestCheckBeadStateDifferentPoolLabel(t *testing.T) {
	q := &fakeQuerier{bead: beads.Bead{ID: "BL-42", Labels: []string{"pool:other/pool"}}}
	a := config.Agent{Name: "polecat", Dir: "hw", MinActiveSessions: intPtr(1), MaxActiveSessions: intPtr(3)}

	result := checkBeadState(q, "BL-42", a)
	if result.Idempotent {
		t.Error("expected Idempotent=false for different pool label")
	}
	if len(result.Warnings) != 1 {
		t.Fatalf("expected 1 warning, got %d: %v", len(result.Warnings), result.Warnings)
	}
	if !strings.Contains(result.Warnings[0], "pool:other/pool") {
		t.Errorf("expected pool label warning, got %q", result.Warnings[0])
	}
}

func TestDoSlingIdempotentSkipsRouting(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
	q := &fakeQuerier{
		bead:   beads.Bead{ID: "BL-42", ParentID: "CVY-1", Metadata: map[string]string{"gc.routed_to": "mayor"}},
		parent: &beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0", code)
	}
	if len(runner.calls) != 0 {
		t.Errorf("idempotent bead should not be routed; got %d calls: %v", len(runner.calls), runner.calls)
	}
	if !strings.Contains(stdout.String(), "already routed to mayor") {
		t.Errorf("stdout = %q, want idempotent skip message", stdout.String())
	}
	if !strings.Contains(stdout.String(), "skipping (idempotent)") {
		t.Errorf("stdout = %q, want idempotent label", stdout.String())
	}
}

// TestDoSlingRecoversMissingConvoyOnPreRoutedBead covers the case where a
// bead has gc.routed_to set (e.g., declared via bd create --metadata) but
// no auto-convoy membership — a prior sling never finished, or the route came
// from outside gc sling. A subsequent sling must re-run finalize() to create
// the auto-convoy tracking dependency and poke the controller, instead of
// skipping as idempotent and leaving the work orphaned.
func TestDoSlingRecoversMissingConvoyOnPreRoutedBead(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	// Override the package-level poke hook so we can count controller pokes
	// without spinning up IPC.
	var pokeCount int
	oldPoke := slingPokeController
	slingPokeController = func(string) error {
		pokeCount++
		return nil
	}
	t.Cleanup(func() { slingPokeController = oldPoke })

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	existingConvoy, err := deps.Store.Create(beads.Bead{Title: "existing convoy", Type: "convoy", Status: "open"})
	if err != nil {
		t.Fatalf("seed existing convoy: %v", err)
	}
	// Pre-set gc.routed_to on the bead via the store, without ever creating
	// a convoy. Mirrors `bd create --metadata '{"gc.routed_to":"mayor"}'`.
	if err := deps.Store.SetMetadata("BL-42", "gc.routed_to", "mayor"); err != nil {
		t.Fatalf("seed routed_to: %v", err)
	}

	opts := testOpts(a, "BL-42")
	code := doSling(opts, deps, deps.Store, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}

	bead, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("store.Get(BL-42): %v", err)
	}
	if got := bead.Metadata["gc.routed_to"]; got != "mayor" {
		t.Errorf("gc.routed_to = %q, want %q (should be unchanged)", got, "mayor")
	}
	if bead.ParentID != "" {
		t.Fatalf("ParentID = %q, want empty because auto-convoy membership uses tracks deps", bead.ParentID)
	}
	trackDeps, err := deps.Store.DepList(bead.ID, "up")
	if err != nil {
		t.Fatalf("DepList(%s, up): %v", bead.ID, err)
	}
	var recoveredConvoyID string
	for _, dep := range trackDeps {
		if dep.Type == "tracks" && dep.DependsOnID == bead.ID {
			recoveredConvoyID = dep.IssueID
		}
	}
	if recoveredConvoyID == "" {
		t.Fatalf("expected recovered bead to have a tracks dependency, got deps=%v", trackDeps)
	}
	if recoveredConvoyID == existingConvoy.ID {
		t.Fatalf("recovered convoy = pre-existing convoy %s, want a fresh convoy", existingConvoy.ID)
	}
	parent, err := deps.Store.Get(recoveredConvoyID)
	if err != nil {
		t.Fatalf("store.Get(%s): %v", recoveredConvoyID, err)
	}
	if parent.Type != "convoy" {
		t.Errorf("parent.Type = %q, want %q", parent.Type, "convoy")
	}
	if parent.Status == "closed" {
		t.Errorf("parent convoy should not be closed immediately after recovery")
	}
	if pokeCount < 1 {
		t.Errorf("expected slingPokeController to fire at least once, got %d", pokeCount)
	}
	if !strings.Contains(stdout.String(), "Auto-convoy") {
		t.Errorf("stdout should mention Auto-convoy: %s", stdout.String())
	}
	if strings.Contains(stdout.String(), "skipping (idempotent)") {
		t.Errorf("stdout should not report idempotent skip: %s", stdout.String())
	}
}

func TestDoSlingNoConvoyRepeatIsIdempotent(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	var pokeCount int
	oldPoke := slingPokeController
	slingPokeController = func(string) error {
		pokeCount++
		return nil
	}
	t.Cleanup(func() { slingPokeController = oldPoke })

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("BL-42")

	opts := testOpts(a, "BL-42")
	opts.NoConvoy = true
	code := doSling(opts, deps, deps.Store, stdout, stderr)
	if code != 0 {
		t.Fatalf("first doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	first, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("store.Get(BL-42): %v", err)
	}
	if first.ParentID != "" {
		t.Fatalf("first no-convoy sling set ParentID = %q, want empty", first.ParentID)
	}
	if pokeCount != 1 {
		t.Fatalf("first no-convoy sling poke count = %d, want 1", pokeCount)
	}

	stdout.Reset()
	stderr.Reset()
	code = doSling(opts, deps, deps.Store, stdout, stderr)
	if code != 0 {
		t.Fatalf("second doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	second, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("store.Get(BL-42) after second sling: %v", err)
	}
	if second.ParentID != "" {
		t.Fatalf("second no-convoy sling set ParentID = %q, want empty", second.ParentID)
	}
	if pokeCount != 1 {
		t.Fatalf("second no-convoy sling poked controller; count = %d, want 1", pokeCount)
	}
	if !strings.Contains(stdout.String(), "skipping (idempotent)") {
		t.Fatalf("stdout = %q, want idempotent skip", stdout.String())
	}

	all, err := deps.Store.List(beads.ListQuery{AllowScan: true})
	if err != nil {
		t.Fatalf("list beads: %v", err)
	}
	for _, b := range all {
		if b.Type == "convoy" {
			t.Fatalf("unexpected convoy bead after no-convoy repeat: %+v", b)
		}
	}
}

func TestDoSlingKeepsWorkflowParentIdempotent(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	var pokeCount int
	oldPoke := slingPokeController
	slingPokeController = func(string) error {
		pokeCount++
		return nil
	}
	t.Cleanup(func() { slingPokeController = oldPoke })

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(0, []beads.Bead{
		{ID: "WF-1", Title: "workflow", Type: "workflow", Status: "in_progress", Metadata: map[string]string{"gc.kind": "workflow"}},
		{
			ID:       "BL-42",
			Title:    "workflow step",
			Type:     "task",
			Status:   "open",
			ParentID: "WF-1",
			Metadata: map[string]string{"gc.routed_to": "mayor"},
		},
	}, nil)

	opts := testOpts(a, "BL-42")
	code := doSling(opts, deps, deps.Store, stdout, stderr)
	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	bead, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("store.Get(BL-42): %v", err)
	}
	if bead.ParentID != "WF-1" {
		t.Fatalf("ParentID = %q, want original workflow parent WF-1", bead.ParentID)
	}
	if pokeCount != 0 {
		t.Fatalf("idempotent workflow parent sling poked controller; count = %d, want 0", pokeCount)
	}
	if !strings.Contains(stdout.String(), "skipping (idempotent)") {
		t.Fatalf("stdout = %q, want idempotent skip", stdout.String())
	}
}

func TestDoSlingIdempotentForceOverrides(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
	q := &fakeQuerier{bead: beads.Bead{ID: "BL-42", Assignee: "mayor"}}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	opts.Force = true
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0", code)
	}
	// --force should bypass idempotency and route.
	if len(runner.calls) != 0 {
		t.Errorf("--force should not shell out for built-in routing; got %d calls", len(runner.calls))
	}
	bead, err := deps.Store.Get("BL-42")
	if err != nil {
		t.Fatalf("store.Get(BL-42): %v", err)
	}
	if bead.Metadata["gc.routed_to"] == "" {
		t.Error("expected gc.routed_to to be set during forced routing")
	}
	if strings.Contains(stdout.String(), "idempotent") {
		t.Errorf("--force should not print idempotent message; stdout = %q", stdout.String())
	}
}

func TestDoSlingIdempotentWithOnFormula(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
	// Bead is already assigned to mayor with a live convoy parent — idempotent.
	q := &fakeQuerier{
		bead:   beads.Bead{ID: "BL-42", ParentID: "CVY-1", Assignee: "mayor"},
		parent: &beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "BL-42")
	opts.OnFormula = "my-formula"
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	// Idempotent — should skip both wisp attachment and routing.
	if len(runner.calls) != 0 {
		t.Errorf("idempotent + --on should skip all mutations; got %d calls: %v", len(runner.calls), runner.calls)
	}
	if !strings.Contains(stdout.String(), "skipping (idempotent)") {
		t.Errorf("stdout = %q, want idempotent skip message", stdout.String())
	}
}

func TestDoSlingBatchIdempotentChildSkipped(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.beadsByID["BL-1"] = beads.Bead{ID: "BL-1", Status: "open", Assignee: "mayor", ParentID: "CVY-1"}
	q.beadsByID["BL-2"] = beads.Bead{ID: "BL-2", Status: "open", ParentID: "CVY-1"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Status: "open", Assignee: "mayor", ParentID: "CVY-1"},
		{ID: "BL-2", Status: "open", ParentID: "CVY-1"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-1")
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for built-in routing: %v", len(runner.calls), runner.calls)
	}
	assertStoreRoutedTo(t, deps.Store, "BL-2", "mayor")
	out := stdout.String()
	if !strings.Contains(out, "Skipped BL-1") {
		t.Errorf("stdout should mention skipped BL-1: %s", out)
	}
	if !strings.Contains(out, "already routed to mayor") {
		t.Errorf("stdout should mention idempotent skip: %s", out)
	}
	if !strings.Contains(out, "Slung 1/2 children") {
		t.Errorf("stdout summary should show 1/2 routed: %s", out)
	}
	if !strings.Contains(out, "(1 already routed)") {
		t.Errorf("stdout summary should mention idempotent count: %s", out)
	}
}

func TestDoSlingBatchAllIdempotent(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.beadsByID["BL-1"] = beads.Bead{ID: "BL-1", Status: "open", Assignee: "mayor", ParentID: "CVY-1"}
	q.beadsByID["BL-2"] = beads.Bead{ID: "BL-2", Status: "open", Assignee: "mayor", ParentID: "CVY-1"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Status: "open", Assignee: "mayor", ParentID: "CVY-1"},
		{ID: "BL-2", Status: "open", Assignee: "mayor", ParentID: "CVY-1"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-1")
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Errorf("all idempotent: got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
	out := stdout.String()
	if !strings.Contains(out, "Slung 0/2 children") {
		t.Errorf("stdout summary should show 0/2 routed: %s", out)
	}
	if !strings.Contains(out, "(2 already routed)") {
		t.Errorf("stdout summary should mention both idempotent: %s", out)
	}
}

func TestDryRunIdempotentBead(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
	q := &fakeQuerier{
		bead:   beads.Bead{ID: "BL-42", Title: "Login page", ParentID: "CVY-1", Assignee: "mayor", Status: "open"},
		parent: &beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("BL-42")
	opts := testOpts(a, "BL-42")
	opts.DryRun = true
	code := doSling(opts, deps, q, stdout, stderr)

	// Dry-run reaches the full preview — including the Idempotency section.
	if code != 0 {
		t.Fatalf("returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	// Should show idempotency section.
	if !strings.Contains(out, "Idempotency:") {
		t.Errorf("stdout missing Idempotency section: %s", out)
	}
	if !strings.Contains(out, "already routed to mayor") {
		t.Errorf("stdout should show idempotent info: %s", out)
	}
	// Should show the footer (proving dryRunSingle was reached).
	if !strings.Contains(out, "No side effects executed (--dry-run).") {
		t.Errorf("stdout missing footer: %s", out)
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

// --- Cross-rig guard tests ---

func TestRigPrefixForAgentCityWide(t *testing.T) {
	cfg := &config.City{
		Rigs: []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "mayor", Dir: ""} // city-wide

	got := rigPrefixForAgent(a, cfg)
	if got != "" {
		t.Errorf("rigPrefixForAgent(city-wide) = %q, want empty (exempt)", got)
	}
}

func TestRigPrefixForAgentRigScoped(t *testing.T) {
	cfg := &config.City{
		Rigs: []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "polecat", Dir: "hello-world"}

	got := rigPrefixForAgent(a, cfg)
	// DeriveBeadsPrefix("hello-world") = "hw"
	if got != "hw" {
		t.Errorf("rigPrefixForAgent(rig-scoped) = %q, want %q", got, "hw")
	}
}

func TestRigPrefixForAgentExplicitPrefix(t *testing.T) {
	cfg := &config.City{
		Rigs: []config.Rig{{Name: "hello-world", Path: "/tmp/hw", Prefix: "HELLO"}},
	}
	a := config.Agent{Name: "polecat", Dir: "hello-world"}

	got := rigPrefixForAgent(a, cfg)
	if got != "hello" {
		t.Errorf("rigPrefixForAgent(explicit prefix) = %q, want %q", got, "hello")
	}
}

func TestRigPrefixForAgentOrphanDir(t *testing.T) {
	cfg := &config.City{
		Rigs: []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "polecat", Dir: "nonexistent"}

	got := rigPrefixForAgent(a, cfg)
	if got != "" {
		t.Errorf("rigPrefixForAgent(orphan dir) = %q, want empty (best-effort skip)", got)
	}
}

func TestCheckCrossRigSameRig(t *testing.T) {
	cfg := &config.City{
		Rigs: []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "polecat", Dir: "hello-world"}

	msg := checkCrossRig("HW-7", a, cfg)
	if msg != "" {
		t.Errorf("checkCrossRig(same rig) = %q, want empty", msg)
	}
}

func TestCheckCrossRigDifferentRig(t *testing.T) {
	cfg := &config.City{
		Rigs: []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "polecat", Dir: "hello-world"}

	msg := checkCrossRig("FE-123", a, cfg)
	if msg == "" {
		t.Fatal("checkCrossRig(different rig) = empty, want error message")
	}
	if !strings.Contains(msg, "cross-rig") {
		t.Errorf("message = %q, want 'cross-rig'", msg)
	}
	if !strings.Contains(msg, `prefix "fe"`) {
		t.Errorf("message = %q, want bead prefix", msg)
	}
	if !strings.Contains(msg, `rig prefix "hw"`) {
		t.Errorf("message = %q, want rig prefix", msg)
	}
	if !strings.Contains(msg, "--force") {
		t.Errorf("message = %q, want --force hint", msg)
	}
}

func TestCheckCrossRigCityAgent(t *testing.T) {
	cfg := &config.City{
		Rigs: []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "mayor", Dir: ""} // city-wide

	msg := checkCrossRig("FE-123", a, cfg)
	if msg != "" {
		t.Errorf("checkCrossRig(city-wide agent) = %q, want empty (exempt)", msg)
	}
}

func TestDoSlingCrossRigBlocks(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "polecat", Dir: "hello-world"}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "FE-123")
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSling returned %d, want 1 (cross-rig block)", code)
	}
	if !strings.Contains(stderr.String(), "cross-rig") {
		t.Errorf("stderr = %q, want cross-rig error", stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0 (should not route)", len(runner.calls))
	}
}

func TestDoSlingCrossRigForceOverrides(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "polecat", Dir: "hello-world"}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.StoreRef = "rig:hello-world"
	opts := testOpts(a, "FE-123")
	opts.Force = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0 (--force overrides cross-rig); stderr: %s", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "cross-rig") {
		t.Errorf("--force should suppress cross-rig block; stderr = %q", stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0 for built-in routing", len(runner.calls))
	}
	assertStoreRoutedTo(t, deps.Store, "FE-123", "hello-world/polecat")
}

func TestDoSlingCrossRigSameRigAllowed(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "polecat", Dir: "hello-world"}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.StoreRef = "rig:hello-world"
	opts := testOpts(a, "HW-7")
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0 (same rig); stderr: %s", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "cross-rig") {
		t.Errorf("same-rig bead should not trigger cross-rig block; stderr = %q", stderr.String())
	}
}

func TestDoSlingBatchCrossRigBlocks(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "polecat", Dir: "hello-world"}

	q := newFakeChildQuerier()
	q.beadsByID["FE-1"] = beads.Bead{ID: "FE-1", Type: "convoy", Status: "open"}
	q.childrenOf["FE-1"] = []beads.Bead{
		{ID: "FE-2", Status: "open"},
		{ID: "FE-3", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "FE-1")
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSlingBatch returned %d, want 1 (cross-rig block)", code)
	}
	if !strings.Contains(stderr.String(), "cross-rig") {
		t.Errorf("stderr = %q, want cross-rig error", stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0 (should not route)", len(runner.calls))
	}
}

func TestDryRunCrossRigSection(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "polecat", Dir: "hello-world"}
	q := &fakeQuerier{bead: beads.Bead{ID: "FE-123", Type: "task", Status: "open"}}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("FE-123")
	opts := testOpts(a, "FE-123")
	opts.DryRun = true
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "Cross-rig:") {
		t.Errorf("stdout missing Cross-rig section: %s", out)
	}
	if !strings.Contains(out, `prefix "fe"`) {
		t.Errorf("stdout missing bead prefix: %s", out)
	}
	if !strings.Contains(out, `rig prefix "hw"`) {
		t.Errorf("stdout missing rig prefix: %s", out)
	}
	if !strings.Contains(out, "--force") {
		t.Errorf("stdout missing --force hint: %s", out)
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

func TestDryRunBatchCrossRigSection(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "polecat", Dir: "hello-world"}

	q := newFakeChildQuerier()
	q.beadsByID["FE-1"] = beads.Bead{ID: "FE-1", Type: "convoy", Status: "open"}
	q.childrenOf["FE-1"] = []beads.Bead{
		{ID: "FE-2", Status: "open"},
		{ID: "FE-3", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "FE-1")
	opts.DryRun = true
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("dry-run returned %d, want 0; stderr: %s", code, stderr.String())
	}
	out := stdout.String()
	if !strings.Contains(out, "Cross-rig:") {
		t.Errorf("stdout missing Cross-rig section: %s", out)
	}
	if !strings.Contains(out, `prefix "fe"`) {
		t.Errorf("stdout missing bead prefix: %s", out)
	}
	if !strings.Contains(out, `rig prefix "hw"`) {
		t.Errorf("stdout missing rig prefix: %s", out)
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0: %v", len(runner.calls), runner.calls)
	}
}

func TestDoSlingCrossRigFormulaExempt(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "polecat", Dir: "hello-world", MaxActiveSessions: intPtr(1)}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.StoreRef = "rig:hello-world"
	opts := testOpts(a, "code-review")
	opts.IsFormula = true
	// Formula mode — cross-rig check should not apply.
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0 (formula exempt from cross-rig); stderr: %s", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "cross-rig") {
		t.Errorf("formula mode should not trigger cross-rig; stderr = %q", stderr.String())
	}
}

// --- New tests for shell quoting, helpers, and edge cases ---

func TestShellQuote(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "'simple'"},
		{"it's", "'it'\\''s'"},
		{"a b c", "'a b c'"},
		{"", "''"},
		{"$(rm -rf /)", "'$(rm -rf /)'"},
		{"`evil`", "'`evil`'"},
		{"hello'world'end", "'hello'\\''world'\\''end'"},
	}
	for _, tt := range tests {
		got := shellquote.Quote(tt.input)
		if got != tt.want {
			t.Errorf("shellquote.Quote(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestFormatBeadLabel(t *testing.T) {
	if got := formatBeadLabel("BL-42", ""); got != "BL-42" {
		t.Errorf("formatBeadLabel(no title) = %q, want %q", got, "BL-42")
	}
	if got := formatBeadLabel("BL-42", "Login page"); !strings.Contains(got, "BL-42") || !strings.Contains(got, "Login page") {
		t.Errorf("formatBeadLabel(with title) = %q, want BL-42 and title", got)
	}
}

func TestDoSlingOnFormulaCrossRigBlocked(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "polecat", Dir: "hello-world"}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "FE-123")
	opts.OnFormula = "code-review"
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSling returned %d, want 1 (cross-rig block with --on)", code)
	}
	if !strings.Contains(stderr.String(), "cross-rig") {
		t.Errorf("stderr = %q, want cross-rig error", stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Errorf("got %d runner calls, want 0", len(runner.calls))
	}
}

func TestDoSlingOnFormulaCrossRigForceOverrides(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs:      []config.Rig{{Name: "hello-world", Path: "/tmp/hw"}},
	}
	a := config.Agent{Name: "polecat", Dir: "hello-world"}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.StoreRef = "rig:hello-world"
	opts := testOpts(a, "FE-123")
	opts.OnFormula = "code-review"
	opts.Force = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "cross-rig") {
		t.Errorf("--force should suppress cross-rig; stderr = %q", stderr.String())
	}
}

func TestDoSlingBatchAllIdempotentNoNudge(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	_ = sp.Start(context.Background(), "mayor", runtime.Config{})
	sp.Calls = nil
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	q := newFakeChildQuerier()
	q.beadsByID["CVY-1"] = beads.Bead{ID: "CVY-1", Type: "convoy", Status: "open"}
	q.beadsByID["BL-1"] = beads.Bead{ID: "BL-1", Status: "open", Assignee: "mayor", ParentID: "CVY-1"}
	q.beadsByID["BL-2"] = beads.Bead{ID: "BL-2", Status: "open", Assignee: "mayor", ParentID: "CVY-1"}
	q.childrenOf["CVY-1"] = []beads.Bead{
		{ID: "BL-1", Status: "open", Assignee: "mayor", ParentID: "CVY-1"},
		{ID: "BL-2", Status: "open", Assignee: "mayor", ParentID: "CVY-1"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "CVY-1")
	opts.Nudge = true
	code := doSlingBatch(opts, deps, q, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	// All idempotent → 0 routed → no nudge should fire.
	for _, c := range sp.Calls {
		if c.Method == "Nudge" {
			t.Error("all-idempotent batch should not nudge")
		}
	}
}

// --- Default sling formula tests ---

func TestDefaultFormulaApplied(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "polecat", Dir: "hw", DefaultSlingFormula: strPtr("mol-polecat-work")}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(1, []beads.Bead{{ID: "HW-42", Title: "Work", Type: "task", Status: "open"}}, nil)
	opts := testOpts(a, "HW-42")
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for built-in routing: %v", len(runner.calls), runner.calls)
	}
	assertStoreRoutedTo(t, deps.Store, "HW-42", "hw/polecat")
	source, err := deps.Store.Get("HW-42")
	if err != nil {
		t.Fatalf("store.Get(HW-42): %v", err)
	}
	rootID := source.Metadata["molecule_id"]
	if rootID == "" {
		t.Fatal("source bead missing molecule_id")
	}
	b, err := deps.Store.Get(rootID)
	if err != nil {
		t.Fatalf("store.Get(%s): %v", rootID, err)
	}
	if b.Ref != "mol-polecat-work" {
		t.Errorf("bead Ref = %q, want %q", b.Ref, "mol-polecat-work")
	}
	if b.ParentID != "" {
		t.Errorf("bead ParentID = %q, want empty", b.ParentID)
	}
	if !strings.Contains(stdout.String(), "default formula") {
		t.Errorf("stdout = %q, want mention of default formula", stdout.String())
	}
}

func TestDefaultFormulaAppliedFromInheritedPackDefault(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "polecat", Dir: "hw", InheritedDefaultSlingFormula: strPtr("mol-polecat-work")}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = beads.NewMemStoreFrom(1, []beads.Bead{{ID: "HW-42", Title: "Work", Type: "task", Status: "open"}}, nil)
	opts := testOpts(a, "HW-42")
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for built-in routing: %v", len(runner.calls), runner.calls)
	}
	assertStoreRoutedTo(t, deps.Store, "HW-42", "hw/polecat")
	source, err := deps.Store.Get("HW-42")
	if err != nil {
		t.Fatalf("store.Get(HW-42): %v", err)
	}
	rootID := source.Metadata["molecule_id"]
	if rootID == "" {
		t.Fatal("source bead missing molecule_id")
	}
	b, err := deps.Store.Get(rootID)
	if err != nil {
		t.Fatalf("store.Get(%s): %v", rootID, err)
	}
	if b.Ref != "mol-polecat-work" {
		t.Errorf("bead Ref = %q, want %q", b.Ref, "mol-polecat-work")
	}
	if !strings.Contains(stdout.String(), "default formula") {
		t.Errorf("stdout = %q, want mention of default formula", stdout.String())
	}
}

func TestDefaultFormulaNoFormulaOverride(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "polecat", Dir: "hw", DefaultSlingFormula: strPtr("mol-polecat-work")}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "HW-42")
	opts.NoFormula = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for built-in routing: %v", len(runner.calls), runner.calls)
	}
	assertStoreRoutedTo(t, deps.Store, "HW-42", "hw/polecat")
}

func TestDefaultFormulaMissingRequiredVarsBeforeExistingMolecule(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}

	dir := testFormulaDir(t)
	cfg.FormulaLayers.City = []string{dir}
	formulaBody := `
formula = "default-requires-workspace"
version = 1

[vars.workspace]
description = "Workspace path"
required = true

[[steps]]
id = "do-work"
title = "Work in {{workspace}}"
`
	if err := os.WriteFile(filepath.Join(dir, "default-requires-workspace.toml"), []byte(formulaBody), 0o644); err != nil {
		t.Fatal(err)
	}

	a := config.Agent{Name: "agent-two", Dir: "hw", DefaultSlingFormula: strPtr("default-requires-workspace")}
	q := newFakeChildQuerier()
	q.beadsByID["HW-42"] = beads.Bead{ID: "HW-42", Type: "task", Status: "open", Assignee: "hw/agent-two"}
	q.childrenOf["HW-42"] = []beads.Bead{
		{ID: "MOL-1", Type: "molecule", Status: "open"},
	}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "HW-42")
	code := doSling(opts, deps, q, stdout, stderr)

	if code != 1 {
		t.Fatalf("doSling returned %d, want 1; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	errText := stderr.String()
	if !strings.Contains(errText, `variable "workspace" is required`) {
		t.Fatalf("stderr = %q, want missing workspace reported", errText)
	}
	if strings.Contains(errText, "already has attached molecule") {
		t.Fatalf("stderr = %q, missing required vars should not be masked by attachment conflict", errText)
	}
}

func TestDefaultFormulaExplicitOnOverrides(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "polecat", Dir: "hw", DefaultSlingFormula: strPtr("mol-polecat-work")}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "HW-42")
	opts.OnFormula = "custom-formula"
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	// MolCookOn goes through the store; verify the explicit formula was used.
	b, err := deps.Store.Get("gc-1")
	if err != nil {
		t.Fatalf("store.Get(gc-1): %v", err)
	}
	if b.Ref != "custom-formula" {
		t.Errorf("bead Ref = %q, want explicit custom-formula", b.Ref)
	}
	// Output should mention explicit formula, not default.
	if strings.Contains(stdout.String(), "default formula") {
		t.Errorf("stdout should not mention default formula when --on is explicit: %q", stdout.String())
	}
}

func TestDefaultFormulaExplicitFormulaOverrides(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	dir := testFormulaDir(t)
	if err := os.WriteFile(filepath.Join(dir, "explicit-root-only.toml"), []byte(`
formula = "explicit-root-only"
version = 1
phase = "vapor"

[[steps]]
id = "work"
title = "Work"
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &config.City{
		Workspace:     config.Workspace{Name: "test-city"},
		FormulaLayers: config.FormulaLayers{City: []string{dir}},
	}
	a := config.Agent{Name: "polecat", Dir: "hw", DefaultSlingFormula: strPtr("mol-polecat-work")}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	opts := testOpts(a, "explicit-root-only")
	opts.IsFormula = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if len(runner.calls) != 0 {
		t.Fatalf("got %d runner calls, want 0 for built-in routing: %v", len(runner.calls), runner.calls)
	}
	assertStoreRoutedTo(t, deps.Store, "gc-1", "hw/polecat")
	b, err := deps.Store.Get("gc-1")
	if err != nil {
		t.Fatalf("store.Get(gc-1): %v", err)
	}
	if b.Ref != "explicit-root-only" {
		t.Errorf("bead Ref = %q, want explicit-root-only", b.Ref)
	}
}

func TestDefaultFormulaBatchApplied(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "polecat", Dir: "hw", DefaultSlingFormula: strPtr("mol-polecat-work")}

	store := beads.NewMemStoreFrom(0, []beads.Bead{
		{ID: "CVY-1", Title: "batch", Type: "convoy", Status: "open"},
		{ID: "HW-1", Title: "first original goal", Type: "task", Status: "open", ParentID: "CVY-1"},
		{ID: "HW-2", Title: "second original goal", Type: "task", Status: "open", ParentID: "CVY-1"},
	}, nil)

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = store
	opts := testOpts(a, "CVY-1")
	code := doSlingBatch(opts, deps, store, stdout, stderr)

	if code != 0 {
		t.Fatalf("doSlingBatch returned %d, want 0; stderr: %s", code, stderr.String())
	}
	// MolCookOn goes through the store; verify 2 molecule beads were created.
	all, _ := deps.Store.ListOpen()
	molCount := 0
	for _, b := range all {
		if b.Type == "molecule" {
			molCount++
		}
	}
	if molCount != 2 {
		t.Errorf("got %d molecule beads in store, want 2 (one per child)", molCount)
	}
	for _, id := range []string{"HW-1", "HW-2"} {
		work, err := store.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		root, err := store.Get(work.Metadata["molecule_id"])
		if err != nil {
			t.Fatal(err)
		}
		if root.Ref != "mol-polecat-work" || work.Metadata["gc.routed_to"] != "hw/polecat" {
			t.Fatalf("default formula did not select and route child %s: work=%+v root=%+v", id, work, root)
		}
	}
}

func TestDefaultFormulaDryRun(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "polecat", Dir: "hw", DefaultSlingFormula: strPtr("mol-polecat-work")}

	deps, stdout, stderr := testDeps(cfg, sp, runner.run)
	deps.Store = seededStore("HW-42")
	opts := testOpts(a, "HW-42")
	opts.DryRun = true
	code := doSling(opts, deps, nil, stdout, stderr)

	if code != 0 {
		t.Fatalf("dryRunSingle returned %d, want 0", code)
	}
	out := stdout.String()
	if !strings.Contains(out, "Default formula:") {
		t.Errorf("dry-run should show Default formula section; got:\n%s", out)
	}
	if !strings.Contains(out, "mol-polecat-work") {
		t.Errorf("dry-run should show formula name; got:\n%s", out)
	}
	if !strings.Contains(out, "--no-formula") {
		t.Errorf("dry-run should mention --no-formula suppression; got:\n%s", out)
	}
	// No runner calls in dry-run.
	if len(runner.calls) != 0 {
		t.Errorf("dry-run should not execute commands; got %v", runner.calls)
	}
}

func TestBuildSlingFormulaVarsPrefersStoredRigDefaultBranchForPolecatFormula(t *testing.T) {
	// Storing default_branch in city.toml must override the live probe so
	// rigs whose origin/HEAD is unset still get the right base_branch.
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs: []config.Rig{
			{Name: "scamper", Path: "/scamper", Prefix: "SC", DefaultBranch: "master"},
		},
	}
	store := beads.NewMemStoreFrom(0, []beads.Bead{{ID: "SC-1"}}, nil)
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	deps.Store = store

	vars := buildSlingFormulaVars("mol-polecat-work", "SC-1", nil, config.Agent{Name: "polecat", Dir: "scamper"}, deps)

	if got, ok := findVarValue(vars, "base_branch"); !ok || got != "master" {
		t.Fatalf("base_branch var = %q, %v; want master, true (from rig DefaultBranch)", got, ok)
	}
}

func TestBuildSlingFormulaVarsPrefersStoredRigDefaultBranchForHyphenatedPrefix(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs: []config.Rig{
			{Name: "agent-diagnostics", Path: "/agent-diagnostics", Prefix: "agent-diagnostics", DefaultBranch: "master"},
		},
	}
	store := beads.NewMemStoreFrom(0, []beads.Bead{{ID: "agent-diagnostics-hnn"}}, nil)
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	deps.Store = store

	vars := buildSlingFormulaVars("mol-polecat-work", "agent-diagnostics-hnn", nil, config.Agent{Name: "polecat"}, deps)

	if got, ok := findVarValue(vars, "base_branch"); !ok || got != "master" {
		t.Fatalf("base_branch var = %q, %v; want master, true (from hyphenated rig prefix DefaultBranch)", got, ok)
	}
}

func TestBuildSlingFormulaVarsPrefersStoredRigDefaultBranchForAgentPath(t *testing.T) {
	rigPath := t.TempDir()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs: []config.Rig{
			{Name: "scamper", Path: rigPath, Prefix: "SC", DefaultBranch: "master"},
		},
	}
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)

	vars := buildSlingFormulaVars("mol-refinery-patrol", "", nil, config.Agent{Name: "refinery", Dir: rigPath}, deps)

	if got, ok := findVarValue(vars, "target_branch"); !ok || got != "master" {
		t.Fatalf("target_branch var = %q, %v; want master, true (from path-scoped agent DefaultBranch)", got, ok)
	}
}

func TestBuildSlingFormulaVarsPrefersStoredRigDefaultBranchForRefineryFormula(t *testing.T) {
	// The refinery's mol-refinery-patrol uses target_branch instead of
	// base_branch, but the resolution path is identical.
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs: []config.Rig{
			{Name: "scamper", Path: "/scamper", Prefix: "SC", DefaultBranch: "master"},
		},
	}
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)

	vars := buildSlingFormulaVars("mol-refinery-patrol", "", nil, config.Agent{Name: "refinery", Dir: "scamper"}, deps)

	if got, ok := findVarValue(vars, "target_branch"); !ok || got != "master" {
		t.Fatalf("target_branch var = %q, %v; want master, true (from rig DefaultBranch)", got, ok)
	}
}

func TestBuildSlingFormulaVarsUsesBeadTargetForPolecatFormula(t *testing.T) {
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	store := beads.NewMemStoreFrom(0, []beads.Bead{{
		ID:       "HW-42",
		Metadata: map[string]string{"target": "integration/convoy-7"},
	}}, nil)
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	deps.Store = store

	vars := buildSlingFormulaVars("mol-polecat-work", "HW-42", nil, config.Agent{Name: "polecat", Dir: "hw"}, deps)

	if got, ok := findVarValue(vars, "issue"); !ok || got != "HW-42" {
		t.Fatalf("issue var = %q, %v; want HW-42, true", got, ok)
	}
	if got, ok := findVarValue(vars, "base_branch"); !ok || got != "integration/convoy-7" {
		t.Fatalf("base_branch var = %q, %v; want integration/convoy-7, true", got, ok)
	}
}

func TestBuildSlingFormulaVarsUsesAncestorTargetForPolecatFormula(t *testing.T) {
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	store := beads.NewMemStoreFrom(0, []beads.Bead{{
		ID:       "HW-42",
		ParentID: "CVY-7",
	}, {
		ID:       "CVY-7",
		Type:     "convoy",
		Metadata: map[string]string{"target": "integration/convoy-7"},
	}}, nil)
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	deps.Store = store

	vars := buildSlingFormulaVars("mol-polecat-work", "HW-42", nil, config.Agent{Name: "polecat", Dir: "hw"}, deps)

	if got, ok := findVarValue(vars, "base_branch"); !ok || got != "integration/convoy-7" {
		t.Fatalf("base_branch var = %q, %v; want integration/convoy-7, true", got, ok)
	}
}

func TestBuildSlingFormulaVarsIgnoresNonConvoyAncestorTarget(t *testing.T) {
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	store := beads.NewMemStoreFrom(0, []beads.Bead{{
		ID:       "HW-42",
		ParentID: "EP-7",
	}, {
		ID:       "EP-7",
		Type:     "epic",
		Metadata: map[string]string{"target": "integration/legacy-epic"},
	}}, nil)
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	deps.Store = store

	vars := buildSlingFormulaVars("mol-polecat-work", "HW-42", nil, config.Agent{Name: "polecat", Dir: "hw"}, deps)

	if _, ok := findVarValue(vars, "base_branch"); !ok {
		t.Fatal("base_branch var missing")
	}
	if got, _ := findVarValue(vars, "base_branch"); got == "integration/legacy-epic" {
		t.Fatalf("base_branch inherited from non-convoy ancestor: %q", got)
	}
}

func TestBuildSlingFormulaVarsSkipsNonConvoyAncestorTargetAndUsesConvoyAncestor(t *testing.T) {
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	store := beads.NewMemStoreFrom(0, []beads.Bead{{
		ID:       "HW-42",
		ParentID: "EP-7",
	}, {
		ID:       "EP-7",
		Type:     "epic",
		ParentID: "CVY-9",
		Metadata: map[string]string{"target": "integration/legacy-epic"},
	}, {
		ID:       "CVY-9",
		Type:     "convoy",
		Metadata: map[string]string{"target": "integration/convoy-9"},
	}}, nil)
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	deps.Store = store

	vars := buildSlingFormulaVars("mol-polecat-work", "HW-42", nil, config.Agent{Name: "polecat", Dir: "hw"}, deps)

	if got, ok := findVarValue(vars, "base_branch"); !ok || got != "integration/convoy-9" {
		t.Fatalf("base_branch var = %q, %v; want integration/convoy-9, true", got, ok)
	}
}

func TestBuildSlingFormulaVarsUsesRigDefaultBranchWhenTargetMissing(t *testing.T) {
	repoDir := newRepoWithOriginHead(t, "develop")
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs: []config.Rig{
			{Name: "hw", Path: repoDir},
		},
	}
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	deps.Store = beads.NewMemStore()

	vars := buildSlingFormulaVars("mol-polecat-work", "HW-42", nil, config.Agent{Name: "polecat", Dir: "hw"}, deps)

	if got, ok := findVarValue(vars, "base_branch"); !ok || got != "develop" {
		t.Fatalf("base_branch var = %q, %v; want develop, true", got, ok)
	}
}

func TestBuildSlingFormulaVarsPreservesSlashesInRigDefaultBranch(t *testing.T) {
	// Regression test for #719: slashes in the default branch must survive
	// the rig → defaultBranchFor → base_branch path, not just the internal
	// git parser. Previously LastIndex(ref, "/") truncated at the consumer
	// boundary too.
	repoDir := newRepoWithOriginHead(t, "team/feature/x")
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs: []config.Rig{
			{Name: "hw", Path: repoDir},
		},
	}
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	deps.Store = beads.NewMemStore()

	vars := buildSlingFormulaVars("mol-polecat-work", "HW-42", nil, config.Agent{Name: "polecat", Dir: "hw"}, deps)

	if got, ok := findVarValue(vars, "base_branch"); !ok || got != "team/feature/x" {
		t.Fatalf("base_branch var = %q, %v; want team/feature/x, true", got, ok)
	}
}

func TestBuildSlingFormulaVarsPreservesSlashesInRefineryTargetBranch(t *testing.T) {
	// Regression test for #719 covering the refinery target_branch path.
	repoDir := newRepoWithOriginHead(t, "boylec/develop")
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs: []config.Rig{
			{Name: "hw", Path: repoDir},
		},
	}
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)

	vars := buildSlingFormulaVars("mol-refinery-patrol", "", nil, config.Agent{Name: "refinery", Dir: "hw"}, deps)

	if got, ok := findVarValue(vars, "target_branch"); !ok || got != "boylec/develop" {
		t.Fatalf("target_branch var = %q, %v; want boylec/develop, true", got, ok)
	}
}

func TestBuildSlingFormulaVarsPreservesExplicitValues(t *testing.T) {
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	store := beads.NewMemStoreFrom(0, []beads.Bead{{
		ID:       "HW-42",
		Metadata: map[string]string{"target": "integration/convoy-7"},
	}}, nil)
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	deps.Store = store

	vars := buildSlingFormulaVars("mol-polecat-work", "HW-42",
		[]string{"issue=custom-1", "base_branch=release/1.2"}, config.Agent{Name: "polecat", Dir: "hw"}, deps)

	if got, ok := findVarValue(vars, "issue"); !ok || got != "custom-1" {
		t.Fatalf("issue var = %q, %v; want custom-1, true", got, ok)
	}
	if got, ok := findVarValue(vars, "base_branch"); !ok || got != "release/1.2" {
		t.Fatalf("base_branch var = %q, %v; want release/1.2, true", got, ok)
	}
}

func TestBuildSlingFormulaVarsSeedsRoutingNamespace(t *testing.T) {
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)

	vars := buildSlingFormulaVars("mol-polecat-work", "HW-42", nil, config.Agent{
		Name:        "polecat",
		Dir:         "hw",
		BindingName: "gastown",
	}, deps)

	if got, ok := findVarValue(vars, "rig_name"); !ok || got != "hw" {
		t.Fatalf("rig_name var = %q, %v; want hw, true", got, ok)
	}
	if got, ok := findVarValue(vars, "binding_name"); !ok || got != "gastown" {
		t.Fatalf("binding_name var = %q, %v; want gastown, true", got, ok)
	}
	if got, ok := findVarValue(vars, "binding_prefix"); !ok || got != "gastown." {
		t.Fatalf("binding_prefix var = %q, %v; want gastown., true", got, ok)
	}
}

func TestBuildSlingFormulaVarsPreservesExplicitRoutingNamespace(t *testing.T) {
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)

	vars := buildSlingFormulaVars("mol-polecat-work", "HW-42", []string{
		"rig_name=override-rig",
		"binding_name=override-binding",
		"binding_prefix=override.",
	}, config.Agent{
		Name:        "polecat",
		Dir:         "hw",
		BindingName: "gastown",
	}, deps)

	if got, ok := findVarValue(vars, "rig_name"); !ok || got != "override-rig" {
		t.Fatalf("rig_name var = %q, %v; want override-rig, true", got, ok)
	}
	if got, ok := findVarValue(vars, "binding_name"); !ok || got != "override-binding" {
		t.Fatalf("binding_name var = %q, %v; want override-binding, true", got, ok)
	}
	if got, ok := findVarValue(vars, "binding_prefix"); !ok || got != "override." {
		t.Fatalf("binding_prefix var = %q, %v; want override., true", got, ok)
	}
}

func TestBuildSlingFormulaVarsSeedsEmptyRoutingNamespaceForUnboundAgent(t *testing.T) {
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)

	vars := buildSlingFormulaVars("mol-deacon-patrol", "CITY-42", nil, config.Agent{
		Name: "deacon",
	}, deps)

	for _, key := range []string{"rig_name", "binding_name", "binding_prefix"} {
		got, ok := findVarValue(vars, key)
		if !ok || got != "" {
			t.Fatalf("%s var = %q, %v; want empty string, true", key, got, ok)
		}
	}
}

func TestBeadMetadataTargetStopsOnParentCycle(t *testing.T) {
	store := beads.NewMemStoreFrom(0, []beads.Bead{{ID: "A", ParentID: "B"}, {ID: "B", ParentID: "A"}}, nil)

	if got := sling.BeadMetadataTarget(store, "A"); got != "" {
		t.Fatalf("BeadMetadataTarget = %q, want empty string", got)
	}
}

func TestBuildSlingFormulaVarsSeedsRefineryTargetBranch(t *testing.T) {
	repoDir := newRepoWithOriginHead(t, "trunk")
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs: []config.Rig{
			{Name: "hw", Path: repoDir},
		},
	}
	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)

	vars := buildSlingFormulaVars("mol-refinery-patrol", "", nil, config.Agent{Name: "refinery", Dir: "hw"}, deps)

	if got, ok := findVarValue(vars, "target_branch"); !ok || got != "trunk" {
		t.Fatalf("target_branch var = %q, %v; want trunk, true", got, ok)
	}
}

func TestBuildSlingFormulaVarsInjectsIssueAndBaseBranch(t *testing.T) {
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test-city"},
		Rigs: []config.Rig{
			{Name: "hw", Path: newRepoWithOriginHead(t, "develop")},
		},
	}
	a := config.Agent{
		Name:              "polecat",
		Dir:               "hw",
		MinActiveSessions: intPtr(0), MaxActiveSessions: intPtr(5),
	}

	deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	store := beads.NewMemStoreFrom(0, []beads.Bead{{
		ID:       "HW-42",
		Metadata: map[string]string{"target": "integration/convoy-7"},
	}}, nil)
	deps.Store = store

	vars := buildSlingFormulaVars("mol-polecat-work", "HW-42", nil, a, deps)

	if got, ok := findVarValue(vars, "issue"); !ok || got != "HW-42" {
		t.Fatalf("issue var = %q, %v; want HW-42, true", got, ok)
	}
	if got, ok := findVarValue(vars, "base_branch"); !ok || got != "integration/convoy-7" {
		t.Fatalf("base_branch var = %q, %v; want integration/convoy-7, true", got, ok)
	}
}

// TestBuildSlingFormulaVarsRigDefaults covers rig-scoped formula var defaults
// flowing into the final vars map, plus precedence:
//
//	--var > rig.formula_vars > routing-injected > formula default
func TestBuildSlingFormulaVarsRigDefaults(t *testing.T) {
	t.Run("rig defaults flow in when caller omits --var", func(t *testing.T) {
		cfg := &config.City{
			Workspace: config.Workspace{Name: "test-city"},
			Rigs: []config.Rig{
				{
					Name: "mo",
					Path: "/mo",
					FormulaVars: map[string]string{
						"test_command": "make test-fast",
						"lint_command": "golangci-lint run",
					},
				},
			},
		}
		deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
		vars := buildSlingFormulaVars("mol-polecat-work", "MO-1", nil,
			config.Agent{Name: "polecat", Dir: "mo"}, deps)

		if got, ok := findVarValue(vars, "test_command"); !ok || got != "make test-fast" {
			t.Fatalf("test_command = %q, %v; want make test-fast (from rig defaults)", got, ok)
		}
		if got, ok := findVarValue(vars, "lint_command"); !ok || got != "golangci-lint run" {
			t.Fatalf("lint_command = %q, %v; want golangci-lint run (from rig defaults)", got, ok)
		}
	})

	t.Run("--var wins over rig defaults", func(t *testing.T) {
		cfg := &config.City{
			Workspace: config.Workspace{Name: "test-city"},
			Rigs: []config.Rig{
				{
					Name:        "mo",
					Path:        "/mo",
					FormulaVars: map[string]string{"test_command": "make test-fast"},
				},
			},
		}
		deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
		vars := buildSlingFormulaVars("mol-polecat-work", "MO-1",
			[]string{"test_command=go test -short ./..."},
			config.Agent{Name: "polecat", Dir: "mo"}, deps)

		if got, ok := findVarValue(vars, "test_command"); !ok || got != "go test -short ./..." {
			t.Fatalf("test_command = %q, %v; want 'go test -short ./...' (--var override)", got, ok)
		}
	})

	t.Run("no rig match is a no-op", func(t *testing.T) {
		cfg := &config.City{
			Workspace: config.Workspace{Name: "test-city"},
			Rigs: []config.Rig{
				{
					Name:        "mo",
					Path:        "/mo",
					FormulaVars: map[string]string{"test_command": "make test-fast"},
				},
			},
		}
		deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
		vars := buildSlingFormulaVars("mol-polecat-work", "UNK-1", nil,
			config.Agent{Name: "polecat", Dir: "other-rig"}, deps)

		if _, ok := findVarValue(vars, "test_command"); ok {
			t.Fatalf("test_command should not be set when agent rig does not match any rig with formula_vars")
		}
	})

	t.Run("rig match by filesystem path resolves like Dir=rigName", func(t *testing.T) {
		rigPath := "/abs/mo-path"
		cfg := &config.City{
			Workspace: config.Workspace{Name: "test-city"},
			Rigs: []config.Rig{
				{
					Name:        "mo",
					Path:        rigPath,
					FormulaVars: map[string]string{"test_command": "make test-fast"},
				},
			},
		}
		deps, _, _ := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
		vars := buildSlingFormulaVars("mol-polecat-work", "MO-1", nil,
			config.Agent{Name: "polecat", Dir: rigPath}, deps)

		if got, ok := findVarValue(vars, "test_command"); !ok || got != "make test-fast" {
			t.Fatalf("test_command = %q, %v; want make test-fast (path-resolved rig lookup)", got, ok)
		}
	})
}

// --- 1-arg sling tests (via doSling, not cmdSling which needs a real city) ---

func TestFindRigByPrefix(t *testing.T) {
	cfg := &config.City{
		Rigs: []config.Rig{
			{Name: "hello-world", Path: "/tmp/hw", Prefix: "hw"},
			{Name: "my-project", Path: "/tmp/mp"},
		},
	}

	// Exact match.
	rig, ok := findRigByPrefix(cfg, "hw")
	if !ok {
		t.Fatal("expected to find rig with prefix hw")
	}
	if rig.Name != "hello-world" {
		t.Errorf("rig.Name = %q, want hello-world", rig.Name)
	}

	// Case-insensitive match.
	rig, ok = findRigByPrefix(cfg, "HW")
	if !ok {
		t.Fatal("expected case-insensitive match for HW")
	}
	if rig.Name != "hello-world" {
		t.Errorf("rig.Name = %q, want hello-world", rig.Name)
	}

	// Derived prefix match.
	rig, ok = findRigByPrefix(cfg, "mp")
	if !ok {
		t.Fatal("expected to find rig with derived prefix mp")
	}
	if rig.Name != "my-project" {
		t.Errorf("rig.Name = %q, want my-project", rig.Name)
	}

	// No match.
	_, ok = findRigByPrefix(cfg, "zz")
	if ok {
		t.Error("expected no match for prefix zz")
	}
}

func TestOneArgSlingNoPrefix(t *testing.T) {
	// A bead ID with no dash can't derive a prefix.
	// We test this through cmdSling but that requires a city on disk.
	// Instead, test the sling.BeadPrefix helper directly — canonical coverage
	// lives in internal/sling; this just verifies the no-dash contract.
	got := sling.BeadPrefix("nodash")
	if got != "" {
		t.Errorf("sling.BeadPrefix(%q) = %q, want empty", "nodash", got)
	}
}

func TestOneArgSlingFormulaRequiresTarget(t *testing.T) {
	// --formula with 1 arg is checked in newSlingCmd via cobra.RangeArgs.
	// Verify the flag exists and the error path message.
	cmd := newSlingCmd(&bytes.Buffer{}, &bytes.Buffer{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{"--formula", "code-review"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for --formula with 1 arg")
	}
}

func TestSlingJSONArgumentErrorIsStructured(t *testing.T) {
	var stdout, stderr bytes.Buffer
	cmd := newSlingCmd(&stdout, &stderr)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetArgs([]string{"--json"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for --formula with 1 arg")
	}

	var payload cliJSONErrorOutput
	if unmarshalErr := json.Unmarshal(stdout.Bytes(), &payload); unmarshalErr != nil {
		t.Fatalf("stdout is not JSON error: %v\n%s", unmarshalErr, stdout.String())
	}
	if payload.OK || payload.Error.Code != "invalid_arguments" || payload.Error.ExitCode != 1 {
		t.Fatalf("payload = %+v, want invalid_arguments exit 1", payload)
	}
	var diagnostic cliJSONDiagnostic
	if unmarshalErr := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &diagnostic); unmarshalErr != nil {
		t.Fatalf("stderr is not JSON diagnostic: %v\n%s", unmarshalErr, stderr.String())
	}
	if diagnostic.Code != payload.Error.Code || diagnostic.ExitCode != payload.Error.ExitCode {
		t.Fatalf("diagnostic = %+v, payload error = %+v", diagnostic, payload.Error)
	}
}

func TestLooksLikeBeadID(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		// Valid bead IDs (digits-only suffix).
		{"BL-42", true},
		{"gc-1", true},
		{"HW-7", true},
		{"FE-123", true},
		{"abc-0", true},

		// Valid bead IDs (base36 hash suffix from bd).
		{"mp-1j1", true},
		{"mp-bfg", true},
		{"od-2ie", true},
		{"BL-42a", true},
		{"g6-53b", true},

		// Valid bead IDs (5-char base36 suffix from bd).
		{"gc-56nqn", true},
		{"mp-a1b2c", true},

		// Valid bead IDs (longer base36 hash suffixes from bd, up to 8 chars).
		{"gc-8bi3tk", true},
		{"gc-r5sr6bm", true},

		// Valid bead IDs (5-digit numeric suffix from bd counter mode).
		{"gc-10000", true},
		{"gc-99999", true},

		// Valid bead IDs (hierarchical / epic children with dot notation).
		{"ProjectWrenUnity-0fze.1", true},
		{"gc-42.3", true},
		{"BL-1a.7", true},

		// Inline text (not bead IDs).
		{"write a README", false},
		{"write README", false},
		{"hello world", false},
		{"fix the bug", false},

		// Edge cases — not bead IDs.
		{"", false},
		{"nodash", false},
		{"-1", false},
		{"42-abc", false},      // digits before dash
		{"BL-", false},         // nothing after dash
		{"code-review", false}, // long suffix (6+ chars, formula name)
		{"hello-world", false}, // all-alpha suffix (no digit), treated as inline text
		{"hello-there", false}, // all-alpha suffix, not a bead ID
		{"od-zzzzz", false},    // all-alpha suffix, rare but caught by beadExistsInStore fallback
	}
	for _, tt := range tests {
		got := looksLikeBeadID(tt.input)
		if got != tt.want {
			t.Errorf("looksLikeBeadID(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestProbeBeadInStoreFallback(t *testing.T) {
	store := beads.NewMemStoreFrom(0, []beads.Bead{{ID: "ProjectWrenUnity-0fze.1", Title: "Expedition step"}}, nil)

	// beadExistsInStore should find it.
	exists, err := sling.ProbeBeadInStore(store, "ProjectWrenUnity-0fze.1")
	if err != nil {
		t.Fatalf("beadExistsInStore(existing): %v", err)
	}
	if !exists {
		t.Error("beadExistsInStore should find existing bead")
	}

	// Non-existent bead should return false.
	exists, err = sling.ProbeBeadInStore(store, "nonexistent-xyz")
	if err != nil {
		t.Fatalf("beadExistsInStore(missing): %v", err)
	}
	if exists {
		t.Error("beadExistsInStore should return false for missing bead")
	}
}

func TestProbeBeadInStoreSurfacesLookupError(t *testing.T) {
	store := &getErrStore{Store: beads.NewMemStore(), err: fmt.Errorf("lookup failed")}

	_, err := sling.ProbeBeadInStore(store, "gc-1")
	if err == nil {
		t.Fatal("ProbeBeadInStore error = nil, want lookup failure")
	}
	if !strings.Contains(err.Error(), "lookup failed") {
		t.Fatalf("ProbeBeadInStore error = %q, want lookup failure", err)
	}
}

func TestOneArgSlingInlineTextRequiresTarget(t *testing.T) {
	// Inline text with 1 arg should error asking for explicit target.
	cmd := newSlingCmd(&bytes.Buffer{}, &bytes.Buffer{})
	cmd.SetArgs([]string{"write a README"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for inline text with 1 arg")
	}
}

func TestSlingStdinSingleLine(t *testing.T) {
	// --stdin with a single line creates a bead with title only.
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
	deps, stdout, stderr := testDeps(cfg, sp, runner.run)

	// Override slingStdin to provide test input.
	old := slingStdin
	slingStdin = func() io.Reader { return strings.NewReader("fix login bug\n") }
	defer func() { slingStdin = old }()

	// Simulate what cmdSling does for --stdin: read stdin, create bead, sling it.
	content := "fix login bug"
	created, err := deps.Store.Create(beads.Bead{Title: content, Type: "task"})
	if err != nil {
		t.Fatalf("creating bead: %v", err)
	}

	opts := testOpts(a, created.ID)
	code := doSling(opts, deps, nil, stdout, stderr)
	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Slung "+created.ID) {
		t.Errorf("stdout = %q, want to contain 'Slung %s'", stdout.String(), created.ID)
	}

	// Verify the bead has no description.
	got, err := deps.Store.Get(created.ID)
	if err != nil {
		t.Fatalf("getting bead: %v", err)
	}
	if got.Description != "" {
		t.Errorf("bead description = %q, want empty", got.Description)
	}
}

func TestSlingStdinMultiLine(t *testing.T) {
	// --stdin with multiple lines: first line = title, rest = description.
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
	deps, stdout, stderr := testDeps(cfg, sp, runner.run)

	old := slingStdin
	slingStdin = func() io.Reader {
		return strings.NewReader("fix login bug\nThe login page returns 500\nwhen email has a plus sign\n")
	}
	defer func() { slingStdin = old }()

	// Create bead with description (simulating the stdin split).
	created, err := deps.Store.Create(beads.Bead{
		Title:       "fix login bug",
		Description: "The login page returns 500\nwhen email has a plus sign",
		Type:        "task",
	})
	if err != nil {
		t.Fatalf("creating bead: %v", err)
	}

	opts := testOpts(a, created.ID)
	code := doSling(opts, deps, nil, stdout, stderr)
	if code != 0 {
		t.Fatalf("doSling returned %d, want 0; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Slung "+created.ID) {
		t.Errorf("stdout = %q, want to contain 'Slung %s'", stdout.String(), created.ID)
	}

	// Verify bead has description.
	got, err := deps.Store.Get(created.ID)
	if err != nil {
		t.Fatalf("getting bead: %v", err)
	}
	if got.Description != "The login page returns 500\nwhen email has a plus sign" {
		t.Errorf("bead description = %q, want multi-line description", got.Description)
	}
}

func TestSlingStdinEmpty(t *testing.T) {
	// --stdin with empty input returns error.
	var stderr bytes.Buffer
	cmd := newSlingCmd(&bytes.Buffer{}, &stderr)
	cmd.SetArgs([]string{"mayor", "--stdin"})

	old := slingStdin
	slingStdin = func() io.Reader { return strings.NewReader("") }
	defer func() { slingStdin = old }()

	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for empty stdin")
	}
	if !strings.Contains(stderr.String(), "no input received") {
		t.Errorf("stderr = %q, want to contain 'no input received'", stderr.String())
	}
}

func TestSlingStdinMutuallyExclusiveWithFormula(t *testing.T) {
	// --stdin and --formula are mutually exclusive.
	var stderr bytes.Buffer
	cmd := newSlingCmd(&bytes.Buffer{}, &stderr)
	cmd.SetArgs([]string{"mayor", "--stdin", "--formula"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for --stdin with --formula")
	}
}

func TestSlingStdinWithExtraArg(t *testing.T) {
	// --stdin with 2 positional args (target + text) should error.
	var stderr bytes.Buffer
	cmd := newSlingCmd(&bytes.Buffer{}, &stderr)
	cmd.SetArgs([]string{"mayor", "extra-arg", "--stdin"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for --stdin with extra positional arg")
	}
	if !strings.Contains(stderr.String(), "--stdin requires exactly 1 argument") {
		t.Errorf("stderr = %q, want to contain '--stdin requires exactly 1 argument'", stderr.String())
	}
}

// setupCmdSlingMultiDefaultTargetsFixture creates a city with two worker agents
// in the "foundations" rig and optionally configures default_sling_targets with
// both. The bead fo-multi-work is pre-seeded in the rig store.
func setupCmdSlingMultiDefaultTargetsFixture(t *testing.T, targets []string) (cityDir, rigDir string) {
	t.Helper()
	configureIsolatedRuntimeEnv(t)
	t.Setenv("GC_BEADS", "file")

	cityDir = t.TempDir()
	t.Setenv("GC_CITY", cityDir)
	t.Setenv("GC_CITY_PATH", "")
	t.Setenv("GC_CITY_ROOT", "")
	t.Setenv("GC_RIG", "")
	t.Setenv("GC_RIG_ROOT", "")
	rigDir = filepath.Join(cityDir, "foundations")
	if err := os.MkdirAll(rigDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(rig): %v", err)
	}
	if err := ensureScopedFileStoreLayout(cityDir); err != nil {
		t.Fatalf("ensureScopedFileStoreLayout: %v", err)
	}
	for _, dir := range []string{cityDir, rigDir} {
		if err := ensurePersistedScopeLocalFileStore(dir); err != nil {
			t.Fatalf("ensurePersistedScopeLocalFileStore(%s): %v", dir, err)
		}
	}
	installSlingMemStores(t, cityDir, map[string][]beads.Bead{
		rigDir: {{ID: "fo-multi-work", Title: "multi-target work bead", Type: "task", Status: "open"}},
	})

	targetsLine := ""
	if len(targets) > 0 {
		quoted := make([]string, len(targets))
		for i, tgt := range targets {
			quoted[i] = fmt.Sprintf("%q", tgt)
		}
		targetsLine = "default_sling_targets = [" + strings.Join(quoted, ", ") + "]\n"
	}
	cityToml := `[workspace]
name = "demo"

[[rigs]]
name = "foundations"
path = "foundations"
prefix = "fo"
` + targetsLine + `
[[agent]]
name = "worker-a"
dir = "foundations"

[[agent]]
name = "worker-b"
dir = "foundations"
`
	writeSlingTestCity(t, cityDir, cityToml)
	t.Chdir(cityDir)
	return cityDir, rigDir
}

// TestCmdSlingMultiDefaultTargetsPicksFromList verifies that when
// default_sling_targets lists two agents, targetless gc sling routes the bead
// to one of them (the exact pick is random, so we accept either).
func TestCmdSlingMultiDefaultTargetsPicksFromList(t *testing.T) {
	cityDir, rigDir := setupCmdSlingMultiDefaultTargetsFixture(t,
		[]string{"foundations/worker-a", "foundations/worker-b"},
	)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"fo-multi-work"},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		false, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code != 0 {
		t.Fatalf("cmdSling returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}

	rigStore, err := slingOpenStoreAtForCity(rigDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(rig): %v", err)
	}
	routed, err := rigStore.Get("fo-multi-work")
	if err != nil {
		t.Fatalf("rigStore.Get(fo-multi-work): %v", err)
	}
	got := routed.Metadata["gc.routed_to"]
	if got != "foundations/worker-a" && got != "foundations/worker-b" {
		t.Fatalf("gc.routed_to = %q, want one of [foundations/worker-a, foundations/worker-b]", got)
	}
}

// TestCmdSlingMultiDefaultTargetsSingleEntry verifies that a single-entry
// default_sling_targets list behaves identically to default_sling_target.
func TestCmdSlingMultiDefaultTargetsSingleEntry(t *testing.T) {
	cityDir, rigDir := setupCmdSlingMultiDefaultTargetsFixture(t,
		[]string{"foundations/worker-a"},
	)

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"fo-multi-work"},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		false, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code != 0 {
		t.Fatalf("cmdSling returned %d, want 0; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}

	rigStore, err := slingOpenStoreAtForCity(rigDir, cityDir)
	if err != nil {
		t.Fatalf("openStoreAtForCity(rig): %v", err)
	}
	routed, err := rigStore.Get("fo-multi-work")
	if err != nil {
		t.Fatalf("rigStore.Get(fo-multi-work): %v", err)
	}
	if routed.Metadata["gc.routed_to"] != "foundations/worker-a" {
		t.Fatalf("gc.routed_to = %q, want foundations/worker-a", routed.Metadata["gc.routed_to"])
	}
}

// TestCmdSlingMultiDefaultTargetsEmptyEntryRejected verifies that an empty
// string inside default_sling_targets is rejected with a clear error.
func TestCmdSlingMultiDefaultTargetsEmptyEntryRejected(t *testing.T) {
	setupCmdSlingMultiDefaultTargetsFixture(t, []string{"foundations/worker-a", ""})

	var stdout, stderr bytes.Buffer
	code := cmdSling(
		[]string{"fo-multi-work"},
		false, false, false,
		"", nil, "",
		true, false, false, "",
		false, false, false,
		"", "",
		&stdout, &stderr,
	)
	if code == 0 {
		t.Fatalf("cmdSling returned 0, want non-zero for empty entry in default_sling_targets")
	}
	if !strings.Contains(stderr.String(), "empty entry") {
		t.Errorf("stderr = %q, want to mention 'empty entry'", stderr.String())
	}
}

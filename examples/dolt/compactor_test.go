package dolt_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const compactorScript = "assets/scripts/compactor.sh"

func TestCompactorDryRunRecordsPreflightEvidenceWithoutSQLMutation(t *testing.T) {
	result := runCompactorFixture(t, true)

	if !strings.Contains(result.output, "would flatten") {
		t.Fatalf("compactor output = %s, want dry-run flatten summary", result.output)
	}
	for _, want := range []string{
		"SELECT COUNT(*) FROM dolt_log;",
		"SELECT table_name FROM information_schema.tables",
		"SELECT commit_hash FROM dolt_log",
	} {
		if !strings.Contains(result.doltLog, want) {
			t.Fatalf("dolt log missing %q:\n%s", want, result.doltLog)
		}
	}
	for _, forbidden := range []string{"DOLT_RESET", "DOLT_COMMIT", "DOLT_GC"} {
		if strings.Contains(result.doltLog, forbidden) {
			t.Fatalf("dry-run executed forbidden SQL %q:\n%s", forbidden, result.doltLog)
		}
	}
	for _, want := range []string{"sc-run", "dolt_compactor.status=preflight", "dolt_compactor.status=dry-run"} {
		if !strings.Contains(result.bdLog, want) {
			t.Fatalf("bd evidence log missing %q:\n%s", want, result.bdLog)
		}
	}
}

func TestCompactorFlattenVerifiesRowsAndRunsDoltGC(t *testing.T) {
	result := runCompactorFixture(t, false)

	for _, want := range []string{"integrity=ok", "dolt_gc=ok", "processed=1 failed=0"} {
		if !strings.Contains(result.output, want) {
			t.Fatalf("compactor output missing %q:\n%s", want, result.output)
		}
	}
	for _, want := range []string{"DOLT_RESET", "DOLT_COMMIT", "CALL DOLT_GC();"} {
		if !strings.Contains(result.doltLog, want) {
			t.Fatalf("dolt log missing %q:\n%s", want, result.doltLog)
		}
	}
	if got := strings.Count(result.doltLog, "SELECT COUNT(*) FROM dolt_log;"); got < 2 {
		t.Fatalf("commit count query count = %d, want preflight and postflight:\n%s", got, result.doltLog)
	}
	for _, want := range []string{
		"dolt_compactor.status=preflight",
		"dolt_compactor.status=verified",
		"dolt_compactor.status=gc",
		"dolt_compactor.status=complete",
	} {
		if !strings.Contains(result.bdLog, want) {
			t.Fatalf("bd evidence log missing %q:\n%s", want, result.bdLog)
		}
	}
}

func TestCompactorFlattenRebaselinesAfterPreflightEvidenceMutation(t *testing.T) {
	mutationFile := filepath.Join(t.TempDir(), "bd-mutated")
	result := runCompactorFixtureWithExtraEnv(t, false, []string{
		"FAKE_BD_MUTATION_FILE=" + mutationFile,
	})

	if !strings.Contains(result.output, "integrity=ok") {
		t.Fatalf("compactor output missing integrity success after evidence mutation:\n%s", result.output)
	}
	if !strings.Contains(result.bdLog, "dolt_compactor.status=preflight") {
		t.Fatalf("bd evidence log missing preflight mutation trigger:\n%s", result.bdLog)
	}
}

func TestCompactorFlattenAllowsOperationalConcurrentChurn(t *testing.T) {
	churnFile := filepath.Join(t.TempDir(), "post-churn")
	result := runCompactorFixtureWithExtraEnv(t, false, []string{
		"FAKE_DOLT_POST_CHURN_FILE=" + churnFile,
	})

	for _, want := range []string{"integrity=ok", "concurrent_changes=", "issues:5->8", "dolt_gc=ok"} {
		if !strings.Contains(result.output, want) {
			t.Fatalf("compactor output missing %q after concurrent churn:\n%s", want, result.output)
		}
	}
}

type compactorFixtureResult struct {
	output  string
	doltLog string
	bdLog   string
}

func runCompactorFixture(t *testing.T, dryRun bool) compactorFixtureResult {
	t.Helper()
	return runCompactorFixtureWithExtraEnv(t, dryRun, nil)
}

func runCompactorFixtureWithExtraEnv(t *testing.T, dryRun bool, extraEnv []string) compactorFixtureResult {
	t.Helper()

	packDir := repoRoot(t)
	cityDir := t.TempDir()
	stateDir := filepath.Join(cityDir, ".gc", "runtime", "packs", "dolt")
	dataDir := filepath.Join(cityDir, ".beads", "dolt")
	if err := os.MkdirAll(filepath.Join(dataDir, "hq", ".dolt"), 0o755); err != nil {
		t.Fatalf("MkdirAll(dolt data): %v", err)
	}
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(state): %v", err)
	}

	port, cleanup := startDeadTCPListener(t)
	defer cleanup()

	stateFile := filepath.Join(stateDir, "dolt-state.json")
	state := `{"running":true,"pid":` + strconv.Itoa(os.Getpid()) + `,"port":` + strconv.Itoa(port) + `,"data_dir":` + strconv.Quote(dataDir) + `}`
	if err := os.WriteFile(stateFile, []byte(state), 0o644); err != nil {
		t.Fatalf("WriteFile(state): %v", err)
	}

	binDir := t.TempDir()
	doltLog := filepath.Join(cityDir, "dolt.log")
	bdLog := filepath.Join(cityDir, "bd.log")
	countFile := filepath.Join(cityDir, "commit-count")
	if err := os.WriteFile(filepath.Join(binDir, "dolt"), []byte(fakeCompactorDoltScript), 0o755); err != nil {
		t.Fatalf("WriteFile(fake dolt): %v", err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "bd"), []byte(fakeCompactorBDScript), 0o755); err != nil {
		t.Fatalf("WriteFile(fake bd): %v", err)
	}

	cmd := exec.Command("sh", filepath.Join(packDir, compactorScript))
	env := filteredEnv(
		"GC_CITY_PATH",
		"GC_PACK_DIR",
		"GC_DOLT_HOST",
		"GC_DOLT_PORT",
		"GC_DOLT_USER",
		"GC_DOLT_PASSWORD",
		"GC_DOLT_DATA_DIR",
		"GC_DOLT_STATE_FILE",
		"GC_DOLT_COMPACTOR_DATABASES",
		"GC_DOLT_COMPACTOR_DRY_RUN",
		"GC_DOLT_COMPACTOR_EVIDENCE_BEAD",
		"GC_DOLT_COMPACTOR_EVIDENCE_BEADS",
		"GC_ORDER_TRACKING_ID",
		"GC_DOLT_COMPACTOR_COMMIT_THRESHOLD",
		"GC_DOLT_COMPACTOR_SQL_TIMEOUT_SECS",
		"GC_DOLT_COMPACTOR_GC_TIMEOUT_SECS",
	)
	env = append(env,
		"PATH="+binDir+":"+os.Getenv("PATH"),
		"GC_CITY_PATH="+cityDir,
		"GC_DOLT_HOST=127.0.0.1",
		"GC_DOLT_PORT="+strconv.Itoa(port),
		"GC_DOLT_USER=root",
		"GC_DOLT_DATA_DIR="+dataDir,
		"GC_DOLT_STATE_FILE="+stateFile,
		"GC_DOLT_COMPACTOR_DATABASES=hq",
		"GC_DOLT_COMPACTOR_EVIDENCE_BEAD=sc-run",
		"GC_DOLT_COMPACTOR_COMMIT_THRESHOLD=500",
		"GC_DOLT_COMPACTOR_SQL_TIMEOUT_SECS=5",
		"GC_DOLT_COMPACTOR_GC_TIMEOUT_SECS=5",
		"FAKE_DOLT_LOG="+doltLog,
		"FAKE_DOLT_COUNT_FILE="+countFile,
		"FAKE_BD_LOG="+bdLog,
	)
	if dryRun {
		env = append(env, "GC_DOLT_COMPACTOR_DRY_RUN=1")
	}
	env = append(env, extraEnv...)
	cmd.Env = env

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("compactor failed: %v\n%s", err, output)
	}
	return compactorFixtureResult{
		output:  string(output),
		doltLog: readCompactorFixtureFile(t, doltLog),
		bdLog:   readCompactorFixtureFile(t, bdLog),
	}
}

func readCompactorFixtureFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	return string(data)
}

const fakeCompactorDoltScript = `#!/bin/sh
set -eu

query=""
db=""
prev=""
for arg in "$@"; do
  if [ "$prev" = "--use-db" ]; then
    db="$arg"
  fi
  if [ "$prev" = "-q" ]; then
    query="$arg"
  fi
  prev="$arg"
done

printf '%s	%s\n' "$db" "$query" >> "$FAKE_DOLT_LOG"

case "$query" in
  "SELECT COUNT(*) FROM dolt_log;")
    count=600
    if [ -f "$FAKE_DOLT_COUNT_FILE" ]; then
      count=$(sed -n '1p' "$FAKE_DOLT_COUNT_FILE")
    fi
    printf 'COUNT(*)\n%s\n' "$count"
    ;;
  "SELECT commit_hash FROM dolt_log ORDER BY commit_order ASC LIMIT 1;")
    printf 'commit_hash\nabc123\n'
    ;;
  *"information_schema.tables"*)
    printf 'table_name\nissues\nsessions\n'
    ;;
  *"issues"*)
    count=5
    if [ -n "${FAKE_BD_MUTATION_FILE:-}" ] && [ -f "$FAKE_BD_MUTATION_FILE" ]; then
      count=6
    fi
    if [ -n "${FAKE_DOLT_POST_CHURN_FILE:-}" ] && [ -f "$FAKE_DOLT_POST_CHURN_FILE" ]; then
      count=8
    fi
    printf 'COUNT(*)\n%s\n' "$count"
    ;;
  *"sessions"*)
    printf 'COUNT(*)\n7\n'
    ;;
  *"DOLT_RESET"* | *"DOLT_COMMIT"*)
    printf '1\n' > "$FAKE_DOLT_COUNT_FILE"
    if [ -n "${FAKE_DOLT_POST_CHURN_FILE:-}" ]; then
      : > "$FAKE_DOLT_POST_CHURN_FILE"
    fi
    printf 'status\nok\n'
    ;;
  "CALL DOLT_GC();")
    printf 'status\nok\n'
    ;;
  *)
    printf 'unexpected query: %s\n' "$query" >&2
    exit 19
    ;;
esac
`

const fakeCompactorBDScript = `#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_BD_LOG"
case "$*" in
  *dolt_compactor.status=preflight*)
    if [ -n "${FAKE_BD_MUTATION_FILE:-}" ]; then
      : > "$FAKE_BD_MUTATION_FILE"
    fi
    ;;
esac
exit 0
`

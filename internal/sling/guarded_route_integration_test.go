//go:build integration

package sling

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/molecule"
)

// The integration runner owns the private SQL server. Each test owns its schema
// and filesystem; admission still calls the unchanged real host authority.
func guardedNativeFixture(t *testing.T) (string, *beads.NativeDoltStore, *beads.NativeDoltStore, *config.City) {
	t.Helper()
	if os.Getenv("GC_SLING_ADMISSION_COMMAND") == "" {
		t.Fatal("configure the real execution-host admission command before this SQL canary")
	}
	fixture := os.Getenv("GC_SLING_TEST_SCOPE")
	if fixture == "" {
		t.Fatal("prepare the private native SQL fixture before this canary")
	}
	raw, err := os.ReadFile(filepath.Join(fixture, "client-env.json"))
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]string
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	scope := t.TempDir()
	beadsDir := filepath.Join(scope, ".beads")
	if err := os.MkdirAll(beadsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(scope))
	database := fmt.Sprintf("beads_%x", digest[:16])
	env["BEADS_DOLT_SERVER_DATABASE"] = database
	for key, value := range env {
		t.Setenv(key, value)
	}
	t.Setenv("BEADS_DIR", beadsDir)
	t.Setenv("BEADS_TEST_MODE", "1")
	t.Setenv("BEADS_TEST_SERVER", "1")
	t.Setenv("NERVE_ANDON_PATH", filepath.Join(scope, "andon.json"))
	dsn := fmt.Sprintf("root@tcp(127.0.0.1:%s)/?timeout=1s&readTimeout=1s&writeTimeout=1s", env["BEADS_DOLT_SERVER_PORT"])
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE `"+database+"`"); err != nil {
		t.Fatal(err)
	}
	metadata := map[string]any{
		"backend": "dolt", "database": database, "dolt_mode": "server",
		"dolt_server_host": "127.0.0.1", "dolt_server_port": json.Number(env["BEADS_DOLT_SERVER_PORT"]),
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(beadsDir, "metadata.json"), metadataJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	storage, err := beads.OpenNativeStorage(ctx, scope, env)
	if err != nil {
		t.Fatal(err)
	}
	err = storage.SetConfig(ctx, "issue_prefix", "gc")
	closeErr := storage.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	open := func() *beads.NativeDoltStore {
		store, err := beads.OpenNativeDoltStoreAt(context.Background(), scope, env)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := store.CloseStore(); err != nil {
				t.Error(err)
			}
		})
		return store
	}
	cfg := &config.City{Workspace: config.Workspace{Name: "guarded-native"}, Agents: []config.Agent{{Name: "worker-a"}, {Name: "worker-b"}}}
	return scope, open(), open(), cfg
}

func TestGuardedNativeRouteCompetingSQLWriters(t *testing.T) {
	scope, a, b, cfg := guardedNativeFixture(t)
	work, err := a.Create(beads.Bead{Title: "competing actual native routes", Type: "task", Status: "open"})
	if err != nil {
		t.Fatal(err)
	}
	guards, err := routeConditions(work, nil)
	if err != nil {
		t.Fatal(err)
	}
	start, results := make(chan struct{}), make(chan error, 2)
	for i, store := range []*beads.NativeDoltStore{a, b} {
		go func() {
			<-start
			results <- CommitRoute(context.Background(), store, cfg, scope, RouteRequest{BeadID: work.ID, Target: fmt.Sprintf("worker-%c", 'a'+i), Conditions: &guards})
		}()
	}
	close(start)
	wins := 0
	for range 2 {
		err := <-results
		if err == nil {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("actual guarded SQL routes produced %d commits, want one", wins)
	}
	got, err := b.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata[beadmeta.RoutedToMetadataKey] != "worker-a" && got.Metadata[beadmeta.RoutedToMetadataKey] != "worker-b" {
		t.Fatalf("no winning canonical native route: %+v", got)
	}
	if got.Assignee != "" || got.Status != "open" {
		t.Fatalf("route stole canonical session ownership: %+v", got)
	}
}

func TestGuardedNativeRouteRejectsOwnerChangeAndHold(t *testing.T) {
	scope, a, b, cfg := guardedNativeFixture(t)
	for _, tc := range []struct {
		hold     bool
		reassign bool
	}{{false, false}, {true, false}, {false, true}, {true, true}} {
		hold := tc.hold
		work, err := a.Create(beads.Bead{Title: "preflight/commit competing owner", Type: "task", Status: "open"})
		if err != nil {
			t.Fatal(err)
		}
		guards, err := routeConditions(work, nil)
		if err != nil {
			t.Fatal(err)
		}
		owner := "new-owner"
		change := beads.UpdateOpts{Assignee: &owner}
		if hold {
			change = beads.UpdateOpts{Metadata: map[string]string{"handoff.conflict_state": "hold"}}
		}
		if err := b.Update(work.ID, change); err != nil {
			t.Fatal(err)
		}
		err = CommitRoute(context.Background(), a, cfg, scope, RouteRequest{BeadID: work.ID, Target: "worker-a", Conditions: &guards, Reassign: tc.reassign})
		var conflict *RouteConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("changed owner/hold must refuse the stale route, got %v", err)
		}
		got, err := b.Get(work.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Metadata[beadmeta.RoutedToMetadataKey] != "" || hold && got.Metadata["handoff.conflict_state"] != "hold" || !hold && got.Assignee != owner {
			t.Fatalf("stale route changed canonical disposition: %+v", got)
		}
	}
}

func TestGuardedNativeRouteLostAcknowledgmentReconcilesWithoutAnotherWrite(t *testing.T) {
	scope, a, b, cfg := guardedNativeFixture(t)
	work, err := a.Create(beads.Bead{Title: "accepted route with lost acknowledgment", Type: "task", Status: "open", Metadata: map[string]string{beadmeta.DispatchEffectIDMetadataKey: "stable-actual-effect", beadmeta.DispatchEffectStateMetadataKey: "attempted"}})
	if err != nil {
		t.Fatal(err)
	}
	guards, err := routeConditions(work, nil)
	if err != nil {
		t.Fatal(err)
	}
	req := RouteRequest{BeadID: work.ID, Target: "worker-a", Conditions: &guards}
	if err := CommitRoute(context.Background(), a, cfg, scope, req); err != nil {
		t.Fatal(err)
	}
	got, err := b.Get(work.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata[beadmeta.DispatchEffectIDMetadataKey] != "stable-actual-effect" || got.Metadata[beadmeta.DispatchEffectStateMetadataKey] != "routed" || got.Metadata[beadmeta.RoutedToMetadataKey] != "worker-a" {
		t.Fatalf("route and effect receipt did not commit together: %+v", got)
	}
	if err := CommitRoute(context.Background(), b, cfg, scope, req); err != nil {
		t.Fatalf("same uncertain effect must reconcile: %v", err)
	}
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s:%s)/%s?parseTime=true", os.Getenv("BEADS_DOLT_SERVER_HOST"), os.Getenv("BEADS_DOLT_SERVER_PORT"), os.Getenv("BEADS_DOLT_SERVER_DATABASE")))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var actualRoutes int
	if err := db.QueryRow("SELECT COUNT(*) FROM events WHERE issue_id = ? AND event_type = 'updated' AND JSON_UNQUOTE(JSON_EXTRACT(IF(JSON_VALID(new_value), new_value, '{}'), '$.metadata.\"gc.dispatch_effect_state\"')) = 'routed'", work.ID).Scan(&actualRoutes); err != nil {
		t.Fatal(err)
	}
	if actualRoutes != 1 {
		t.Fatalf("accepted effect wrote %d native route events, want one original event", actualRoutes)
	}
}

func TestGuardedNativeFormulaActivationRollsBackWithStaleSource(t *testing.T) {
	scope, a, b, cfg := guardedNativeFixture(t)
	source, err := a.Create(beads.Bead{Title: "source", Type: "task", Status: "open"})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := a.Create(beads.Bead{Title: "speculative workflow", Type: "gate", Status: "open", Metadata: map[string]string{beadmeta.AttachFencePendingMetadataKey: "true", molecule.DeferredTypeMetadataKey: "task", molecule.DeferredRoutedToMetadataKey: "worker-a"}})
	if err != nil {
		t.Fatal(err)
	}
	guards, err := routeConditions(source, nil)
	if err != nil {
		t.Fatal(err)
	}
	owner := "new-owner"
	if err := b.Update(source.ID, beads.UpdateOpts{Assignee: &owner}); err != nil {
		t.Fatal(err)
	}
	deps := SlingDeps{Store: a, Cfg: cfg, CityPath: scope}
	err = commitFormulaTransaction(context.Background(), a, source.ID, beads.UpdateOpts{Metadata: map[string]string{"workflow_id": candidate.ID, beadmeta.ExecutionRoutedToMetadataKey: "worker-a"}}, guards, &molecule.Result{RootID: candidate.ID, IDMapping: map[string]string{"root": candidate.ID}}, true, deps, false, nil)
	var conflict *RouteConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("stale source must lose atomic activation, got %v", err)
	}
	got, err := b.Get(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != "gate" || got.Status != "open" || got.Metadata[beadmeta.RoutedToMetadataKey] != "" || got.Metadata[beadmeta.AttachFencePendingMetadataKey] != "true" {
		t.Fatalf("losing native candidate became runnable: %+v", got)
	}
}

func TestGuardedNativeRouteRejectsChangedLabelsButKeepsOrdinaryMetadata(t *testing.T) {
	scope, a, b, cfg := guardedNativeFixture(t)
	for _, labelChange := range []bool{true, false} {
		work, err := a.Create(beads.Bead{Title: "precise current-state route predicates", Type: "task", Status: "open"})
		if err != nil {
			t.Fatal(err)
		}
		guards, err := routeConditions(work, nil)
		if err != nil {
			t.Fatal(err)
		}
		change := beads.UpdateOpts{Metadata: map[string]string{"operator.note": "must survive"}}
		if labelChange {
			change.Labels = []string{"private-review"}
		}
		if err := b.Update(work.ID, change); err != nil {
			t.Fatal(err)
		}
		err = CommitRoute(context.Background(), a, cfg, scope, RouteRequest{BeadID: work.ID, Target: "worker-a", Conditions: &guards})
		got, readErr := b.Get(work.ID)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if labelChange {
			var conflict *RouteConflictError
			if !errors.As(err, &conflict) || got.Metadata[beadmeta.RoutedToMetadataKey] != "" {
				t.Fatalf("changed canonical labels admitted stale routing: err=%v bead=%+v", err, got)
			}
		} else if err != nil || got.Metadata[beadmeta.RoutedToMetadataKey] != "worker-a" || got.Metadata["operator.note"] != "must survive" {
			t.Fatalf("unrelated metadata was revision-fenced or lost: err=%v bead=%+v", err, got)
		}
	}
}

func TestGuardedNativeRouteMissingHostAdmissionCannotCommit(t *testing.T) {
	scope, a, b, cfg := guardedNativeFixture(t)
	work, err := a.Create(beads.Bead{Title: "no unknown-admission dispatch", Type: "task", Status: "open", Metadata: map[string]string{beadmeta.DispatchEffectIDMetadataKey: "admission-required", beadmeta.DispatchEffectStateMetadataKey: "attempted"}})
	if err != nil {
		t.Fatal(err)
	}
	guards, err := routeConditions(work, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GC_SLING_ADMISSION_COMMAND", "")
	err = CommitRoute(context.Background(), a, cfg, scope, RouteRequest{BeadID: work.ID, Target: "worker-a", Conditions: &guards})
	got, readErr := b.Get(work.ID)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if err == nil || got.Metadata[beadmeta.RoutedToMetadataKey] != "" || got.Metadata[beadmeta.DispatchEffectStateMetadataKey] != "attempted" {
		t.Fatalf("unknown host admission committed a dispatch: err=%v bead=%+v", err, got)
	}
}

func TestGuardedNativeFormulaMixedTierForceReplacementAndHold(t *testing.T) {
	scope, a, b, cfg := guardedNativeFixture(t)
	for _, held := range []bool{false, true} {
		previous, err := a.Create(beads.Bead{Title: "running predecessor", Type: "task", Status: "in_progress", Assignee: "original-owner", Metadata: map[string]string{beadmeta.FormulaContractMetadataKey: beadmeta.FormulaContractGraphV2}})
		if err != nil {
			t.Fatal(err)
		}
		source, err := a.Create(beads.Bead{Title: "durable source", Type: "task", Status: "open", Metadata: map[string]string{"workflow_id": previous.ID}})
		if err != nil {
			t.Fatal(err)
		}
		replaced, err := json.Marshal([]string{previous.ID})
		if err != nil {
			t.Fatal(err)
		}
		candidate, err := a.Create(beads.Bead{Title: "ignored-tier speculative replacement", Type: "gate", Status: "open", Ephemeral: true, Metadata: map[string]string{beadmeta.AttachFencePendingMetadataKey: "true", beadmeta.FormulaContractMetadataKey: beadmeta.FormulaContractGraphV2, molecule.DeferredTypeMetadataKey: "task", molecule.DeferredRoutedToMetadataKey: "worker-a", beadmeta.DispatchReplacedRootsMetadataKey: string(replaced)}})
		if err != nil {
			t.Fatal(err)
		}
		guards, err := routeConditions(source, nil)
		if err != nil {
			t.Fatal(err)
		}
		if held {
			if err := b.Update(previous.ID, beads.UpdateOpts{Metadata: map[string]string{"handoff.conflict_state": "hold"}}); err != nil {
				t.Fatal(err)
			}
		}
		deps := SlingDeps{Store: a, Cfg: cfg, CityPath: scope}
		err = commitFormulaTransaction(context.Background(), a, source.ID, beads.UpdateOpts{Metadata: map[string]string{"workflow_id": candidate.ID, beadmeta.ExecutionRoutedToMetadataKey: "worker-a"}}, guards, &molecule.Result{RootID: candidate.ID, IDMapping: map[string]string{"root": candidate.ID}}, true, deps, false, nil)
		gotSource, readErr := b.Get(source.ID)
		if readErr != nil {
			t.Fatal(readErr)
		}
		gotCandidate, readErr := b.Get(candidate.ID)
		if readErr != nil {
			t.Fatal(readErr)
		}
		gotPrevious, readErr := b.Get(previous.ID)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if held {
			var conflict *RouteConflictError
			if !errors.As(err, &conflict) || gotSource.Metadata["workflow_id"] != previous.ID || gotCandidate.Type != "gate" || gotCandidate.Metadata[beadmeta.AttachFencePendingMetadataKey] != "true" || gotPrevious.Status != "in_progress" || gotPrevious.Assignee != "original-owner" || gotPrevious.Metadata["handoff.conflict_state"] != "hold" {
				t.Fatalf("force lifted hold or partially activated: err=%v source=%+v candidate=%+v previous=%+v", err, gotSource, gotCandidate, gotPrevious)
			}
		} else if err != nil || gotSource.Metadata["workflow_id"] != candidate.ID || gotCandidate.Type != "task" || gotCandidate.Status != "in_progress" || gotCandidate.Metadata[beadmeta.RoutedToMetadataKey] != "worker-a" || gotCandidate.Metadata[beadmeta.AttachFencePendingMetadataKey] != "" || !gotCandidate.Ephemeral || gotPrevious.Status != "closed" || gotPrevious.Metadata[beadmeta.OutcomeMetadataKey] != beadmeta.OutcomeSkipped {
			t.Fatalf("mixed-tier source/closure/activation did not commit together: err=%v source=%+v candidate=%+v previous=%+v", err, gotSource, gotCandidate, gotPrevious)
		}
	}
}

func TestGuardedNativeFormulaCommittedReceiptResumesOriginalCandidate(t *testing.T) {
	scope, a, b, cfg := guardedNativeFixture(t)
	for _, scenario := range []string{"", "owner", "goal", "labels", "hold", "activated-owner", "activated-goal", "activated-labels", "activated-hold"} {
		activated := strings.HasPrefix(scenario, "activated-")
		change := strings.TrimPrefix(scenario, "activated-")
		source, err := a.Create(beads.Bead{Title: "selected source with interrupted activation", Type: "task", Status: "open", Metadata: map[string]string{beadmeta.DispatchEffectIDMetadataKey: "original-provider-effect", beadmeta.DispatchEffectStateMetadataKey: "committed", beadmeta.DispatchActivationStatusMetadataKey: "open", beadmeta.DispatchActivationAssigneeMetadataKey: ""}})
		if err != nil {
			t.Fatal(err)
		}
		root, err := b.Create(beads.Bead{Title: "original selected candidate", Type: "gate", Status: "open", Ephemeral: true, Metadata: map[string]string{beadmeta.AttachFencePendingMetadataKey: "true", beadmeta.FormulaContractMetadataKey: beadmeta.FormulaContractGraphV2, beadmeta.DispatchEffectIDMetadataKey: "original-provider-effect", beadmeta.DispatchSourceBeadMetadataKey: source.ID, molecule.DeferredTypeMetadataKey: "task", molecule.DeferredRoutedToMetadataKey: "worker-a"}})
		if err != nil {
			t.Fatal(err)
		}
		mapping, err := json.Marshal(map[string]string{"root": root.ID})
		if err != nil {
			t.Fatal(err)
		}
		if err := b.SetMetadata(root.ID, beadmeta.DispatchCandidateIDsMetadataKey, string(mapping)); err != nil {
			t.Fatal(err)
		}
		if err := a.Update(source.ID, beads.UpdateOpts{Metadata: map[string]string{"workflow_id": root.ID, beadmeta.ExecutionRoutedToMetadataKey: "worker-a"}}); err != nil {
			t.Fatal(err)
		}
		selected, err := a.Get(source.ID)
		if err != nil {
			t.Fatal(err)
		}
		original, err := routeConditions(selected, nil)
		if err != nil {
			t.Fatal(err)
		}
		receipt := beads.UpdateOpts{Metadata: map[string]string{beadmeta.DispatchEffectStateMetadataKey: "committed"}}
		if err := freezeActivationConditions(&receipt, original); err != nil {
			t.Fatal(err)
		}
		if err := a.Update(source.ID, receipt); err != nil {
			t.Fatal(err)
		}
		if activated {
			update := molecule.DeferredRoutingActivationUpdate(root)
			if update.Metadata == nil {
				update.Metadata = map[string]string{}
			}
			update.Metadata[beadmeta.AttachFencePendingMetadataKey] = ""
			status := "in_progress"
			update.Status = &status
			if err := b.Update(root.ID, update); err != nil {
				t.Fatal(err)
			}
		}
		switch change {
		case "owner":
			owner := "new-owner"
			if err := b.Update(source.ID, beads.UpdateOpts{Assignee: &owner}); err != nil {
				t.Fatal(err)
			}
		case "goal":
			title := "new human-approved goal"
			if err := b.Update(source.ID, beads.UpdateOpts{Title: &title}); err != nil {
				t.Fatal(err)
			}
		case "labels":
			if err := b.Update(source.ID, beads.UpdateOpts{Labels: []string{"private-review"}}); err != nil {
				t.Fatal(err)
			}
		case "hold":
			if err := b.Update(source.ID, beads.UpdateOpts{Metadata: map[string]string{"handoff.conflict_state": "hold"}}); err != nil {
				t.Fatal(err)
			}
		}
		current, err := a.Get(source.ID)
		if err != nil {
			t.Fatal(err)
		}
		conditions, err := routeConditions(current, nil)
		if err != nil {
			t.Fatal(err)
		}
		opts := SlingOpts{BeadOrFormula: source.ID, Target: cfg.Agents[0], OnFormula: "original-formula", Conditions: &conditions}
		deps := SlingDeps{Store: a, GraphStore: b, Cfg: cfg, CityPath: scope}
		result, err := resumeCommittedFormula(opts, deps, current)
		gotRoot, readErr := b.Get(root.ID)
		if readErr != nil {
			t.Fatal(readErr)
		}
		gotSource, readErr := b.Get(source.ID)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if change != "" {
			var conflict *RouteConflictError
			wantType, wantPending := "gate", "true"
			if activated {
				wantType, wantPending = "task", ""
			}
			if !errors.As(err, &conflict) || gotRoot.Type != wantType || gotRoot.Metadata[beadmeta.AttachFencePendingMetadataKey] != wantPending || gotSource.Metadata[beadmeta.DispatchEffectStateMetadataKey] != "committed" || change == "owner" && gotSource.Assignee != "new-owner" || change == "goal" && gotSource.Title != "new human-approved goal" || change == "labels" && !slices.Contains(gotSource.Labels, "private-review") || change == "hold" && gotSource.Metadata["handoff.conflict_state"] != "hold" {
				t.Fatalf("fresh caller replayed stale %s: err=%v root=%+v source=%+v", scenario, err, gotRoot, gotSource)
			}
		} else {
			if err != nil || result.WorkflowID != root.ID || !result.Idempotent || gotRoot.Type != "task" || gotRoot.Status != "in_progress" || gotRoot.Metadata[beadmeta.AttachFencePendingMetadataKey] != "" || gotSource.Metadata[beadmeta.DispatchEffectStateMetadataKey] != "routed" || gotSource.Metadata["workflow_id"] != root.ID {
				t.Fatalf("recovery did not activate the original selected provider: err=%v result=%+v root=%+v source=%+v", err, result, gotRoot, gotSource)
			}
			reconciled, ok, err := reconciledFormulaRoute(opts, deps, &gotSource)
			if err != nil || !ok || !reconciled.Idempotent || reconciled.WorkflowID != root.ID {
				t.Fatalf("accepted provider could not be read-only reconciled: err=%v result=%+v", err, reconciled)
			}
		}
	}
}

func TestGuardedNativeRouteRejectsChangedHumanGoal(t *testing.T) {
	scope, a, b, cfg := guardedNativeFixture(t)
	for _, field := range []string{"title", "description", "acceptance"} {
		work, err := a.Create(beads.Bead{Title: "original approved goal", Description: "original implementation", AcceptanceCriteria: "original acceptance", Type: "task", Status: "open"})
		if err != nil {
			t.Fatal(err)
		}
		guards, err := routeConditions(work, nil)
		if err != nil {
			t.Fatal(err)
		}
		changed := "new human goal"
		switch field {
		case "title":
			err = b.Update(work.ID, beads.UpdateOpts{Title: &changed})
		case "description":
			err = b.Update(work.ID, beads.UpdateOpts{Description: &changed})
		case "acceptance":
			db, openErr := sql.Open("mysql", fmt.Sprintf("root@tcp(%s:%s)/%s?parseTime=true", os.Getenv("BEADS_DOLT_SERVER_HOST"), os.Getenv("BEADS_DOLT_SERVER_PORT"), os.Getenv("BEADS_DOLT_SERVER_DATABASE")))
			if openErr != nil {
				t.Fatal(openErr)
			}
			_, err = db.Exec("UPDATE issues SET acceptance_criteria = ?, row_lock = row_lock + 1, updated_at = NOW() WHERE id = ?", changed, work.ID)
			if closeErr := db.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		err = CommitRoute(context.Background(), a, cfg, scope, RouteRequest{BeadID: work.ID, Target: "worker-a", Conditions: &guards})
		var conflict *RouteConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("changed %s goal must refuse the original route, got %v", field, err)
		}
		got, err := b.Get(work.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Metadata[beadmeta.RoutedToMetadataKey] != "" || field == "title" && got.Title != changed || field == "description" && got.Description != changed || field == "acceptance" && got.AcceptanceCriteria != changed {
			t.Fatalf("stale route overwrote human %s edits: %+v", field, got)
		}
	}
}

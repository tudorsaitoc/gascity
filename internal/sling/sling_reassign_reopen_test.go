package sling

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/formula"
	"github.com/gastownhall/gascity/internal/molecule"
	"github.com/gastownhall/gascity/internal/runtime"
)

// orderClaimedPoolHandoffSetup builds a sling against a worker pool for a bead
// an order has already claimed (status=in_progress, assignee=order:<name>),
// reproducing the gastownhall/gascity#3231 starting state. The agent is a
// multi-session pool in a rig so the bead is routed to the pool's claim queue
// rather than a single named session. MemStore.Create forces status=open, so
// the in_progress/assignee state is applied via a follow-up Update.
func orderClaimedPoolHandoffSetup(t *testing.T) (SlingOpts, SlingDeps, beads.Bead) {
	t.Helper()
	runner := newFakeRunner()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test"},
		Rigs: []config.Rig{
			{Name: "myrig", Path: "/myrig", Prefix: "gc"},
		},
	}
	a := config.Agent{Name: "polecat", Dir: "myrig", MaxActiveSessions: intPtr(2)}
	deps := testDeps(cfg, runtime.NewFake(), runner.run)
	bead, err := deps.Store.Create(beads.Bead{Title: "hotspot work", Type: "task"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	inProgress, orderActor := "in_progress", "order:mol-dog-jsonl"
	if err := deps.Store.Update(bead.ID, beads.UpdateOpts{Status: &inProgress, Assignee: &orderActor}); err != nil {
		t.Fatalf("Update to order-claimed state: %v", err)
	}
	opts := SlingOpts{Target: a, BeadOrFormula: bead.ID, NoFormula: true, Reassign: true}
	return opts, deps, bead
}

// TestDoSling_Reassign_ReopensOrderClaimedBead is the regression test for
// gastownhall/gascity#3231. An order runs `bd update --claim` on a bead
// (status=in_progress, assignee=order:<name>) and then slings it to a worker
// pool with --reassign. Clearing the assignee alone is not enough: the bead
// stays in_progress, and IsReadyCandidate (which requires status=open) filters
// it out, so no pool worker ever claims it — "work looks in progress, but no
// polecat actually owns it." --reassign must reopen the bead so the target
// pool can claim it.
func TestDoSling_Reassign_ReopensOrderClaimedBead(t *testing.T) {
	for _, tc := range []struct {
		name   string
		routed bool
		batch  bool
	}{{"unrouted", false, false}, {"already routed", true, false},
		{"convoy", false, true}, {"already routed convoy", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			opts, deps, bead := orderClaimedPoolHandoffSetup(t)
			if tc.routed {
				if err := deps.Store.SetMetadata(bead.ID, "gc.routed_to", opts.Target.QualifiedName()); err != nil {
					t.Fatal(err)
				}
			}
			if tc.batch {
				convoy, err := deps.Store.Create(beads.Bead{Title: "handoff convoy", Type: "convoy"})
				if err != nil {
					t.Fatal(err)
				}
				if err := deps.Store.DepAdd(convoy.ID, bead.ID, "tracks"); err != nil {
					t.Fatal(err)
				}
				if tc.routed {
					if err := deps.Store.SetMetadata(convoy.ID, "gc.routed_to", opts.Target.QualifiedName()); err != nil {
						t.Fatal(err)
					}
				}
				opts.BeadOrFormula = convoy.ID
			}
			var err error
			if tc.batch {
				_, err = DoSlingBatch(opts, deps, deps.Store)
			} else {
				_, err = DoSling(opts, deps, deps.Store)
			}
			if err != nil {
				t.Fatalf("sling --reassign: %v", err)
			}
			got, err := deps.Store.Get(bead.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Assignee != "" || got.Status != "open" || got.Metadata["gc.routed_to"] != opts.Target.QualifiedName() {
				t.Fatalf("explicit reassignment did not transfer claimable work: %+v", got)
			}
		})
	}
}

// Reassignment is an ownership transfer, not permission to dispatch blocked work.
func TestDoSling_Reassign_RefusesBlockedWork(t *testing.T) {
	runner := newFakeRunner()
	cfg := &config.City{
		Workspace: config.Workspace{Name: "test"},
		Rigs:      []config.Rig{{Name: "myrig", Path: "/myrig", Prefix: "gc"}},
	}
	a := config.Agent{Name: "polecat", Dir: "myrig", MaxActiveSessions: intPtr(2)}
	deps := testDeps(cfg, runtime.NewFake(), runner.run)
	bead, err := deps.Store.Create(beads.Bead{Title: "blocked work", Type: "task"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	blocked, orderActor := "blocked", "order:mol-dog-jsonl"
	if err := deps.Store.Update(bead.ID, beads.UpdateOpts{Status: &blocked, Assignee: &orderActor}); err != nil {
		t.Fatalf("Update to blocked state: %v", err)
	}
	opts := SlingOpts{Target: a, BeadOrFormula: bead.ID, NoFormula: true, Reassign: true}
	_, err = DoSling(opts, deps, nil)
	var conflict *RouteConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("blocked reassignment must refuse routing, got %v", err)
	}
	got, err := deps.Store.Get(bead.ID)
	if err != nil {
		t.Fatalf("store.Get(%s): %v", bead.ID, err)
	}
	if got.Assignee != orderActor || got.Metadata["gc.routed_to"] != "" || len(runner.calls) != 0 {
		t.Fatalf("refused reassignment mutated ownership or delivered work: %+v calls=%v", got, runner.calls)
	}
	if got.Status != "blocked" {
		t.Errorf("Status = %q, want blocked (reopen must only apply to in_progress beads)", got.Status)
	}
}

// Standalone formula dispatch selects its own source root. Reassignment must
// never transfer ownership of a work bead with the same ID as the formula name.
func TestDoSling_ReassignFormula_DoesNotReopenCollidingBead(t *testing.T) {
	runner := newFakeRunner()
	sp := runtime.NewFake()
	cfg := &config.City{Workspace: config.Workspace{Name: "test-city"}}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	deps := testDeps(cfg, sp, runner.run)
	// Seed an independently owned work bead whose ID is the formula name.
	deps.Store = seededStore("code-review")
	inProgress, orderActor := "in_progress", "order:mol-dog-jsonl"
	if err := deps.Store.Update("code-review", beads.UpdateOpts{Status: &inProgress, Assignee: &orderActor}); err != nil {
		t.Fatalf("Update colliding bead to order-claimed state: %v", err)
	}

	result, err := DoSling(SlingOpts{
		Target:        a,
		BeadOrFormula: "code-review",
		IsFormula:     true,
		Reassign:      true,
	}, deps, nil)
	if err != nil {
		t.Fatalf("DoSling formula launch with --reassign: %v", err)
	}
	if result.Method != "formula" {
		t.Errorf("Method = %q, want formula (standalone formula launch)", result.Method)
	}

	got, err := deps.Store.Get("code-review")
	if err != nil {
		t.Fatalf("store.Get(code-review): %v", err)
	}
	if got.Assignee != orderActor {
		t.Errorf("Assignee = %q, want %q — a standalone formula launch must not reopen a bead sharing the formula name", got.Assignee, orderActor)
	}
	if got.Status != "in_progress" {
		t.Errorf("Status = %q, want in_progress — a standalone formula launch must not reopen a bead sharing the formula name", got.Status)
	}
}

func TestReassignmentRejectsOriginalSnapshotDrift(t *testing.T) {
	changed := "changed"
	for _, tc := range []struct {
		name   string
		update beads.UpdateOpts
	}{
		{"owner", beads.UpdateOpts{Assignee: &changed}},
		{"status", beads.UpdateOpts{Status: &changed}},
		{"goal", beads.UpdateOpts{Title: &changed}},
		{"description", beads.UpdateOpts{Description: &changed}},
		{"acceptance", beads.UpdateOpts{}},
		{"labels", beads.UpdateOpts{Labels: []string{"private-review"}}},
		{"hold", beads.UpdateOpts{Metadata: map[string]string{"handoff.conflict_state": "hold"}}},
		{"route", beads.UpdateOpts{Metadata: map[string]string{"gc.routed_to": "other-worker"}}},
		{"selected provider", beads.UpdateOpts{Metadata: map[string]string{"workflow_id": "other-provider"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts, deps, bead := orderClaimedPoolHandoffSetup(t)
			original, err := deps.Store.Get(bead.ID)
			if err != nil {
				t.Fatal(err)
			}
			if tc.name == "acceptance" {
				original.AcceptanceCriteria = changed
			}
			conditions, err := routeConditions(original, nil)
			if err != nil {
				t.Fatal(err)
			}
			if tc.name != "acceptance" {
				if err := deps.Store.Update(bead.ID, tc.update); err != nil {
					t.Fatal(err)
				}
			}
			_, err = DoSling(SlingOpts{
				Target: opts.Target, BeadOrFormula: bead.ID, NoFormula: true,
				Reassign: true, Conditions: &conditions,
			}, deps, deps.Store)
			var conflict *RouteConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("stale native reassignment admitted %s: %v", tc.name, err)
			}
			current, err := deps.Store.Get(bead.ID)
			if err != nil {
				t.Fatal(err)
			}
			if current.Assignee == "" || current.Status == "open" {
				t.Fatalf("stale reassignment cleared or reopened current ownership: %+v", current)
			}
		})
	}
}

func writeCustomDeliveryGraphFormula(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "graph-work.formula.toml"), []byte(`
formula = "graph-work"
version = 2
contract = "graph.v2"

[[steps]]
id = "step"
title = "Do work"
`), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestStagedFormulaReplayPreservesOriginalProviderIntent(t *testing.T) {
	for _, scenario := range []string{"same", "target", "formula", "committed-same", "committed-target", "committed-formula"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			writeCustomDeliveryGraphFormula(t, dir)
			if err := os.WriteFile(filepath.Join(dir, "other-work.formula.toml"), []byte(`
formula = "other-work"
version = 2
contract = "graph.v2"
[[steps]]
id = "step"
title = "Other work"
`), 0o644); err != nil {
				t.Fatal(err)
			}
			cfg := graphV2SlingTestConfig(t, dir)
			target := config.Agent{Name: "worker-a", MaxActiveSessions: intPtr(1)}
			other := config.Agent{Name: "worker-b", MaxActiveSessions: intPtr(1)}
			cfg.Agents = append(cfg.Agents, target, other)
			deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
			deps.CityPath = t.TempDir()
			admitCustomDeliveryFixture(t, deps.CityPath)
			source, err := deps.Store.Create(beads.Bead{
				Title: "original goal", Description: "original instructions",
				AcceptanceCriteria: "original acceptance",
				Metadata:           map[string]string{DispatchEffectIDKey: "original-effect", DispatchEffectStateKey: "attempted"},
			})
			if err != nil {
				t.Fatal(err)
			}
			conditions, err := routeConditions(source, nil)
			if err != nil {
				t.Fatal(err)
			}
			opts := SlingOpts{Target: target, BeadOrFormula: source.ID, OnFormula: "graph-work", Conditions: &conditions, NoConvoy: true}
			inv, _, err := prepareGraphV2FormulaInvocation(context.Background(), opts.OnFormula, source.ID, opts, deps, target)
			if err != nil {
				t.Fatal(err)
			}
			recipe, err := formula.CompileWithoutRuntimeVarValidation(context.Background(), opts.OnFormula, SlingFormulaSearchPaths(deps, target), inv.Vars)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := encodeRouteConditions(conditions)
			if err != nil {
				t.Fatal(err)
			}
			recipe.Steps[0].Metadata[beadmeta.AttachFencePendingMetadataKey] = "true"
			recipe.Steps[0].Metadata[DispatchEffectIDKey] = "original-effect"
			recipe.Steps[0].Metadata["gc.dispatch_source_bead"] = source.ID
			recipe.Steps[0].Metadata[dispatchSourceConditionsKey] = raw
			recipe.Steps[0].Metadata[dispatchTargetKey] = target.Name
			candidate, err := InstantiateCompiledSlingFormula(context.Background(), recipe, opts.OnFormula,
				molecule.Options{Vars: inv.Vars, DeferAssignees: true, IdempotencyKey: "original-effect"},
				source.ID, opts.ScopeKind, opts.ScopeRef, target, deps)
			if err != nil {
				t.Fatal(err)
			}
			mapping, err := json.Marshal(candidate.IDMapping)
			if err != nil {
				t.Fatal(err)
			}
			if err := deps.graphStore().SetMetadata(candidate.RootID, dispatchCandidateIDsKey, string(mapping)); err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(scenario, "committed-") {
				update := beads.UpdateOpts{Metadata: map[string]string{
					DispatchEffectStateKey:                "committed",
					"workflow_id":                         candidate.RootID,
					beadmeta.ExecutionRoutedToMetadataKey: target.Name,
					"gc.dispatch_activation_status":       source.Status,
					"gc.dispatch_activation_assignee":     source.Assignee,
				}}
				if err := freezeActivationConditions(&update, conditions); err != nil {
					t.Fatal(err)
				}
				if err := deps.Store.Update(source.ID, update); err != nil {
					t.Fatal(err)
				}
				current, err := deps.Store.Get(source.ID)
				if err != nil {
					t.Fatal(err)
				}
				selected, err := routeConditions(current, nil)
				if err != nil {
					t.Fatal(err)
				}
				opts.Conditions = &selected
			}
			intent := strings.TrimPrefix(scenario, "committed-")
			originalSource, _ := deps.Store.Get(source.ID)
			originalRoot, _ := deps.graphStore().Get(candidate.RootID)
			switch intent {
			case "target":
				opts.Target = other
			case "formula":
				opts.OnFormula = "other-work"
			}
			result, err := DoSling(opts, deps, deps.Store)
			currentSource, sourceErr := deps.Store.Get(source.ID)
			currentRoot, rootErr := deps.graphStore().Get(candidate.RootID)
			if sourceErr != nil || rootErr != nil {
				t.Fatalf("read recovered effect: source=%v provider=%v", sourceErr, rootErr)
			}
			if intent == "same" {
				if err != nil || result.WorkflowID != candidate.RootID || currentSource.Metadata["workflow_id"] != candidate.RootID ||
					currentSource.Metadata[beadmeta.ExecutionRoutedToMetadataKey] != target.Name ||
					currentRoot.Metadata[beadmeta.AttachFencePendingMetadataKey] != "" {
					t.Fatalf("original staged provider did not resume: result=%+v err=%v source=%+v root=%+v", result, err, currentSource, currentRoot)
				}
				return
			}
			var conflict *RouteConflictError
			if !errors.As(err, &conflict) || !reflect.DeepEqual(currentSource, originalSource) || !reflect.DeepEqual(currentRoot, originalRoot) {
				t.Fatalf("changed %s selected or activated the original provider: result=%+v err=%v source=%+v root=%+v", scenario, result, err, currentSource, currentRoot)
			}
		})
	}
}

func TestCustomDeliveryReservesTheOriginalEffectBeforeActivation(t *testing.T) {
	for _, tc := range []struct {
		name, formula              string
		standalone, originalEffect bool
	}{
		{"plain", "", false, false},
		{"guarded plain", "", false, true},
		{"attached wisp", "code-review", false, false},
		{"attached graph", "graph-work", false, false},
		{"guarded attached graph", "graph-work", false, true},
		{"standalone wisp", "code-review", true, false},
		{"standalone graph", "graph-work", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			formulaDir := t.TempDir()
			writeCustomDeliveryGraphFormula(t, formulaDir)
			cfg := graphV2SlingTestConfig(t, formulaDir)
			cfg.FormulaLayers.City = append(cfg.FormulaLayers.City, sharedTestFormulaDir)
			target := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1), SlingQuery: "custom-dispatch {}"}
			cfg.Agents = append(cfg.Agents, target)
			deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
			deps.CityPath = t.TempDir()
			admitCustomDeliveryFixture(t, deps.CityPath)
			source, err := deps.Store.Create(beads.Bead{Title: "original goal", Type: "task", Description: "original instructions", AcceptanceCriteria: "original acceptance"})
			if err != nil {
				t.Fatal(err)
			}
			sourceID := source.ID
			opts := SlingOpts{Target: target, BeadOrFormula: sourceID, OnFormula: tc.formula, Reassign: true, NoConvoy: true}
			if tc.standalone {
				sourceID = ""
				opts.BeadOrFormula, opts.OnFormula, opts.IsFormula = tc.formula, "", true
			} else {
				inProgress, owner := "in_progress", "order:original"
				if err := deps.Store.Update(sourceID, beads.UpdateOpts{Status: &inProgress, Assignee: &owner}); err != nil {
					t.Fatal(err)
				}
			}
			if tc.originalEffect {
				if err := deps.Store.Update(sourceID, beads.UpdateOpts{Metadata: map[string]string{DispatchEffectIDKey: "custom-original", DispatchEffectStateKey: "attempted"}}); err != nil {
					t.Fatal(err)
				}
				original, err := deps.Store.Get(sourceID)
				if err != nil {
					t.Fatal(err)
				}
				conditions, err := routeConditions(original, nil)
				if err != nil {
					t.Fatal(err)
				}
				opts.Conditions = &conditions
			}
			deliveries := 0
			deps.Runner = func(_, _ string, env map[string]string) (string, error) {
				deliveries++
				if sourceID == "" {
					roots, err := deps.Store.ListByMetadata(map[string]string{DispatchEffectIDKey: env["GC_SLING_EFFECT_ID"]}, 0, beads.WithBothTiers)
					if err != nil {
						t.Fatal(err)
					}
					for _, root := range roots {
						if root.Metadata["gc.dispatch_source_bead"] == root.ID {
							sourceID = root.ID
						}
					}
				}
				current, err := deps.Store.Get(sourceID)
				if err != nil {
					t.Fatal(err)
				}
				if current.Metadata[DispatchEffectStateKey] != "attempted" ||
					current.Metadata[DispatchEffectIDKey] == "" || current.Metadata[DispatchEffectIDKey] != env["GC_SLING_EFFECT_ID"] ||
					current.Assignee != "" || current.Status != "open" {
					t.Fatalf("provider ran before native original-effect reservation: %+v env=%v", current, env)
				}
				if tc.formula != "" {
					rootID := current.Metadata["workflow_id"]
					if rootID == "" {
						rootID = current.Metadata[beadmeta.MoleculeIDMetadataKey]
					}
					if tc.standalone {
						rootID = sourceID
					}
					root, err := deps.graphStore().Get(rootID)
					if err != nil {
						t.Fatal(err)
					}
					if root.Metadata[beadmeta.AttachFencePendingMetadataKey] == "" {
						t.Fatalf("selected provider lost its pending fence before acknowledgment: %+v", root)
					}
					var candidates map[string]string
					if err := json.Unmarshal([]byte(root.Metadata[dispatchCandidateIDsKey]), &candidates); err != nil {
						t.Fatal(err)
					}
					ready, err := deps.graphStore().Ready()
					if err != nil {
						t.Fatal(err)
					}
					for _, runnable := range ready {
						if candidates[runnable.Metadata[beadmeta.StepIDMetadataKey]] == runnable.ID {
							t.Fatalf("selected candidate became runnable before acknowledgment: %+v", runnable)
						}
					}
				}
				return "accepted", nil
			}
			result, err := DoSling(opts, deps, deps.Store)
			if err != nil {
				t.Fatal(err)
			}
			current, err := deps.Store.Get(sourceID)
			if err != nil {
				t.Fatal(err)
			}
			if deliveries != 1 || current.Metadata[DispatchEffectStateKey] != "routed" {
				t.Fatalf("acknowledged original effect did not complete: deliveries=%d source=%+v", deliveries, current)
			}
			if tc.originalEffect && current.Metadata[DispatchEffectIDKey] != "custom-original" {
				t.Fatalf("custom delivery replaced the original proposed effect identity: %+v", current.Metadata)
			}
			if !tc.standalone && (current.Title != source.Title || current.Description != source.Description || current.AcceptanceCriteria != source.AcceptanceCriteria) {
				t.Fatalf("reassignment changed the original goal: %+v", current)
			}
			rootID := result.WorkflowID
			if rootID == "" {
				rootID = result.WispRootID
			}
			if rootID != "" {
				root, err := deps.graphStore().Get(rootID)
				if err != nil {
					t.Fatal(err)
				}
				if root.Type == "gate" || root.Metadata[beadmeta.AttachFencePendingMetadataKey] != "" {
					t.Fatalf("acknowledged provider remained gated: %+v", root)
				}
			}
		})
	}
}

func TestUnknownCustomDeliveryNeverReplaysOrSubstitutesProvider(t *testing.T) {
	for _, standalone := range []bool{false, true} {
		t.Run(map[bool]string{false: "attached", true: "standalone"}[standalone], func(t *testing.T) {
			formulaDir := t.TempDir()
			writeCustomDeliveryGraphFormula(t, formulaDir)
			cfg := graphV2SlingTestConfig(t, formulaDir)
			target := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1), SlingQuery: "custom-dispatch {}"}
			cfg.Agents = append(cfg.Agents, target)
			deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
			deps.CityPath = t.TempDir()
			admitCustomDeliveryFixture(t, deps.CityPath)
			source, err := deps.Store.Create(beads.Bead{Title: "original goal", Type: "task"})
			if err != nil {
				t.Fatal(err)
			}
			opts := SlingOpts{Target: target, BeadOrFormula: source.ID, OnFormula: "graph-work", NoConvoy: true}
			if standalone {
				opts.BeadOrFormula, opts.OnFormula, opts.IsFormula = "graph-work", "", true
			}
			deliveries, lostAck := 0, errors.New("provider accepted delivery but acknowledgment was lost")
			deps.Runner = func(_, _ string, _ map[string]string) (string, error) {
				deliveries++
				return "", lostAck
			}
			result, err := DoSling(opts, deps, deps.Store)
			if !errors.Is(err, lostAck) {
				t.Fatalf("lost acknowledgment was not preserved: %v", err)
			}
			sourceID := source.ID
			if standalone {
				sourceID = result.WorkflowID
			}
			selected, err := deps.Store.Get(sourceID)
			if err != nil {
				t.Fatal(err)
			}
			effectID := selected.Metadata[DispatchEffectIDKey]
			if effectID == "" || selected.Metadata[DispatchEffectStateKey] != "unknown" {
				t.Fatalf("ambiguous delivery was reported as completed: %+v", selected.Metadata)
			}
			for _, substitute := range []bool{false, true} {
				retry := opts
				if substitute {
					retry.Target.SlingQuery = "different-dispatch {}"
				}
				_, err := DoSling(retry, deps, deps.Store)
				var conflict *RouteConflictError
				if !errors.As(err, &conflict) {
					t.Fatalf("unresolved original effect admitted retry/substitution=%v: %v", substitute, err)
				}
			}
			after, err := deps.Store.Get(sourceID)
			if err != nil {
				t.Fatal(err)
			}
			root, err := deps.graphStore().Get(result.WorkflowID)
			if err != nil {
				t.Fatal(err)
			}
			if deliveries != 1 || after.Metadata[DispatchEffectIDKey] != effectID || after.Metadata[DispatchEffectStateKey] != "unknown" ||
				root.Type != "gate" || root.Metadata[beadmeta.AttachFencePendingMetadataKey] == "" {
				t.Fatalf("unknown original delivery was replayed or activated: deliveries=%d source=%+v root=%+v", deliveries, after, root)
			}
		})
	}
}

func TestCustomAcknowledgmentRecoversOnlyTheSelectedPendingRoot(t *testing.T) {
	formulaDir := t.TempDir()
	writeGraphV2ConvoyFormula(t, formulaDir)
	cfg := graphV2SlingTestConfig(t, formulaDir)
	target := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1), SlingQuery: "custom-dispatch {}"}
	cfg.Agents = append(cfg.Agents, target)
	deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	deps.CityPath = t.TempDir()
	admitCustomDeliveryFixture(t, deps.CityPath)
	graph := &promoteFailingStore{Store: beads.NewMemStore(), armed: true}
	deps.GraphStore = graph
	source, err := deps.Store.Create(beads.Bead{Title: "original goal", Type: "task"})
	if err != nil {
		t.Fatal(err)
	}
	deliveries := 0
	deps.Runner = func(_, _ string, _ map[string]string) (string, error) {
		deliveries++
		return "accepted", nil
	}
	opts := SlingOpts{Target: target, BeadOrFormula: source.ID, OnFormula: "graph-work", NoConvoy: true}
	result, err := DoSling(opts, deps, deps.Store)
	if !errors.Is(err, errRefusedPromotion) {
		t.Fatalf("activation refusal was not preserved: %v", err)
	}
	selected, err := deps.Store.Get(source.ID)
	if err != nil {
		t.Fatal(err)
	}
	effectID := selected.Metadata[DispatchEffectIDKey]
	if selected.Metadata[DispatchEffectStateKey] != "routed" || selected.Metadata["workflow_id"] != result.WorkflowID ||
		selected.Metadata[beadmeta.RoutedToMetadataKey] != "" || selected.Status != "open" || selected.Title != source.Title {
		t.Fatalf("actual acknowledgment did not retain its original selected candidate: %+v", selected.Metadata)
	}
	graph.armed = false
	recovered, err := DoSling(opts, deps, deps.Store)
	if err != nil {
		t.Fatal(err)
	}
	root, err := graph.Get(result.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	after, err := deps.Store.Get(source.ID)
	if err != nil {
		t.Fatal(err)
	}
	if deliveries != 1 || !recovered.Idempotent || recovered.WorkflowID != result.WorkflowID ||
		after.Metadata[DispatchEffectIDKey] != effectID || after.Metadata[DispatchEffectStateKey] != "routed" ||
		after.Metadata[beadmeta.RoutedToMetadataKey] != "" || after.Status != "open" || after.Title != source.Title ||
		root.Type != "task" || root.Status != "in_progress" || root.Metadata[beadmeta.AttachFencePendingMetadataKey] != "" {
		t.Fatalf("acknowledgment recovery redelivered or replaced the original provider: deliveries=%d result=%+v source=%+v root=%+v", deliveries, recovered, after, root)
	}
}

func TestCustomAcknowledgmentCannotReplaceChangedSourceConditions(t *testing.T) {
	changed := "changed"
	for _, tc := range []struct {
		name   string
		update beads.UpdateOpts
	}{
		{"owner", beads.UpdateOpts{Assignee: &changed}},
		{"goal", beads.UpdateOpts{Title: &changed}},
		{"labels", beads.UpdateOpts{Labels: []string{"private-review"}}},
		{"hold", beads.UpdateOpts{Metadata: map[string]string{"handoff.conflict_state": "hold"}}},
		{"selected provider", beads.UpdateOpts{Metadata: map[string]string{"workflow_id": "changed-provider"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			formulaDir := t.TempDir()
			writeGraphV2ConvoyFormula(t, formulaDir)
			cfg := graphV2SlingTestConfig(t, formulaDir)
			target := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1), SlingQuery: "custom-dispatch {}"}
			cfg.Agents = append(cfg.Agents, target)
			deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
			deps.CityPath = t.TempDir()
			admitCustomDeliveryFixture(t, deps.CityPath)
			source, err := deps.Store.Create(beads.Bead{Title: "original goal", Type: "task"})
			if err != nil {
				t.Fatal(err)
			}
			deliveries := 0
			var expected beads.Bead
			deps.Runner = func(_, _ string, _ map[string]string) (string, error) {
				deliveries++
				if err := deps.Store.Update(source.ID, tc.update); err != nil {
					t.Fatal(err)
				}
				var err error
				expected, err = deps.Store.Get(source.ID)
				if err != nil {
					t.Fatal(err)
				}
				return "accepted", nil
			}
			opts := SlingOpts{Target: target, BeadOrFormula: source.ID, OnFormula: "graph-work", NoConvoy: true}
			result, err := DoSling(opts, deps, deps.Store)
			var conflict *RouteConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("acknowledgment replaced a changed source %s: %v", tc.name, err)
			}
			if _, err := DoSling(opts, deps, deps.Store); !errors.As(err, &conflict) {
				t.Fatalf("changed original source was admitted on retry: %v", err)
			}
			current, err := deps.Store.Get(source.ID)
			if err != nil {
				t.Fatal(err)
			}
			root, err := deps.graphStore().Get(result.WorkflowID)
			if err != nil {
				t.Fatal(err)
			}
			if deliveries != 1 || current.Title != expected.Title || current.Assignee != expected.Assignee ||
				!slices.Equal(current.Labels, expected.Labels) || current.Metadata["handoff.conflict_state"] != expected.Metadata["handoff.conflict_state"] ||
				current.Metadata[DispatchEffectIDKey] != expected.Metadata[DispatchEffectIDKey] || current.Metadata[DispatchEffectStateKey] != "attempted" ||
				root.Type != "gate" || root.Metadata[beadmeta.AttachFencePendingMetadataKey] == "" {
				t.Fatalf("source drift was overwritten or its effect replayed/activated: deliveries=%d source=%+v root=%+v", deliveries, current, root)
			}
		})
	}
}

func TestCustomDeliveryCannotAdoptAForeignUnknownEffect(t *testing.T) {
	for _, formula := range []string{"", "code-review"} {
		t.Run(map[bool]string{false: "plain", true: "formula"}[formula != ""], func(t *testing.T) {
			target := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1), SlingQuery: "custom-dispatch {}"}
			cfg := &config.City{Workspace: config.Workspace{Name: "test"}, Agents: []config.Agent{target}}
			runner := newFakeRunner()
			deps := testDeps(cfg, runtime.NewFake(), runner.run)
			deps.CityPath = t.TempDir()
			admitCustomDeliveryFixture(t, deps.CityPath)
			source, err := deps.Store.Create(beads.Bead{Title: "original goal", Type: "task", Metadata: map[string]string{
				DispatchEffectIDKey: "unknown-original", DispatchEffectStateKey: "unknown",
			}})
			if err != nil {
				t.Fatal(err)
			}
			conditions, err := routeConditions(source, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = DoSling(SlingOpts{
				Target: target, BeadOrFormula: source.ID, OnFormula: formula,
				Conditions: &conditions, NoConvoy: true,
			}, deps, deps.Store)
			var conflict *RouteConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("unknown effect without its selected provider was adopted: %v", err)
			}
			current, err := deps.Store.Get(source.ID)
			if err != nil {
				t.Fatal(err)
			}
			if len(runner.calls) != 0 || current.Metadata[DispatchEffectIDKey] != "unknown-original" ||
				current.Metadata[DispatchEffectStateKey] != "unknown" || current.Metadata[customDispatchProviderKey] != "" ||
				current.Metadata["workflow_id"] != "" || current.Metadata[beadmeta.MoleculeIDMetadataKey] != "" {
				t.Fatalf("foreign unknown effect was delivered, replaced, or fabricated: source=%+v calls=%v", current, runner.calls)
			}
		})
	}
}

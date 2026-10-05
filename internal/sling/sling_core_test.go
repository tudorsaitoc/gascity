package sling

import (
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
)

// Both formula entry points must atomically select the attachment on the source
// and make its root runnable, while retaining their public result method.
func TestAttachFormulaToBeadEntryShapes(t *testing.T) {
	newDeps := func(t *testing.T) (SlingDeps, string) {
		t.Helper()
		cfg := &config.City{Workspace: config.Workspace{Name: "test"}}
		deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
		b, err := deps.Store.Create(beads.Bead{Title: "work", Type: "task", Status: "open"})
		if err != nil {
			t.Fatal(err)
		}
		return deps, b.ID
	}

	t.Run("on-formula success", func(t *testing.T) {
		deps, beadID := newDeps(t)
		a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
		result, err := DoSling(SlingOpts{Target: a, BeadOrFormula: beadID, OnFormula: "code-review"}, deps, deps.Store)
		if err != nil {
			t.Fatalf("DoSling on-formula: %v", err)
		}
		if result.Method != "on-formula" {
			t.Errorf("Method = %q, want on-formula", result.Method)
		}
		if result.FormulaName != "code-review" {
			t.Errorf("FormulaName = %q, want code-review", result.FormulaName)
		}
		source, err := deps.Store.Get(beadID)
		if err != nil {
			t.Fatal(err)
		}
		root, err := deps.Store.Get(result.WispRootID)
		if err != nil {
			t.Fatal(err)
		}
		if source.Metadata["molecule_id"] != root.ID || source.Metadata["gc.routed_to"] != a.Name || root.Metadata["gc.attach_fence_pending"] != "" {
			t.Fatalf("attachment did not activate with its source route: source=%+v root=%+v", source, root)
		}
	})

	t.Run("default-formula success", func(t *testing.T) {
		deps, beadID := newDeps(t)
		a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1), DefaultSlingFormula: stringPtr("code-review")}
		result, err := DoSling(SlingOpts{Target: a, BeadOrFormula: beadID}, deps, deps.Store)
		if err != nil {
			t.Fatalf("DoSling default-formula: %v", err)
		}
		if result.Method != "default-on-formula" {
			t.Errorf("Method = %q, want default-on-formula", result.Method)
		}
		if result.FormulaName != "code-review" {
			t.Errorf("FormulaName = %q, want code-review", result.FormulaName)
		}
		source, err := deps.Store.Get(beadID)
		if err != nil {
			t.Fatal(err)
		}
		root, err := deps.Store.Get(result.WispRootID)
		if err != nil {
			t.Fatal(err)
		}
		if source.Metadata["molecule_id"] != root.ID || source.Metadata["gc.routed_to"] != a.Name || root.Metadata["gc.attach_fence_pending"] != "" {
			t.Fatalf("attachment did not activate with its source route: source=%+v root=%+v", source, root)
		}
	})
}

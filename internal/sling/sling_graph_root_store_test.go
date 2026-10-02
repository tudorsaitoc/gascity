package sling

import (
	"errors"
	"testing"

	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/beads/splittest"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/runtime"
)

// Source selection and graph activation must respect distinct work and graph
// owners, including forced replacement and recovery after activation failure.

// splitSlingDeps returns deps whose graph class is served by a separate,
// prefix-disjoint store — the shape a split city has. The work leaf mints
// "gc-<n>" and the class leaf "gcg-<n>", so a root read from the wrong leg is
// not merely a different row, it is an id the leg cannot mint.
func splitSlingDeps(t *testing.T, cfg *config.City) (SlingDeps, beads.Store, beads.Store) {
	t.Helper()
	work, graph := splittest.NewSplitStores(t)
	deps := testDeps(cfg, runtime.NewFake(), newFakeRunner().run)
	deps.Store = work
	deps.GraphStore = graph
	return deps, work, graph
}

// Forced replacement must close the predecessor in the graph owner, without
// manufacturing another work-ledger root or leaving two runnable workflows.
func TestForcedGraphV2ReplacementUsesTheGraphStore(t *testing.T) {
	formulaDir := t.TempDir()
	writeGraphV2ConvoyFormula(t, formulaDir)
	cfg := graphV2SlingTestConfig(t, formulaDir)
	deps, work, graph := splitSlingDeps(t, cfg)
	deps.CityPath = t.TempDir()
	convoy, err := work.Create(beads.Bead{Title: "input", Type: "convoy"})
	if err != nil {
		t.Fatal(err)
	}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}
	first, err := DoSling(SlingOpts{Target: a, BeadOrFormula: convoy.ID, OnFormula: "graph-work"}, deps, work)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DoSling(SlingOpts{Target: a, BeadOrFormula: convoy.ID, OnFormula: "graph-work", Force: true}, deps, work)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := graph.Get(first.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	current, err := graph.Get(second.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	source, err := work.Get(convoy.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.ID == previous.ID || previous.Status != "closed" || current.Status != "in_progress" ||
		current.Metadata[beadmeta.AttachFencePendingMetadataKey] != "" || source.Metadata["workflow_id"] != current.ID || source.Metadata[DispatchEffectStateKey] != "routed" {
		t.Fatalf("replacement did not select exactly one active graph root: previous=%+v current=%+v source=%+v", previous, current, source)
	}
	if _, err := work.Get(current.ID); !errors.Is(err, beads.ErrNotFound) {
		t.Fatalf("graph root unexpectedly exists in the work ledger: %v", err)
	}
}

// Fault injection runs inside the real staged transaction; sequential Update
// overrides cannot model a native activation failure or its rollback.
type promoteFailingStore struct {
	beads.Store
	spare string
	armed bool
}

func (s *promoteFailingStore) UpdateGuarded(id string, opts beads.UpdateOpts, conditions beads.UpdateConditions) (bool, error) {
	writer, ok := beads.GuardedUpdateWriterFor(s.Store)
	if !ok {
		return false, beads.ErrConditionalWriteUnsupported
	}
	return writer.UpdateGuarded(id, opts, conditions)
}

func (s *promoteFailingStore) TxSingle(message string, fn func(beads.Tx) error) error {
	atomic, ok := beads.SingleTransactionStoreFor(s.Store)
	if !ok {
		return beads.ErrConditionalWriteUnsupported
	}
	return atomic.TxSingle(message, func(tx beads.Tx) error {
		writer, ok := tx.(beads.GuardedUpdateWriter)
		if !ok {
			return beads.ErrConditionalWriteUnsupported
		}
		return fn(&promoteFailingTx{Tx: tx, writer: writer, store: s})
	})
}

func (s *promoteFailingStore) GraphApplyHandle() (beads.GraphApplyStore, bool) {
	return beads.GraphApplyFor(s.Store)
}

type promoteFailingTx struct {
	beads.Tx
	writer beads.GuardedUpdateWriter
	store  *promoteFailingStore
}

func (tx *promoteFailingTx) UpdateGuarded(id string, opts beads.UpdateOpts, conditions beads.UpdateConditions) (bool, error) {
	if tx.store.armed && id != tx.store.spare && opts.Status != nil && *opts.Status == "in_progress" {
		return false, errRefusedPromotion
	}
	return tx.writer.UpdateGuarded(id, opts, conditions)
}

var errRefusedPromotion = errorString("refusing the workflow promotion")

type errorString string

func (e errorString) Error() string { return string(e) }

// Cross-owner failure preserves a committed selection of the pending candidate.
// Recovery activates that exact candidate; it must not launch a substitute.
func TestForcedGraphV2ReplacementActivationFailurePreservesAndResumesSelectedRoot(t *testing.T) {
	formulaDir := t.TempDir()
	writeGraphV2ConvoyFormula(t, formulaDir)
	cfg := graphV2SlingTestConfig(t, formulaDir)
	deps, work, graph := splitSlingDeps(t, cfg)
	deps.CityPath = t.TempDir()
	promoteGuard := &promoteFailingStore{Store: graph}
	deps.GraphStore = promoteGuard

	convoy, err := work.Create(beads.Bead{Title: "input", Type: "convoy"})
	if err != nil {
		t.Fatalf("creating the input convoy: %v", err)
	}
	a := config.Agent{Name: "mayor", MaxActiveSessions: intPtr(1)}

	first, err := DoSling(SlingOpts{Target: a, BeadOrFormula: convoy.ID, OnFormula: "graph-work", Force: true}, deps, deps.Store)
	if err != nil {
		t.Fatalf("first forced sling: %v", err)
	}
	if first.WorkflowID == "" {
		t.Fatal("the first forced sling started no workflow; there is nothing for a replacement to displace")
	}
	promoteGuard.spare = first.WorkflowID
	promoteGuard.armed = true

	_, err = DoSling(SlingOpts{Target: a, BeadOrFormula: convoy.ID, OnFormula: "graph-work", Force: true}, deps, deps.Store)
	if !errors.Is(err, errRefusedPromotion) {
		t.Fatalf("replacement must fail its guarded activation, got %v", err)
	}

	restored, err := graph.Get(first.WorkflowID)
	if err != nil {
		t.Fatalf("reading the displaced root back: %v", err)
	}
	if restored.Status != "in_progress" {
		t.Fatalf("predecessor changed despite rolled-back activation: %+v", restored)
	}
	selected, err := work.Get(convoy.ID)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := graph.Get(selected.Metadata["workflow_id"])
	if err != nil {
		t.Fatal(err)
	}
	if pending.ID == first.WorkflowID || pending.Type != "gate" || pending.Metadata[beadmeta.AttachFencePendingMetadataKey] != "true" || selected.Metadata[DispatchEffectStateKey] != "committed" {
		t.Fatalf("failed activation lost its durable selected identity: source=%+v candidate=%+v", selected, pending)
	}
	promoteGuard.armed = false
	recovered, err := DoSling(SlingOpts{Target: a, BeadOrFormula: convoy.ID, OnFormula: "graph-work"}, deps, work)
	if err != nil {
		t.Fatal(err)
	}
	current, err := graph.Get(pending.ID)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := graph.Get(first.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	selected, err = work.Get(convoy.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.WorkflowID != pending.ID || !recovered.Idempotent || current.Status != "in_progress" ||
		current.Metadata[beadmeta.AttachFencePendingMetadataKey] != "" || previous.Status != "closed" || selected.Metadata[DispatchEffectStateKey] != "routed" {
		t.Fatalf("recovery did not activate the original selected graph root: result=%+v source=%+v current=%+v previous=%+v", recovered, selected, current, previous)
	}
}

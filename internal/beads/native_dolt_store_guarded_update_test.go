package beads

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"testing"

	beadslib "github.com/steveyegge/beads"
)

// The unit fixture models state and rollback, not history or isolation. Real
// event attribution and contention are asserted by the SQL integration row.
func (tx nativeDoltTransactionForTest) UpdateIssueWithEvents(ctx context.Context, id string, updates map[string]interface{}, actor string) error {
	opts, err := nativeDoltMemUpdateOpts(updates)
	if err != nil {
		return err
	}
	_, replacesMetadata := updates["metadata"]
	if replacesMetadata && !json.Valid(updates["metadata"].(json.RawMessage)) {
		return fmt.Errorf("metadata is not valid JSON")
	}
	if err := tx.storage.UpdateIssue(ctx, id, updates, actor); err != nil {
		return err
	}
	if memory, ok := tx.storage.(*nativeDoltMemStorage); replacesMetadata && ok {
		memory.store.mu.Lock()
		defer memory.store.mu.Unlock()
		for index := range memory.store.beads {
			if memory.store.beads[index].ID == id {
				memory.store.beads[index].Metadata = opts.Metadata
				break
			}
		}
	}
	return nil
}

func (tx nativeDoltTransactionForTest) GetLabels(ctx context.Context, id string) ([]string, error) {
	issue, err := tx.storage.GetIssue(ctx, id)
	if err != nil {
		return nil, err
	}
	return issue.Labels, nil
}

func (s *nativeDoltMemStorage) RunInSingleTransaction(ctx context.Context, message string, fn func(beadslib.Transaction) error) error {
	return s.RunInTransaction(ctx, message, fn)
}

func TestNativeDoltGuardedUpdateRefusesAnyStaleCondition(t *testing.T) {
	store := newNativeDoltStoreForTest(newNativeDoltMemStorage())
	created, err := store.Create(Bead{Title: "guarded", Description: "current description", AcceptanceCriteria: "current acceptance", Status: "in_progress", Assignee: "refinery", Metadata: map[string]string{"polecat_session": "session-a", "retained": "keep"}})
	if err != nil {
		t.Fatal(err)
	}
	currentStatus := "in_progress"
	if err := store.Update(created.ID, UpdateOpts{Status: &currentStatus}); err != nil {
		t.Fatal(err)
	}
	before, err := store.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	ptr := func(v string) *string { return &v }
	for name, guards := range map[string]UpdateConditions{
		"status":      {Status: ptr("open"), Assignee: ptr("refinery"), Metadata: map[string]string{"polecat_session": "session-a"}},
		"assignee":    {Status: ptr("in_progress"), Assignee: ptr("old-owner"), Metadata: map[string]string{"polecat_session": "session-a"}},
		"session":     {Status: ptr("in_progress"), Assignee: ptr("refinery"), Metadata: map[string]string{"polecat_session": "session-old"}},
		"title":       {Title: ptr("superseded title")},
		"description": {Description: ptr("superseded description")},
		"acceptance":  {AcceptanceCriteria: ptr("superseded acceptance")},
	} {
		t.Run(name, func(t *testing.T) {
			guards.SetMetadataIfAbsent = map[string]string{RefineryDecisionAtKey: "2026-10-01T01:00:00Z"}
			applied, err := store.UpdateGuarded(created.ID, UpdateOpts{Status: ptr("closed"), Assignee: ptr("wrong"), Title: ptr("wrong"), Metadata: map[string]string{"outcome": "wrong"}}, guards)
			if err != nil || applied {
				t.Fatalf("stale guard = (%v, %v)", applied, err)
			}
			got, err := store.Get(created.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, before) {
				t.Fatalf("stale guard mutated bead: got %+v, before %+v", got, before)
			}
		})
	}
}

func TestNativeDoltGuardedUpdatePreservesFirstClockAndMetadata(t *testing.T) {
	store := newNativeDoltStoreForTest(newNativeDoltMemStorage())
	created, err := store.Create(Bead{Title: "clock", Assignee: "worker", Metadata: map[string]string{"polecat_session": "session-a", "retained": "keep", "empty": ""}})
	if err != nil {
		t.Fatal(err)
	}
	ptr := func(v string) *string { return &v }
	first := "2026-10-01T01:00:00Z"
	guards := UpdateConditions{Status: ptr("open"), Assignee: ptr("worker"), Metadata: map[string]string{"polecat_session": "session-a", "absent": "", "empty": ""}, SetMetadataIfAbsent: map[string]string{RefineryDecisionAtKey: first, "retained": "replace"}}
	applied, err := store.UpdateGuarded(created.ID, UpdateOpts{Status: ptr("in_progress"), Assignee: ptr("refinery"), Metadata: map[string]string{"outcome": "pending"}}, guards)
	if err != nil || !applied {
		t.Fatalf("matching update = (%v, %v)", applied, err)
	}
	guards.Status, guards.Assignee = ptr("in_progress"), ptr("refinery")
	guards.SetMetadataIfAbsent[RefineryDecisionAtKey] = "2026-10-01T02:00:00Z"
	if applied, err = store.UpdateGuarded(created.ID, UpdateOpts{Metadata: map[string]string{"outcome": "merged"}}, guards); err != nil || !applied {
		t.Fatalf("retry = (%v, %v)", applied, err)
	}
	if err := store.Update(created.ID, UpdateOpts{Metadata: map[string]string{RefineryDecisionAtKey: "later", "ordinary": "updated"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetMetadataBatch(created.ID, map[string]string{RefineryDecisionAtKey: "", "batch": "updated"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Tx("ordinary transaction", func(tx Tx) error {
		return tx.SetMetadataBatch(created.ID, map[string]string{RefineryDecisionAtKey: "replace", "tx": "updated"})
	}); err != nil {
		t.Fatal(err)
	}
	if swapped, err := store.CompareAndSetMetadataKey(created.ID, RefineryDecisionAtKey, first, "replace"); err != nil || swapped {
		t.Fatalf("clock CAS = (%v, %v)", swapped, err)
	}
	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	wantMetadata := map[string]string{"polecat_session": "session-a", "retained": "keep", "empty": "", RefineryDecisionAtKey: first, "outcome": "merged", "ordinary": "updated", "batch": "updated", "tx": "updated"}
	if got.Status != "in_progress" || got.Assignee != "refinery" || !maps.Equal(got.Metadata, wantMetadata) {
		t.Fatalf("updated bead = %+v, metadata want %#v", got, wantMetadata)
	}
}

func TestNativeDoltGuardedUpdateDeletesOnlyRequestedMetadata(t *testing.T) {
	store := newNativeDoltStoreForTest(newNativeDoltMemStorage())
	created, err := store.Create(Bead{Title: "handoff", Assignee: "refinery", Metadata: map[string]string{
		"polecat_session": "session-a", "refinery_handoff_at": "handoff", "refinery_handoff_contract": "contract",
		RefineryDecisionAtKey: "2026-10-01T01:00:00Z", "retained": "keep",
	}})
	if err != nil {
		t.Fatal(err)
	}
	ptr := func(v string) *string { return &v }
	conditions := UpdateConditions{
		Status: ptr("open"), Assignee: ptr("refinery"), Metadata: map[string]string{"polecat_session": "session-old"},
		UnsetMetadata: []string{"refinery_handoff_at", "refinery_handoff_contract", RefineryDecisionAtKey},
	}
	before, err := store.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if applied, err := store.UpdateGuarded(created.ID, UpdateOpts{Status: ptr("closed")}, conditions); err != nil || applied {
		t.Fatalf("stale unset = (%v, %v)", applied, err)
	}
	unchanged, err := store.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(unchanged, before) {
		t.Fatalf("stale unset removed authority: %+v", unchanged)
	}
	conditions.Metadata["polecat_session"] = "session-a"
	if applied, err := store.UpdateGuarded(created.ID, UpdateOpts{Status: ptr("closed"), Metadata: map[string]string{"outcome": "merged"}}, conditions); err != nil || !applied {
		t.Fatalf("guarded unset = (%v, %v)", applied, err)
	}
	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"refinery_handoff_at", "refinery_handoff_contract"} {
		if _, exists := got.Metadata[key]; exists {
			t.Fatalf("authority key %s remains as a value/tombstone: %#v", key, got.Metadata)
		}
	}
	if got.Status != "closed" || got.Metadata["outcome"] != "merged" || got.Metadata["retained"] != "keep" || got.Metadata[RefineryDecisionAtKey] != before.Metadata[RefineryDecisionAtKey] {
		t.Fatalf("guarded removal lost outcome, clock, or sibling: %+v", got)
	}
}

func TestCachedGuardedUpdateEvictsStaleOwnerAndReturnsCommittedState(t *testing.T) {
	backing := newNativeDoltStoreForTest(newNativeDoltMemStorage())
	created, err := backing.Create(Bead{Title: "cached", Assignee: "worker-a", Metadata: map[string]string{"polecat_session": "session-a", "retained": "keep"}})
	if err != nil {
		t.Fatal(err)
	}
	cache := NewCachingStore(backing, nil)
	if _, err := cache.Get(created.ID); err != nil {
		t.Fatal(err)
	}
	ptr := func(v string) *string { return &v }
	if err := backing.Update(created.ID, UpdateOpts{Assignee: ptr("worker-b"), Metadata: map[string]string{"polecat_session": "session-b"}}); err != nil {
		t.Fatal(err)
	}
	conditions := UpdateConditions{Status: ptr("open"), Assignee: ptr("worker-a"), Metadata: map[string]string{"polecat_session": "session-a"}}
	if applied, err := cache.UpdateGuarded(created.ID, UpdateOpts{Status: ptr("closed")}, conditions); err != nil || applied {
		t.Fatalf("cached stale owner = (%v, %v)", applied, err)
	}
	got, err := cache.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Assignee != "worker-b" || got.Metadata["polecat_session"] != "session-b" || got.Status != "open" {
		t.Fatalf("cached stale guard kept old owner or wrote: %+v", got)
	}
	conditions.Assignee, conditions.Metadata["polecat_session"] = ptr("worker-b"), "session-b"
	if applied, err := cache.UpdateGuarded(created.ID, UpdateOpts{Status: ptr("in_progress"), Assignee: ptr("refinery"), Metadata: map[string]string{"outcome": "submitted"}}, conditions); err != nil || !applied {
		t.Fatalf("cached matching owner = (%v, %v)", applied, err)
	}
	got, err = cache.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Assignee != "refinery" || got.Status != "in_progress" || got.Metadata["outcome"] != "submitted" || got.Metadata["retained"] != "keep" {
		t.Fatalf("cached result does not describe committed guarded update: %+v", got)
	}
}

func TestCachedUpdatesPreserveFirstTerminalClock(t *testing.T) {
	backing := newNativeDoltStoreForTest(newNativeDoltMemStorage())
	first := "2026-10-01T01:00:00Z"
	created, err := backing.Create(Bead{Title: "cached clock", Metadata: map[string]string{RefineryDecisionAtKey: first}})
	if err != nil {
		t.Fatal(err)
	}
	cache := NewCachingStore(backing, nil)
	if _, err := cache.Get(created.ID); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func() error{
		"update": func() error {
			return cache.Update(created.ID, UpdateOpts{Metadata: map[string]string{RefineryDecisionAtKey: "2026-10-01T02:00:00Z", "ordinary": "changed"}})
		},
		"single metadata": func() error {
			return cache.SetMetadata(created.ID, RefineryDecisionAtKey, "")
		},
		"batch metadata": func() error {
			return cache.SetMetadataBatch(created.ID, map[string]string{RefineryDecisionAtKey: "2026-10-01T03:00:00Z", "batch": "changed"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := mutate(); err != nil {
				t.Fatal(err)
			}
			for source, store := range map[string]Store{"cache": cache, "backing": backing} {
				got, err := store.Get(created.ID)
				if err != nil {
					t.Fatal(err)
				}
				if got.Metadata[RefineryDecisionAtKey] != first {
					t.Fatalf("%s reported a changed immutable clock: %+v", source, got)
				}
			}
		})
	}
}

func TestNativeDoltGuardedUpdateChecksExactLabelSnapshot(t *testing.T) {
	store := newNativeDoltStoreForTest(newNativeDoltMemStorage())
	created, err := store.Create(Bead{Title: "labels", Labels: []string{"existing", "hold"}, Assignee: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	stale := []string{"existing"}
	next := "refinery"
	if applied, err := store.UpdateGuarded(created.ID, UpdateOpts{Assignee: &next, RemoveLabels: []string{"hold"}}, UpdateConditions{Labels: &stale}); err != nil || applied {
		t.Fatalf("changed labels = (%v, %v)", applied, err)
	}
	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, before) {
		t.Fatalf("stale label snapshot mutated hold/owner: %+v", got)
	}
	expected := []string{"hold", "existing"}
	if applied, err := store.UpdateGuarded(created.ID, UpdateOpts{Assignee: &next, RemoveLabels: []string{"hold"}}, UpdateConditions{Labels: &expected}); err != nil || !applied {
		t.Fatalf("unordered matching labels = (%v, %v)", applied, err)
	}
	got, err = store.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Assignee != next || !reflect.DeepEqual(got.Labels, []string{"existing"}) {
		t.Fatalf("label mutation and owner did not commit together: %+v", got)
	}
	empty := []string{}
	if applied, err := store.UpdateGuarded(created.ID, UpdateOpts{RemoveLabels: []string{"existing"}}, UpdateConditions{Labels: &empty}); err != nil || applied {
		t.Fatalf("empty label guard ignored existing labels = (%v, %v)", applied, err)
	}
}

func TestCachedGuardedTransactionRollsBackComposedActivation(t *testing.T) {
	backing := newNativeDoltStoreForTest(newNativeDoltMemStorage())
	source, err := backing.Create(Bead{Title: "source", Assignee: "worker", Metadata: map[string]string{"session": "source-a"}})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := backing.Create(Bead{Title: "candidate", Ephemeral: true, Metadata: map[string]string{"session": "candidate-a"}})
	if err != nil {
		t.Fatal(err)
	}
	cache := NewCachingStore(backing, nil)
	if _, err := cache.Get(source.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.Get(candidate.ID); err != nil {
		t.Fatal(err)
	}
	if err := backing.Update(candidate.ID, UpdateOpts{Metadata: map[string]string{"session": "candidate-b"}}); err != nil {
		t.Fatal(err)
	}
	currentCandidate, err := backing.Get(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	lost := errors.New("activation guard lost")
	owner, active := "next-worker", "in_progress"
	activate := func(expectedCandidate string) error {
		return cache.TxSingle("composed guarded activation", func(tx Tx) error {
			writer, ok := tx.(GuardedUpdateWriter)
			if !ok {
				return ErrConditionalWriteUnsupported
			}
			applied, err := writer.UpdateGuarded(source.ID, UpdateOpts{Assignee: &owner, Metadata: map[string]string{"routed_to": owner}}, UpdateConditions{Metadata: map[string]string{"session": "source-a"}})
			if err != nil {
				return err
			}
			if !applied {
				return lost
			}
			applied, err = writer.UpdateGuarded(candidate.ID, UpdateOpts{Status: &active, Assignee: &owner}, UpdateConditions{Metadata: map[string]string{"session": expectedCandidate}})
			if err != nil {
				return err
			}
			if !applied {
				return lost
			}
			return nil
		})
	}
	if err := activate("candidate-a"); !errors.Is(err, lost) {
		t.Fatalf("lost candidate guard = %v", err)
	}
	gotSource, err := cache.Get(source.ID)
	if err != nil {
		t.Fatal(err)
	}
	gotCandidate, err := cache.Get(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotSource, source) || !reflect.DeepEqual(gotCandidate, currentCandidate) {
		t.Fatalf("aborted activation leaked source write or stale cached candidate: source %+v, candidate %+v", gotSource, gotCandidate)
	}
	if err := activate("candidate-b"); err != nil {
		t.Fatal(err)
	}
	gotSource, err = cache.Get(source.ID)
	if err != nil {
		t.Fatal(err)
	}
	gotCandidate, err = cache.Get(candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotSource.Assignee != owner || gotSource.Metadata["routed_to"] != owner || gotCandidate.Status != active || gotCandidate.Assignee != owner || gotCandidate.Metadata["session"] != "candidate-b" {
		t.Fatalf("composed activation did not commit current incarnation: source %+v, candidate %+v", gotSource, gotCandidate)
	}
}

type nativeDoltHiddenEventTransactionForTest struct{ beadslib.Transaction }

func TestNativeDoltGuardedUpdateRefusesMissingEventCapability(t *testing.T) {
	storage := newNativeDoltMemStorage()
	store := newNativeDoltStoreForTest(storage)
	created, err := store.Create(Bead{Title: "unsupported", Assignee: "worker"})
	if err != nil {
		t.Fatal(err)
	}
	next := "refinery"
	tx := nativeDoltHiddenEventTransactionForTest{Transaction: nativeDoltTransactionForTest{storage: storage}}
	applied, err := store.applyGuardedUpdateInTx(context.Background(), tx, created.ID, UpdateOpts{Assignee: &next}, UpdateConditions{})
	if applied || !errors.Is(err, ErrConditionalWriteUnsupported) {
		t.Fatalf("missing event capability = (%v, %v)", applied, err)
	}
	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, created) {
		t.Fatalf("unsupported transaction mutated owner: %+v", got)
	}
}

func TestCachedFirstClockRefreshFailureDoesNotInventReceipt(t *testing.T) {
	for _, operation := range []string{"update", "single", "batch"} {
		t.Run(operation, func(t *testing.T) {
			backing := newNativeDoltStoreForTest(newNativeDoltMemStorage())
			created, err := backing.Create(Bead{Title: "first clock"})
			if err != nil {
				t.Fatal(err)
			}
			fault := &releaseRefreshFailOnceStore{Store: backing}
			var publishedClocks []string
			cache := NewCachingStoreForTest(fault, func(_ string, _ string, payload json.RawMessage) {
				var emitted Bead
				if err := json.Unmarshal(payload, &emitted); err != nil {
					t.Fatal(err)
				}
				if clock := emitted.Metadata[RefineryDecisionAtKey]; clock != "" {
					publishedClocks = append(publishedClocks, clock)
				}
			})
			if err := cache.Prime(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := cache.Get(created.ID); err != nil {
				t.Fatal(err)
			}
			first, rejected := "2026-10-01T01:00:00Z", "2026-10-01T02:00:00Z"
			if err := backing.SetMetadata(created.ID, RefineryDecisionAtKey, first); err != nil {
				t.Fatal(err)
			}
			fault.failNextGet = true
			switch operation {
			case "update":
				err = cache.Update(created.ID, UpdateOpts{Metadata: map[string]string{RefineryDecisionAtKey: rejected}})
			case "single":
				err = cache.SetMetadata(created.ID, RefineryDecisionAtKey, rejected)
			case "batch":
				err = cache.SetMetadataBatch(created.ID, map[string]string{RefineryDecisionAtKey: rejected})
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, clock := range publishedClocks {
				if clock != first {
					t.Fatalf("published an unconfirmed first clock %q", clock)
				}
			}
			got, err := cache.Get(created.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Metadata[RefineryDecisionAtKey] != first {
				t.Fatalf("cache invented a receipt after failed refresh: %+v", got)
			}
		})
	}
}

type acceptedMetadataWriteErrorStore struct {
	Store
	writeErr error
	writes   int
}

func (s *acceptedMetadataWriteErrorStore) SetMetadata(id, key, value string) error {
	s.writes++
	if err := s.Store.SetMetadata(id, key, value); err != nil {
		return err
	}
	return s.writeErr
}

func TestCachedMetadataAcceptedWriteErrorRequiresBackingTruth(t *testing.T) {
	backing := newNativeDoltStoreForTest(newNativeDoltMemStorage())
	created, err := backing.Create(Bead{Title: "uncertain metadata receipt"})
	if err != nil {
		t.Fatal(err)
	}
	lostAck := errors.New("lost write acknowledgement")
	fault := &acceptedMetadataWriteErrorStore{Store: backing, writeErr: lostAck}
	cache := NewCachingStoreForTest(fault, nil)
	if err := cache.Prime(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := "2026-10-01T01:00:00Z"
	if err := cache.SetMetadata(created.ID, RefineryDecisionAtKey, first); !errors.Is(err, lostAck) {
		t.Fatalf("uncertain write outcome was hidden: %v", err)
	}
	got, err := cache.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata[RefineryDecisionAtKey] != first || fault.writes != 1 {
		t.Fatalf("uncertain receipt served stale cache or replayed write: bead=%+v writes=%d", got, fault.writes)
	}
}

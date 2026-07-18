package main

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
)

type budgetSlowNudgeStore struct {
	beads.Store
	latency   time.Duration
	failAfter int64
	ops       int64
}

func (s *budgetSlowNudgeStore) tick() error {
	op := atomic.AddInt64(&s.ops, 1)
	time.Sleep(s.latency)
	if s.failAfter > 0 && op > s.failAfter {
		return fmt.Errorf("slow nudge store operation %d exceeded test budget %d", op, s.failAfter)
	}
	return nil
}

func (s *budgetSlowNudgeStore) List(beads.ListQuery) ([]beads.Bead, error) {
	if err := s.tick(); err != nil {
		return nil, err
	}
	return []beads.Bead{{
		ID:       "shadow-open",
		Type:     nudgeBeadType,
		Status:   "open",
		Labels:   []string{nudgeBeadLabel},
		Metadata: map[string]string{"state": "queued"},
	}}, nil
}

func (s *budgetSlowNudgeStore) Create(b beads.Bead) (beads.Bead, error) {
	if err := s.tick(); err != nil {
		return beads.Bead{}, err
	}
	if b.ID == "" {
		b.ID = "created-shadow"
	}
	b.Status = "open"
	return b, nil
}

func (s *budgetSlowNudgeStore) Get(id string) (beads.Bead, error) {
	if err := s.tick(); err != nil {
		return beads.Bead{}, err
	}
	return beads.Bead{ID: id, Type: nudgeBeadType, Status: "open", Metadata: map[string]string{"state": "queued"}}, nil
}

func (s *budgetSlowNudgeStore) Close(string) error {
	return s.tick()
}

func (s *budgetSlowNudgeStore) SetMetadata(string, string, string) error {
	return s.tick()
}

func (s *budgetSlowNudgeStore) SetMetadataBatch(string, map[string]string) error {
	return s.tick()
}

func seedNudgeBudgetPreservationBacklog(t *testing.T, cityPath string, reference *nudgeReference, deadCount int) []string {
	t.Helper()
	now := time.Now().UTC()
	ids := make([]string, 0, deadCount+4)
	if err := withNudgeQueueState(cityPath, func(state *nudgeQueueState) error {
		for i := 0; i < 2; i++ {
			id := fmt.Sprintf("nudge-pending-preserve-%d", i)
			ids = append(ids, id)
			state.Pending = append(state.Pending, queuedNudge{
				ID:           id,
				BeadID:       "bead-" + id,
				Agent:        "gascity/deployer",
				Source:       "sling",
				Message:      "pending supersede tail",
				Reference:    reference,
				CreatedAt:    now.Add(-time.Hour),
				DeliverAfter: now.Add(time.Hour),
				ExpiresAt:    now.Add(time.Hour),
			})
		}
		for i := 0; i < 2; i++ {
			id := fmt.Sprintf("nudge-in-flight-preserve-%d", i)
			ids = append(ids, id)
			state.InFlight = append(state.InFlight, queuedNudge{
				ID:           id,
				BeadID:       "bead-" + id,
				Agent:        "gascity/deployer",
				Source:       "sling",
				Message:      "in-flight supersede tail",
				Reference:    reference,
				CreatedAt:    now.Add(-time.Hour),
				DeliverAfter: now.Add(-time.Minute),
				ExpiresAt:    now.Add(time.Hour),
				ClaimedAt:    now.Add(-time.Minute),
				LeaseUntil:   now.Add(time.Hour),
			})
		}
		for i := 0; i < deadCount; i++ {
			id := fmt.Sprintf("nudge-dead-preserve-%03d", i)
			ids = append(ids, id)
			state.Dead = append(state.Dead, queuedNudge{
				ID:        id,
				BeadID:    "bead-" + id,
				Agent:     "gascity/deployer",
				Source:    "sling",
				Message:   "dead backlog",
				CreatedAt: now.Add(-2 * time.Hour),
				DeadAt:    now.Add(-2 * time.Hour),
				LastError: "expired",
			})
		}
		return nil
	}); err != nil {
		t.Fatalf("seeding nudge backlog: %v", err)
	}
	return ids
}

func nudgeQueueBucketsByID(t *testing.T, cityPath string) map[string]string {
	t.Helper()
	buckets := make(map[string]string)
	if err := withNudgeQueueState(cityPath, func(state *nudgeQueueState) error {
		for _, item := range state.Pending {
			buckets[item.ID] = "pending"
		}
		for _, item := range state.InFlight {
			buckets[item.ID] = "in-flight"
		}
		for _, item := range state.Dead {
			buckets[item.ID] = "dead"
		}
		return nil
	}); err != nil {
		t.Fatalf("reading nudge queue state: %v", err)
	}
	return buckets
}

// TestSlingNudgeEnqueueBudgetPreservesQueuedItems exercises all three
// maintenance loops AND the supersede loop at once: a Dead backlog large
// enough to guarantee the budget cuts in, plus Pending/InFlight items that
// share the new item's supersession reference, so their "did the budget's
// early exit correctly leave supersede candidates untouched" behavior is
// asserted, not just inferred from pruneDeadQueuedNudges alone.
func TestSlingNudgeEnqueueBudgetPreservesQueuedItems(t *testing.T) {
	const deadBacklog = 160
	reference := &nudgeReference{Kind: "bead", ID: "ga-budget-preservation"}
	cityPath := t.TempDir()
	seededIDs := seedNudgeBudgetPreservationBacklog(t, cityPath, reference, deadBacklog)
	store := &budgetSlowNudgeStore{latency: 40 * time.Millisecond, failAfter: 90}
	item := newQueuedNudgeWithOptions("gascity/deployer", "Work slung. Check your hook.", "sling", time.Now(), queuedNudgeOptions{
		ID:        "nudge-new-preservation",
		Reference: reference,
	})

	start := time.Now()
	if err := enqueueQueuedNudgeWithStore(cityPath, beads.NudgesStore{Store: store}, item); err != nil {
		t.Fatalf("enqueueQueuedNudgeWithStore: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 5*time.Second {
		t.Fatalf("enqueue elapsed = %v, want budgeted foreground maintenance under 5s", elapsed.Round(time.Millisecond))
	}
	if ops := atomic.LoadInt64(&store.ops); ops >= deadBacklog {
		t.Fatalf("slow store ops = %d, want fewer than dead backlog %d to prove the maintenance budget cut in", ops, deadBacklog)
	}

	buckets := nudgeQueueBucketsByID(t, cityPath)
	if got, want := len(buckets), len(seededIDs)+1; got != want {
		t.Fatalf("queued item count = %d, want %d; buckets=%v", got, want, buckets)
	}
	for _, id := range seededIDs {
		if bucket := buckets[id]; bucket == "" {
			t.Fatalf("seeded queued nudge %q vanished after budgeted enqueue; buckets=%v", id, buckets)
		}
	}
	for i := 0; i < 2; i++ {
		id := fmt.Sprintf("nudge-pending-preserve-%d", i)
		if bucket := buckets[id]; bucket != "pending" {
			t.Fatalf("%s bucket = %q, want pending because supersede budget was already exhausted", id, bucket)
		}
		id = fmt.Sprintf("nudge-in-flight-preserve-%d", i)
		if bucket := buckets[id]; bucket != "in-flight" {
			t.Fatalf("%s bucket = %q, want in-flight because supersede budget was already exhausted", id, bucket)
		}
	}
	if bucket := buckets[item.ID]; bucket != "pending" {
		t.Fatalf("new queued nudge bucket = %q, want pending", bucket)
	}
}

// TestSlingNudgeEnqueueEmptyBacklogFast pins that an empty queue still
// enqueues near-instantly: none of the three maintenance loops iterate, so
// the deadline check never fires regardless of nudgeEnqueueMaintenanceBudget.
func TestSlingNudgeEnqueueEmptyBacklogFast(t *testing.T) {
	cityPath := t.TempDir()
	store := &budgetSlowNudgeStore{latency: 40 * time.Millisecond, failAfter: 4}
	item := newQueuedNudgeWithOptions("gascity/deployer", "Work slung. Check your hook.", "sling", time.Now(), queuedNudgeOptions{
		ID: "nudge-empty-backlog",
	})

	start := time.Now()
	if err := enqueueQueuedNudgeWithStore(cityPath, beads.NudgesStore{Store: store}, item); err != nil {
		t.Fatalf("enqueueQueuedNudgeWithStore: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("empty-backlog enqueue elapsed = %v, want under 500ms", elapsed.Round(time.Millisecond))
	}
	if ops := atomic.LoadInt64(&store.ops); ops > 2 {
		t.Fatalf("slow store ops = %d, want at most backing-bead setup ops for an empty backlog", ops)
	}

	buckets := nudgeQueueBucketsByID(t, cityPath)
	if got := buckets[item.ID]; got != "pending" {
		t.Fatalf("new queued nudge bucket = %q, want pending; buckets=%v", got, buckets)
	}
	if got, want := len(buckets), 1; got != want {
		t.Fatalf("queued item count = %d, want %d; buckets=%v", got, want, buckets)
	}
}

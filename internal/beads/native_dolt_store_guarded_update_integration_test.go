//go:build integration

package beads

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	beadslib "github.com/steveyegge/beads"
)

// Two independent native handles share an isolated real Dolt SQL server. This
// tests the backend row_lock collision/replay, not an in-process mutex fixture.
func TestNativeDoltGuardedUpdateCompetingSQLWriters(t *testing.T) {
	if _, err := exec.LookPath("dolt"); err != nil {
		t.Fatal("real guarded SQL proof requires dolt in PATH")
	}
	ctx := context.Background()
	port := startTestDoltServer(t)
	beadsDir := filepath.Join(t.TempDir(), ".beads")
	t.Setenv("BEADS_DIR", beadsDir)
	t.Setenv("BEADS_TEST_MODE", "1")
	t.Setenv("BEADS_TEST_SERVER", "1")
	t.Setenv("BEADS_DOLT_SERVER_HOST", "127.0.0.1")
	t.Setenv("BEADS_DOLT_SERVER_PORT", fmt.Sprint(port))
	t.Setenv("BEADS_DOLT_PORT", fmt.Sprint(port))
	t.Setenv("BEADS_DOLT_SERVER_SOCKET", "")
	t.Setenv("BEADS_DOLT_SERVER_DATABASE", "beads")
	t.Setenv("BEADS_DOLT_PASSWORD", "")
	if err := os.MkdirAll(beadsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"backend":"dolt","database":"beads","dolt_mode":"server","dolt_server_host":"127.0.0.1","dolt_server_port":%d}`, port)
	if err := os.WriteFile(filepath.Join(beadsDir, "metadata.json"), []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	open := func(actor string) *NativeDoltStore {
		storage, err := beadslib.OpenBestAvailable(ctx, beadsDir)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := storage.Close(); err != nil {
				t.Error(err)
			}
		})
		if err := storage.SetConfig(ctx, "issue_prefix", "gc"); err != nil {
			t.Fatal(err)
		}
		return newNativeDoltStoreWithStorageAndPrefix(storage, actor, "gc")
	}
	a, b := open("writer-a"), open("writer-b")
	created, err := a.Create(Bead{Title: "guarded SQL race", Status: "in_progress", Assignee: "refinery", Metadata: map[string]string{"polecat_session": "session-a", "push_verified_head": "head-a", "retained": "keep"}})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := b.Get(created.ID); err != nil || got.ID != created.ID {
		t.Fatalf("independent writer cannot see bead: %+v, %v", got, err)
	}
	ptr := func(v string) *string { return &v }
	type result struct {
		actor, clock string
		applied      bool
		err          error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for i, store := range []*NativeDoltStore{a, b} {
		go func() {
			actor := fmt.Sprintf("saitoc/refinery-%d", i)
			clock := fmt.Sprintf("2026-10-01T0%d:00:00Z", i+1)
			<-start
			applied, err := store.UpdateGuarded(created.ID, UpdateOpts{Status: ptr("closed"), Metadata: map[string]string{"outcome": actor, "close_reason": "verified landing"}}, UpdateConditions{Actor: actor, Status: ptr("in_progress"), Assignee: ptr("refinery"), Metadata: map[string]string{"polecat_session": "session-a", "push_verified_head": "head-a"}, SetMetadataIfAbsent: map[string]string{RefineryDecisionAtKey: clock}})
			results <- result{actor, clock, applied, err}
		}()
	}
	close(start)
	var winner result
	wins := 0
	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatalf("competing native SQL update: %v", r.err)
		}
		if r.applied {
			winner = r
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("competing guarded writes produced %d winners, want exactly one", wins)
	}
	got, err := a.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "closed" || got.Assignee != "refinery" || got.Metadata["outcome"] != winner.actor || got.Metadata[RefineryDecisionAtKey] != winner.clock || got.Metadata["retained"] != "keep" {
		t.Fatalf("winner's atomic receipt not retained: %+v, winner %+v", got, winner)
	}
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/beads?parseTime=true", port))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var closedAt sql.NullTime
	if err := db.QueryRowContext(ctx, "SELECT closed_at FROM issues WHERE id = ?", created.ID).Scan(&closedAt); err != nil {
		t.Fatal(err)
	}
	if !closedAt.Valid {
		t.Fatal("guarded terminal transition did not set closed_at")
	}
	var closedEvents int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE issue_id = ? AND event_type = 'closed' AND actor = ?", created.ID, winner.actor).Scan(&closedEvents); err != nil {
		t.Fatal(err)
	}
	if closedEvents != 1 {
		t.Fatalf("durable closed events for winning actor = %d, want one", closedEvents)
	}
	// A new incarnation can share the refinery assignee. The session guard,
	// not owner equality or an unusable revision zero, must refuse the old one.
	applied, err := a.UpdateGuarded(created.ID, UpdateOpts{Status: ptr("in_progress"), Metadata: map[string]string{"polecat_session": "session-b", "push_verified_head": "head-b", "outcome": "pending"}}, UpdateConditions{Actor: "next-incarnation", Status: ptr("closed"), Assignee: ptr("refinery"), Metadata: map[string]string{"polecat_session": "session-a"}})
	if err != nil || !applied {
		t.Fatalf("guarded reopening = (%v, %v)", applied, err)
	}
	var reopenedEvents int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE issue_id = ? AND event_type = 'reopened' AND actor = 'next-incarnation'", created.ID).Scan(&reopenedEvents); err != nil {
		t.Fatal(err)
	}
	if reopenedEvents != 1 {
		t.Fatalf("canonical reopened events = %d, want one", reopenedEvents)
	}
	before, err := b.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	applied, err = b.UpdateGuarded(created.ID, UpdateOpts{Status: ptr("closed"), Assignee: ptr("old-owner"), Metadata: map[string]string{"outcome": "stale"}}, UpdateConditions{Status: ptr("in_progress"), Assignee: ptr("refinery"), Metadata: map[string]string{"polecat_session": "session-a"}, SetMetadataIfAbsent: map[string]string{RefineryDecisionAtKey: "later"}})
	if err != nil || applied {
		t.Fatalf("stale incarnation = (%v, %v)", applied, err)
	}
	after, err := a.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("stale incarnation mutated later owner/outcome: before %+v after %+v", before, after)
	}
	if err := b.SetMetadataBatch(created.ID, map[string]string{RefineryDecisionAtKey: "replace", "ordinary": "preserved"}); err != nil {
		t.Fatal(err)
	}
	final, err := a.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Metadata[RefineryDecisionAtKey] != winner.clock || final.Metadata["retained"] != "keep" || final.Metadata["ordinary"] != "preserved" {
		t.Fatalf("ordinary metadata update lost first clock/siblings: %+v", final)
	}
	var metadataEvents int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE issue_id = ? AND event_type = 'updated' AND actor = 'writer-b' AND CASE WHEN JSON_VALID(new_value) THEN JSON_UNQUOTE(JSON_EXTRACT(new_value, '$.metadata.ordinary')) END = 'preserved'", created.ID).Scan(&metadataEvents); err != nil {
		t.Fatal(err)
	}
	if metadataEvents != 1 {
		t.Fatalf("canonical metadata events for ordinary update = %d, want one", metadataEvents)
	}
	for name, metadata := range map[string]map[string]string{
		"last key":      {"temporary": "remove"},
		"already empty": {},
	} {
		t.Run(name, func(t *testing.T) {
			empty, err := a.Create(Bead{Title: name, Assignee: "worker", Metadata: metadata})
			if err != nil {
				t.Fatal(err)
			}
			applied, err := a.UpdateGuarded(empty.ID, UpdateOpts{Assignee: ptr("refinery")}, UpdateConditions{Assignee: ptr("worker"), UnsetMetadata: []string{"temporary"}})
			if err != nil || !applied {
				t.Fatalf("guarded unset and owner change = (%v, %v)", applied, err)
			}
			got, err := b.Get(empty.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Assignee != "refinery" || len(got.Metadata) != 0 {
				t.Fatalf("empty object and owner did not commit together: %+v", got)
			}
			var valid, keys int
			if err := db.QueryRowContext(ctx, "SELECT JSON_VALID(metadata), JSON_LENGTH(metadata) FROM issues WHERE id = ?", empty.ID).Scan(&valid, &keys); err != nil {
				t.Fatal(err)
			}
			if valid != 1 || keys != 0 {
				t.Fatalf("guarded unset stored invalid/nonempty JSON: valid=%d keys=%d", valid, keys)
			}
		})
	}
}

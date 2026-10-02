package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/beads"
)

func TestBdMutationGuardValuesAreNotIDs(t *testing.T) {
	args := []string{"update", "saitoc-13w90g", "--status", "in_progress", "--if-status", "open", "--if-assignee", "omp", "--append-notes", "saitoc-not-an-id", "--json"}
	ids, ok, ambiguous := bdMutationWriteIDs(args)
	if !ok || ambiguous || !reflect.DeepEqual(ids, []string{"saitoc-13w90g"}) {
		t.Fatalf("actual guarded argv IDs = %v, ok=%v ambiguous=%v", ids, ok, ambiguous)
	}
	args = []string{"update", "--if-metadata", "polecat_session=saitoc-not-an-id", "--set-metadata-if-absent", "refinery_decision_at=2026-10-01T01:00:00Z", "saitoc-13w90g", "--status=closed"}
	ids, ok, ambiguous = bdMutationWriteIDs(args)
	if !ok || ambiguous || !reflect.DeepEqual(ids, []string{"saitoc-13w90g"}) {
		t.Fatalf("native guard values became IDs: %v, ok=%v ambiguous=%v", ids, ok, ambiguous)
	}
	args = []string{"update", "--if-title", "saitoc-not-an-id", "--if-description", "saitoc-also-not-an-id", "--if-acceptance", "saitoc-criteria-not-an-id", "--if-labels-json", `["saitoc-label-not-an-id"]`, "saitoc-13w90g"}
	ids, ok, ambiguous = bdMutationWriteIDs(args)
	if !ok || ambiguous || !reflect.DeepEqual(ids, []string{"saitoc-13w90g"}) {
		t.Fatalf("source/label guard values became IDs: %v, ok=%v ambiguous=%v", ids, ok, ambiguous)
	}
}

func TestNativeBdGuardedUpdateParsingAndRefusals(t *testing.T) {
	args := []string{"--json", "--actor", "saitoc/refinery", "update", "saitoc-full-id", "--status=closed", "--if-status=in_progress", "--if-assignee=refinery", "--if-metadata=polecat_session=session-a", "--if-metadata=push_verified_head=head-a", "--set-metadata-if-absent=refinery_decision_at=2026-10-01T01:00:00Z", "--metadata", `{"outcome":"merged","new_key":"value"}`}
	if !nativeBdUpdateRequested(args) {
		t.Fatal("native guarded update was not selected")
	}
	op, rejected, ok := parseNativeBdUpdate(args)
	if !ok {
		t.Fatalf("native parser refused supported mutation: %q", rejected)
	}
	if op.ID != "saitoc-full-id" || !op.JSON || op.Conditions.Actor != "saitoc/refinery" || *op.Conditions.Status != "in_progress" || *op.Conditions.Assignee != "refinery" || op.Conditions.Metadata["polecat_session"] != "session-a" || op.Conditions.Metadata["push_verified_head"] != "head-a" || op.Conditions.SetMetadataIfAbsent[beads.RefineryDecisionAtKey] != "2026-10-01T01:00:00Z" || op.Update.Metadata["outcome"] != "merged" {
		t.Fatalf("parser lost a mutation/guard: %+v", op)
	}
	for _, extra := range [][]string{
		{"--append-notes", "must not be ignored"},
		{"--if-fence", "1"},
		{"--unimplemented=value"},
		{"--set-metadata", "other=disallowed-with-metadata-document"},
		{"--if-metadata=polecat_session=contradiction"},
		{"--set-metadata-if-absent=refinery_decision_at=2026-10-01T02:00:00Z"},
	} {
		if _, _, ok := parseNativeBdUpdate(append(append([]string(nil), args...), extra...)); ok {
			t.Fatalf("unsupported/contradictory fields silently accepted: %v", extra)
		}
	}
	if nativeBdUpdateRequested([]string{"update", "saitoc-full-id", "--append-notes", "--if-metadata=quoted-not-a-flag", "--status=open"}) {
		t.Fatal("a flag quoted as a notes value selected native mutation")
	}
	if nativeBdUpdateRequested([]string{"update", "saitoc-full-id", "--metadata", `{"typed":42}`}) {
		t.Fatal("ordinary typed JSON was diverted into the narrower native contract")
	}
}

func TestNativeBdGuardedUpdateCombinesMetadataRemovalAndLabels(t *testing.T) {
	args := []string{"update", "saitoc-full-id", "--json", "--actor=saitoc/scheduler",
		"--if-status=open", "--if-assignee=worker", "--if-metadata=gc.routed_to=",
		"--set-metadata=gc.routed_to=refinery", "--unset-metadata=refinery_handoff_at",
		"--remove-label=value-contract-missing"}
	args = append(args, "--if-labels-json=[]", "--if-title=original title", "--if-description=original description", "--if-acceptance=original acceptance")
	op, rejected, ok := parseNativeBdUpdate(args)
	if !ok {
		t.Fatalf("guarded cleanup refused supported flags: %q", rejected)
	}
	if op.Conditions.Labels == nil || len(*op.Conditions.Labels) != 0 || op.Conditions.Title == nil || *op.Conditions.Title != "original title" || op.Conditions.Description == nil || *op.Conditions.Description != "original description" || op.Conditions.AcceptanceCriteria == nil || *op.Conditions.AcceptanceCriteria != "original acceptance" {
		t.Fatalf("source/label guards changed: %+v", op.Conditions)
	}
	if !nativeBdUpdateRequested(args) || !op.JSON || op.Conditions.Actor != "saitoc/scheduler" ||
		op.Conditions.Metadata["gc.routed_to"] != "" || op.Update.Metadata["gc.routed_to"] != "refinery" ||
		!reflect.DeepEqual(op.Conditions.UnsetMetadata, []string{"refinery_handoff_at"}) ||
		!reflect.DeepEqual(op.Update.RemoveLabels, []string{"value-contract-missing"}) {
		t.Fatalf("guarded cleanup changed the requested mutation: %+v", op)
	}
}

func TestNativeBdLabelGuardRefusesInvalidSnapshots(t *testing.T) {
	for _, raw := range []string{"null", `["duplicate","duplicate"]`, `["ok",42]`, `{"label":"hold"}`} {
		args := []string{"update", "saitoc-full-id", "--if-labels-json=" + raw, "--status=closed"}
		if !nativeBdUpdateRequested(args) {
			t.Fatalf("invalid requested label guard bypassed native refusal: %s", raw)
		}
		if _, _, ok := parseNativeBdUpdate(args); ok {
			t.Fatalf("invalid label snapshot accepted: %s", raw)
		}
	}
}

func TestBdGuardedUpdateRefusesUnsupportedBackendWithoutMutation(t *testing.T) {
	store := beads.NewMemStore()
	created, err := store.Create(beads.Bead{Title: "untouched", Metadata: map[string]string{"polecat_session": "session-a"}})
	if err != nil {
		t.Fatal(err)
	}
	status := "closed"
	var stdout, stderr bytes.Buffer
	code := doBdGuardedUpdate(struct{ beads.Store }{store}, bdByIDOp{ID: created.ID, JSON: true, Update: beads.UpdateOpts{Status: &status}, Conditions: beads.UpdateConditions{Metadata: map[string]string{"polecat_session": "session-a"}}}, "", &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "refusing unchecked mutation") {
		t.Fatalf("unsupported backend = %d, stderr %q", code, stderr.String())
	}
	got, err := store.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, created) {
		t.Fatalf("unsupported capability fell back to mutation: before %+v after %+v", created, got)
	}
}

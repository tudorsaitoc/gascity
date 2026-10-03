package main

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/gastownhall/gascity/internal/config"
)

// TestReadSlingStdinBead exercises the extracted --stdin parser directly via the
// slingStdin seam: first line is the title, the rest (trimmed) is the
// description, and empty input is an error. Demonstrates that hoisting the parse
// out of cmdSlingWithJSON made it independently testable.
func TestReadSlingStdinBead(t *testing.T) {
	prev := slingStdin
	t.Cleanup(func() { slingStdin = prev })
	for _, tc := range []struct {
		name, input, wantTitle, wantDesc, wantErr string
	}{
		{"title-only", "just a title", "just a title", "", ""},
		{"title-and-desc", "the title\nthe description\nmore", "the title", "the description\nmore", ""},
		{"trailing-newline-trimmed", "title\n", "title", "", ""},
		{"empty", "", "", "", "invalid_arguments"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slingStdin = func() io.Reader { return strings.NewReader(tc.input) }
			title, desc, code, _ := readSlingStdinBead()
			if code != tc.wantErr {
				t.Fatalf("errCode = %q, want %q", code, tc.wantErr)
			}
			if code == "" && (title != tc.wantTitle || desc != tc.wantDesc) {
				t.Fatalf("got (title=%q desc=%q), want (%q, %q)", title, desc, tc.wantTitle, tc.wantDesc)
			}
		})
	}
}

// TestInferSling1ArgTarget_FormulaRejected exercises the extracted 1-arg
// target-inference helper directly (its pure --formula guard needs no store),
// demonstrating that hoisting the store-touching pre-core orchestration out of
// cmdSlingWithJSON makes it independently testable.
func TestInferSling1ArgTarget_FormulaRejected(t *testing.T) {
	target, _, errCode, errMsg := inferSling1ArgTarget(&config.City{}, "/tmp/nonexistent", "some-bead", true)
	if target != "" || errCode != "invalid_arguments" || errMsg == "" {
		t.Fatalf("isFormula 1-arg: got (target=%q code=%q msg=%q), want (\"\", invalid_arguments, non-empty)", target, errCode, errMsg)
	}
}

// TestCmdSlingMultiDefaultTargets_DeterministicPick uses the seam to prove the
// exact target a 1-arg `gc sling <bead>` routes to from a multi-entry
// default_sling_targets list — a stronger assertion than the existing
// "accept either" test, now that the random pick is injectable.
func TestCmdSlingMultiDefaultTargets_DeterministicPick(t *testing.T) {
	for _, tc := range []struct {
		name string
		idx  int
		want string
	}{
		{"first", 0, "foundations/worker-a"},
		{"second", 1, "foundations/worker-b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cityDir, rigDir := setupCmdSlingMultiDefaultTargetsFixture(t,
				[]string{"foundations/worker-a", "foundations/worker-b"})
			restore := SetSlingTargetIndexForTest(func(int) int { return tc.idx })
			defer restore()

			var stdout, stderr bytes.Buffer
			code := cmdSlingWithJSON([]string{"fo-multi-work"}, false, false, false, "", nil, "", true, false, false, "", false, false, false, "", "", false, nil, &stdout, &stderr)
			if code != 0 {
				t.Fatalf("cmdSling = %d, want 0; stderr=%s", code, stderr.String())
			}
			rigStore, err := slingOpenStoreAtForCity(rigDir, cityDir)
			if err != nil {
				t.Fatalf("openStoreAtForCity: %v", err)
			}
			routed, err := rigStore.Get("fo-multi-work")
			if err != nil {
				t.Fatalf("Get(fo-multi-work): %v", err)
			}
			if got := routed.Metadata["gc.routed_to"]; got != tc.want {
				t.Fatalf("idx=%d: gc.routed_to = %q, want %q", tc.idx, got, tc.want)
			}
		})
	}
}

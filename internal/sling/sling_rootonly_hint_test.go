package sling

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/gastownhall/gascity/internal/formula"
)

func compileHintFixture(t *testing.T, name, content string) *formula.Recipe {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, name+".toml"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	recipe, err := formula.Compile(context.Background(), name, []string{dir}, nil)
	if err != nil {
		t.Fatalf("Compile(%s): %v", name, err)
	}
	return recipe
}

// TestRootOnlyVaporPourHint pins the cause (a) vs cause (b) distinction: only a
// vapor formula without pour = true (a) gets the hint; a genuinely step-less
// formula (b), a poured vapor formula, and a normal multi-step formula do not.
func TestRootOnlyVaporPourHint(t *testing.T) {
	tests := []struct {
		name     string
		formula  string
		content  string
		wantHint bool
	}{
		{
			name:     "vapor without pour (cause a)",
			formula:  "patrol",
			content:  "formula = \"patrol\"\nversion = 1\nphase = \"vapor\"\n\n[[steps]]\nid = \"scan\"\ntitle = \"Scan\"\n",
			wantHint: true,
		},
		{
			name:     "step-less formula (cause b)",
			formula:  "router",
			content:  "formula = \"router\"\nversion = 1\n",
			wantHint: false,
		},
		{
			name:     "vapor with pour",
			formula:  "eager",
			content:  "formula = \"eager\"\nversion = 1\nphase = \"vapor\"\npour = true\n\n[[steps]]\nid = \"scan\"\ntitle = \"Scan\"\n",
			wantHint: false,
		},
		{
			name:     "normal multi-step liquid",
			formula:  "build",
			content:  "formula = \"build\"\nversion = 1\n\n[[steps]]\nid = \"a\"\ntitle = \"A\"\n\n[[steps]]\nid = \"b\"\ntitle = \"B\"\n",
			wantHint: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recipe := compileHintFixture(t, tc.formula, tc.content)
			got := rootOnlyVaporPourHint(tc.formula, recipe)
			if tc.wantHint {
				if got == "" {
					t.Fatalf("want hint, got empty (RootOnly=%v Phase=%q Pour=%v)", recipe.RootOnly, recipe.Phase, recipe.Pour)
				}
				return
			}
			if got != "" {
				t.Errorf("want no hint, got %q (RootOnly=%v Phase=%q Pour=%v)", got, recipe.RootOnly, recipe.Phase, recipe.Pour)
			}
		})
	}
}

package api

// Per-domain Huma input/output types for the sling handler
// group. Split out of the original huma_types.go; mirrors the layout
// of huma_handlers_sling.go.

// --- Sling types ---

// SlingInput is the Huma input for POST /v0/city/{cityName}/sling.
//
// `target` is a hard requirement (handler returns 400 when empty). The
// spec marks it required + minLength 1 so generated clients validate at
// the edge rather than only at runtime.
type SlingInput struct {
	CityScope
	Body struct {
		Rig            string            `json:"rig,omitempty" doc:"Rig name."`
		Target         string            `json:"target" minLength:"1" doc:"Target agent or pool."`
		Bead           string            `json:"bead,omitempty" doc:"Bead ID to sling."`
		Formula        string            `json:"formula,omitempty" doc:"Formula name for workflow launch."`
		AttachedBeadID string            `json:"attached_bead_id,omitempty" doc:"Bead ID to attach a formula to."`
		Title          string            `json:"title,omitempty" doc:"Workflow title."`
		Vars           map[string]string `json:"vars,omitempty" doc:"Formula variables."`
		ScopeKind      string            `json:"scope_kind,omitempty" doc:"Scope kind (city or rig)."`
		ScopeRef       string            `json:"scope_ref,omitempty" doc:"Scope reference."`
		Force          bool              `json:"force,omitempty" doc:"Allow cross-rig routing and graph workflow replacement without bypassing current holds, unreadable receipt beads, or ownership conditions."`
		Reassign       bool              `json:"reassign,omitempty" doc:"Clear the current assignee in the guarded route transaction, handing the bead to the target's claim path."`
		Merge          string            `json:"merge,omitempty" doc:"Merge strategy: direct, mr, or local."`
		NoConvoy       bool              `json:"no_convoy,omitempty" doc:"Do not create an auto-convoy for the routed bead."`
		Owned          bool              `json:"owned,omitempty" doc:"Mark the routed bead as owned by the target."`
		NoFormula      bool              `json:"no_formula,omitempty" doc:"Suppress the target's default_sling_formula even when configured."`
		IfStatus       *string           `json:"if_status,omitempty" doc:"Exact canonical status required at the native route commit."`
		IfAssignee     *string           `json:"if_assignee,omitempty" doc:"Exact canonical assignee required at route commit; an empty string requires unowned work."`
		IfMetadata     map[string]string `json:"if_metadata,omitempty" doc:"Canonical metadata equality predicates consumed by the same route transaction; an empty value matches absent."`
		IfLabels       *[]string         `json:"if_labels,omitempty" doc:"Exact unordered canonical label snapshot required by the same route transaction; an empty array requires no labels."`
		IfTitle        *string           `json:"if_title,omitempty" doc:"Exact original canonical title required at route commit."`
		IfDescription  *string           `json:"if_description,omitempty" doc:"Exact original canonical description required at route commit."`
		IfAcceptance   *string           `json:"if_acceptance,omitempty" doc:"Exact original native acceptance criteria required at route commit."`
	}
}

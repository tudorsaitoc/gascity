package sling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/agentutil"
	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/fsys"
	"github.com/gastownhall/gascity/internal/suspensionstate"
)

const (
	DispatchEffectIDKey    = "gc.dispatch_effect_id"
	DispatchEffectStateKey = "gc.dispatch_effect_state"
)

// RouteConflictError refuses the current mutation. A previously selected
// provider remains authoritative and must be reconciled, never blindly replaced.
type RouteConflictError struct{ BeadID, Reason string }

func (e *RouteConflictError) Error() string {
	return fmt.Sprintf("sling %s refused: %s", e.BeadID, e.Reason)
}

func routeConditions(b beads.Bead, requested *beads.UpdateConditions) (beads.UpdateConditions, error) {
	conditions := beads.UpdateConditions{Status: &b.Status, Assignee: &b.Assignee, Title: &b.Title, Description: &b.Description, AcceptanceCriteria: &b.AcceptanceCriteria, Labels: &b.Labels, Metadata: map[string]string{}}
	for _, key := range []string{beadmeta.RoutedToMetadataKey, beadmeta.ExecutionRoutedToMetadataKey, beadmeta.MoleculeIDMetadataKey, "workflow_id", "handoff.conflict_state", DispatchEffectIDKey, DispatchEffectStateKey, customDispatchProviderKey} {
		conditions.Metadata[key] = b.Metadata[key]
	}
	if requested != nil {
		if requested.Status != nil && *requested.Status != b.Status || requested.Assignee != nil && *requested.Assignee != b.Assignee {
			return conditions, &RouteConflictError{b.ID, "expected status or assignee is stale"}
		}
		if requested.Title != nil && *requested.Title != b.Title || requested.Description != nil && *requested.Description != b.Description || requested.AcceptanceCriteria != nil && *requested.AcceptanceCriteria != b.AcceptanceCriteria {
			return conditions, &RouteConflictError{b.ID, "expected human goal or acceptance is stale"}
		}
		for key, expected := range requested.Metadata {
			if b.Metadata[key] != expected {
				return conditions, &RouteConflictError{b.ID, "expected metadata is stale: " + key}
			}
			conditions.Metadata[key] = expected
		}
		conditions.Actor = requested.Actor
		if requested.Labels != nil {
			conditions.Labels = requested.Labels
		}
	}
	return conditions, nil
}

func admitRouteBead(b beads.Bead, target string, reassign bool) error {
	if b.Metadata["handoff.conflict_state"] == "hold" {
		return &RouteConflictError{b.ID, "canonical handoff hold is active"}
	}
	if b.Status != "open" && b.Status != "deferred" && !(b.Status == "in_progress" && reassign) {
		return &RouteConflictError{b.ID, "current status does not admit new routing"}
	}
	if b.Assignee != "" && b.Assignee != target && !reassign {
		return &RouteConflictError{b.ID, "work is already owned by " + b.Assignee}
	}
	if routed := b.Metadata[beadmeta.RoutedToMetadataKey]; routed != "" && routed != target && !reassign {
		return &RouteConflictError{b.ID, "work already has a different canonical route"}
	}
	if b.Metadata[DispatchEffectIDKey] != "" && b.Metadata[DispatchEffectStateKey] != "attempted" {
		return &RouteConflictError{b.ID, "dispatch effect requires canonical reconciliation, not a new route"}
	}
	return nil
}

// CheckExecutionAdmission invokes the operator-configured project admission
// owner on this execution host. Request payloads cannot select or override it.
// A durable effect requires the seam; ordinary cities may omit project policy.
func CheckExecutionAdmission(ctx context.Context, cityPath, target string, required bool) error {
	command := strings.TrimSpace(os.Getenv("GC_SLING_ADMISSION_COMMAND"))
	if command == "" {
		if required {
			return fmt.Errorf("sling execution-host admission is required: GC_SLING_ADMISSION_COMMAND is not configured")
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Dir = cityPath
	cmd.Env = append(os.Environ(), "GC_CITY_PATH="+cityPath, "GC_SLING_TARGET="+target)
	out, err := cmd.Output()
	var response struct {
		Allowed *bool  `json:"allowed"`
		Reason  string `json:"reason"`
	}
	if decodeErr := json.Unmarshal(out, &response); decodeErr != nil || response.Allowed == nil {
		return fmt.Errorf("sling execution-host admission is unreadable (command error: %v)", err)
	}
	if err != nil || !*response.Allowed {
		return fmt.Errorf("sling execution-host admission refused: %s", response.Reason)
	}
	return nil
}

func checkNativeAdmission(ctx context.Context, cfg *config.City, cityPath, target string, required bool) error {
	if cfg == nil {
		return fmt.Errorf("native sling admission requires city config")
	}
	state, err := suspensionstate.Load(fsys.OSFS{}, cityPath)
	if err != nil {
		return fmt.Errorf("native suspension state is unreadable: %w", err)
	}
	if suspensionstate.EffectiveCitySuspended(state, cfg.Workspace.EffectiveSuspendedOnStart()) {
		return fmt.Errorf("native city is suspended")
	}
	for _, a := range cfg.Agents {
		if agentutil.NormalizePoolRouteTarget(cfg, a.QualifiedName()) != target {
			continue
		}
		if a.Suspended {
			return fmt.Errorf("native target %s is suspended", target)
		}
		for _, rig := range cfg.Rigs {
			if rig.Name == a.Dir && suspensionstate.EffectiveRigSuspended(state, rig.Name, rig.EffectiveSuspendedOnStart()) {
				return fmt.Errorf("native target rig %s is suspended", rig.Name)
			}
		}
	}
	return CheckExecutionAdmission(ctx, cityPath, target, required)
}

// CommitRoute is the built-in CLI/API route writer. Every predicate and the
// actual delivery/effect receipt are consumed by one backend transaction.
// Unsupported backends are not emulated with a read followed by SetMetadata.
func CommitRoute(ctx context.Context, store beads.Store, cfg *config.City, cityPath string, req RouteRequest) error {
	if store == nil {
		return fmt.Errorf("built-in sling routing requires a store")
	}
	_, ok := beads.GuardedUpdateWriterFor(store)
	if !ok {
		return fmt.Errorf("guarded native sling routing: %w", beads.ErrConditionalWriteUnsupported)
	}
	b, err := beads.HandlesFor(store).Live.Get(req.BeadID)
	if err != nil {
		return err
	}
	target := agentutil.NormalizePoolRouteTarget(cfg, req.Target)
	// A repeat is a read-only reconciliation. Never re-run a custom provider or
	// wake a worker merely because the prior acknowledgment was lost.
	effectID := ""
	if req.Conditions != nil {
		effectID = req.Conditions.Metadata[DispatchEffectIDKey]
	}
	if b.Metadata[DispatchEffectIDKey] != "" && effectID != b.Metadata[DispatchEffectIDKey] {
		return &RouteConflictError{b.ID, "original dispatch effect must be reconciled with its exact identity"}
	}
	if effectID != "" && b.Metadata[DispatchEffectIDKey] == effectID && b.Metadata[beadmeta.RoutedToMetadataKey] == target && b.Metadata[DispatchEffectStateKey] != "attempted" && b.Metadata[DispatchEffectStateKey] != "committed" {
		return nil
	}
	conditions, err := routeConditions(b, req.Conditions)
	if err != nil {
		return err
	}
	if effectID != "" && (req.Conditions.Status == nil || req.Conditions.Assignee == nil || req.Conditions.Metadata[DispatchEffectStateKey] != "attempted") {
		return fmt.Errorf("dispatch effect routing requires exact if-status, if-assignee, and attempted effect metadata")
	}
	if err := admitRouteBead(b, target, req.Reassign); err != nil {
		return err
	}
	metadata := maps.Clone(req.Metadata)
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadata[beadmeta.RoutedToMetadataKey] = target
	if effectID != "" {
		metadata[DispatchEffectIDKey], metadata[DispatchEffectStateKey] = effectID, "routed"
	}
	update := beads.UpdateOpts{Metadata: metadata}
	if req.Reassign {
		empty, open := "", "open"
		update.Assignee = &empty
		if b.Status == "in_progress" {
			update.Status = &open
		}
	}
	atomic, ok := beads.SingleTransactionStoreFor(store)
	if !ok {
		return beads.ErrConditionalWriteUnsupported
	}
	return atomic.TxSingle("gc: commit guarded sling route", func(tx beads.Tx) error {
		writer, ok := tx.(beads.GuardedUpdateWriter)
		if !ok {
			return beads.ErrConditionalWriteUnsupported
		}
		if err := checkNativeAdmission(ctx, cfg, cityPath, target, effectID != ""); err != nil {
			return err
		}
		applied, err := writer.UpdateGuarded(req.BeadID, update, conditions)
		if err != nil {
			return err
		}
		if !applied {
			return &RouteConflictError{req.BeadID, "status, ownership, route, or hold changed before route commit"}
		}
		return nil
	})
}

func prepareRouteConditions(opts SlingOpts, deps SlingDeps) (SlingOpts, *beads.Bead, error) {
	if opts.IsFormula {
		if opts.Conditions != nil {
			return opts, nil, fmt.Errorf("guarded formula dispatch requires an existing receipt bead and --on")
		}
		return opts, nil, nil
	}
	if opts.DryRun && opts.InlineText {
		return opts, nil, nil
	}
	b, err := beads.HandlesFor(deps.Store).Live.Get(opts.BeadOrFormula)
	if err != nil {
		storeRef := strings.TrimSpace(deps.StoreRef)
		if storeRef == "" {
			storeRef = "local"
		}
		if errors.Is(err, beads.ErrNotFound) {
			return opts, nil, &MissingBeadError{BeadID: opts.BeadOrFormula, StoreRef: storeRef}
		}
		return opts, nil, &BeadLookupError{BeadID: opts.BeadOrFormula, StoreRef: storeRef, Err: err}
	}
	custom := IsCustomSlingQuery(opts.Target)
	if custom && opts.Conditions == nil && b.Metadata[DispatchEffectIDKey] != "" && b.Metadata[customDispatchProviderKey] != "" {
		original, err := routeConditions(b, nil)
		if err != nil {
			return opts, nil, err
		}
		opts.Conditions = &original
	}
	if custom && b.Metadata[DispatchEffectIDKey] != "" && b.Metadata[customDispatchProviderKey] == "" &&
		b.Metadata[DispatchEffectStateKey] != "attempted" {
		return opts, nil, &RouteConflictError{b.ID, "custom effect lacks its original provider receipt; authoritative reconciliation is required"}
	}
	if opts.Conditions != nil && opts.Conditions.Metadata[DispatchEffectIDKey] != "" && (opts.Conditions.Status == nil || opts.Conditions.Assignee == nil) {
		return opts, nil, fmt.Errorf("dispatch effect requires explicit exact if-status and if-assignee")
	}
	if opts.Conditions != nil && opts.Conditions.Metadata[DispatchEffectIDKey] != "" {
		state := opts.Conditions.Metadata[DispatchEffectStateKey]
		if state != "attempted" && state != "committed" && !(custom && (state == "unknown" || state == "routed")) {
			return opts, nil, fmt.Errorf("dispatch effect requires an explicit canonical if-metadata state")
		}
	}
	if b.Metadata[DispatchEffectIDKey] != "" && (opts.Conditions == nil || opts.Conditions.Metadata[DispatchEffectIDKey] != b.Metadata[DispatchEffectIDKey]) {
		return opts, nil, &RouteConflictError{b.ID, "original dispatch effect must be reconciled with its exact identity"}
	}
	target := agentutil.NormalizePoolRouteTarget(deps.Cfg, agentutil.RoutedToIdentity(&opts.Target))
	if opts.Conditions != nil {
		id := opts.Conditions.Metadata[DispatchEffectIDKey]
		if id != "" && b.Metadata[DispatchEffectIDKey] == id && b.Metadata[DispatchEffectStateKey] != "attempted" && (b.Metadata[beadmeta.RoutedToMetadataKey] == target || b.Metadata[beadmeta.ExecutionRoutedToMetadataKey] == target) {
			return opts, &b, nil
		}
	}
	if !opts.Force && opts.Conditions == nil && b.Metadata[DispatchEffectStateKey] == "routed" && (b.Metadata[beadmeta.RoutedToMetadataKey] == target || b.Metadata[beadmeta.ExecutionRoutedToMetadataKey] == target) {
		return opts, &b, nil
	}
	conditions, err := routeConditions(b, opts.Conditions)
	if err != nil {
		return opts, nil, err
	}
	if b.Metadata["handoff.conflict_state"] == "hold" {
		return opts, nil, &RouteConflictError{b.ID, "canonical handoff hold is active"}
	}
	alreadyRouted := b.Metadata[beadmeta.RoutedToMetadataKey] == target && (b.Assignee == "" || b.Assignee == target)
	if !alreadyRouted || custom || opts.Force || opts.Reassign || usesFormulaBackedRoute(opts) {
		if err := admitRouteBead(b, target, opts.Reassign); err != nil {
			return opts, nil, err
		}
	}
	opts.Conditions = &conditions
	return opts, &b, nil
}

func isReconciledEffect(opts SlingOpts, b *beads.Bead, deps SlingDeps) bool {
	if b == nil || opts.Conditions == nil {
		return false
	}
	id := opts.Conditions.Metadata[DispatchEffectIDKey]
	target := agentutil.NormalizePoolRouteTarget(deps.Cfg, agentutil.RoutedToIdentity(&opts.Target))
	state := b.Metadata[DispatchEffectStateKey]
	return id != "" && b.Metadata[DispatchEffectIDKey] == id && state != "attempted" && state != "committed" && (b.Metadata[beadmeta.RoutedToMetadataKey] == target || b.Metadata[beadmeta.ExecutionRoutedToMetadataKey] == target)
}

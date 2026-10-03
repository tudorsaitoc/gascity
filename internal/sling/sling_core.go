package sling

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/gastownhall/gascity/internal/agentutil"
	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	convoycore "github.com/gastownhall/gascity/internal/convoy"
	"github.com/gastownhall/gascity/internal/formula"
	"github.com/gastownhall/gascity/internal/graphroute"
	"github.com/gastownhall/gascity/internal/graphv2"
	"github.com/gastownhall/gascity/internal/molecule"
	"github.com/gastownhall/gascity/internal/sourceworkflow"
	"github.com/gastownhall/gascity/internal/telemetry"
)

func depsTracef(deps SlingDeps, format string, args ...any) {
	if deps.Tracer != nil {
		deps.Tracer(format, args...)
		return
	}
	SlingTracef(format, args...)
}

// validateDeps checks that required SlingDeps fields are non-nil.
func validateDeps(deps SlingDeps) error {
	if deps.Cfg == nil {
		return fmt.Errorf("sling: Cfg is required")
	}
	if deps.Store == nil {
		return fmt.Errorf("sling: Store is required")
	}
	if deps.Runner == nil {
		return fmt.Errorf("sling: Runner is required")
	}
	return nil
}

// DoSling is the core logic for routing work to an agent.
// Returns structured data -- callers format display strings.
func DoSling(opts SlingOpts, deps SlingDeps, querier BeadQuerier) (SlingResult, error) {
	if err := validateDeps(deps); err != nil {
		return SlingResult{}, err
	}
	a := opts.Target
	var source *beads.Bead
	var guardErr error
	opts, source, guardErr = prepareRouteConditions(opts, deps)
	if guardErr != nil {
		return SlingResult{Target: a.QualifiedName()}, guardErr
	}
	if !opts.DryRun && source != nil && IsCustomSlingQuery(a) && source.Metadata[beadmeta.DispatchProviderMetadataKey] != "" {
		return resumeCommittedFormula(opts, deps, *source)
	}
	if !opts.DryRun && source != nil && source.Metadata[beadmeta.DispatchEffectStateMetadataKey] == "committed" && opts.Conditions != nil && source.Metadata[beadmeta.DispatchEffectIDMetadataKey] == opts.Conditions.Metadata[beadmeta.DispatchEffectIDMetadataKey] {
		return resumeCommittedFormula(opts, deps, *source)
	}
	if reconciled, ok, err := reconciledFormulaRoute(opts, deps, source); ok {
		return reconciled, err
	}
	if isReconciledEffect(opts, source, deps) {
		return SlingResult{BeadID: opts.BeadOrFormula, Target: a.QualifiedName(), Method: "bead", Idempotent: true, WorkflowID: source.Metadata["workflow_id"], WispRootID: source.Metadata[beadmeta.MoleculeIDMetadataKey]}, nil
	}
	result, preErr := preflight(opts, deps, querier)
	if preErr != nil {
		return result, preErr
	}
	if result.DryRun || result.Idempotent {
		if result.NudgeAgent != nil {
			target := agentutil.NormalizePoolRouteTarget(deps.Cfg, agentutil.RoutedToIdentity(&a))
			if err := checkNativeAdmission(context.Background(), deps.Cfg, deps.CityPath, target, opts.Conditions != nil && opts.Conditions.Metadata[beadmeta.DispatchEffectIDMetadataKey] != ""); err != nil {
				return result, err
			}
		}
		return result, nil
	}

	beadID := opts.BeadOrFormula
	if opts.IsFormula || usesFormulaBackedRoute(opts) {
		return slingGuardedFormula(opts, deps, source, result)
	}

	return slingPlainBead(opts, deps, beadID, result)
}

// preflight performs warnings, idempotency check, dry-run short-circuit,
// and cross-rig guard. Returns a partially populated result.
func preflight(opts SlingOpts, deps SlingDeps, querier BeadQuerier) (SlingResult, error) {
	a := opts.Target
	var result SlingResult
	result.Target = a.QualifiedName()

	if a.Suspended && !opts.Force {
		result.AgentSuspended = true
	}
	if !opts.Force && rigSuspended(deps.Cfg, a.Dir) {
		result.SuspendedRig = a.Dir
	}
	sp := agentutil.ScaleParamsFor(&a)
	if sp.Max == 0 && !opts.Force {
		result.PoolEmpty = true
	}

	if shouldValidateExistingBead(opts) {
		if err := validateExistingBead(opts.BeadOrFormula, deps); err != nil {
			return result, err
		}
	}
	if shouldGuardCrossRig(opts) {
		if err := CrossRigRouteError(opts.BeadOrFormula, a, deps.Cfg); err != nil {
			return result, err
		}
	}

	// Dependency cycle check: reject slings that would create a deadlock.
	if shouldCheckDepCycle(opts) {
		if err := DetectCycle(opts.BeadOrFormula, deps.Store); err != nil {
			return result, err
		}
	}

	// Pre-flight idempotency check.
	if shouldCheckBeadState(opts) && (opts.Conditions == nil || opts.Conditions.Metadata[beadmeta.DispatchEffectIDMetadataKey] == "") {
		if resolveIdempotentShortCircuit(opts, a, deps, querier, &result) {
			return result, nil
		}
	}
	if shouldValidateBuiltInRouteStoreReachable(opts, deps) {
		if err := validateBuiltInRouteStoreReachable(deps, opts.BeadOrFormula, a); err != nil {
			return result, fmt.Errorf("%w", err)
		}
	}

	// Dry-run: return early with preview info.
	if opts.DryRun {
		result.DryRun = true
		result.BeadID = opts.BeadOrFormula
		result.Method = "bead"
		if opts.IsFormula {
			result.Method = "formula"
		} else if opts.OnFormula != "" {
			result.Method = "on-formula"
		}
		return result, nil
	}

	if opts.ScopeKind != "" && !opts.IsFormula && opts.OnFormula == "" && (opts.NoFormula || a.EffectiveDefaultSlingFormula() == "") {
		return result, fmt.Errorf("--scope-kind/--scope-ref require a formula-backed workflow launch")
	}

	return result, nil
}

// resolveIdempotentShortCircuit runs the plain-bead pre-flight idempotency
// check and reports whether the sling is a settled no-op. When it returns true,
// result is populated for an early idempotent return; otherwise any bead-state
// warnings are appended to result and the sling proceeds. An explicit --on
// formula on a routed-but-unmoleculed root is not treated as idempotent, so the
// formula still attaches. If the molecule-attachment probe cannot complete, the
// fail-closed idempotent state is preserved and the probe failure is surfaced
// as a bead warning rather than silently flipping into a mutating attach path.
func resolveIdempotentShortCircuit(opts SlingOpts, a config.Agent, deps SlingDeps, querier BeadQuerier, result *SlingResult) bool {
	check := CheckBeadStateWithOptions(querier, opts.BeadOrFormula, a, deps, BeadCheckOptions{
		NoConvoy: opts.NoConvoy,
	})
	if check.Idempotent {
		decision, probeErr := onFormulaNeedsAttachment(opts, querier, deps)
		switch {
		case probeErr != nil:
			// The attachment probe failed, so we cannot prove the routed bead
			// lacks a live molecule. Preserve the fail-closed idempotent result
			// instead of risking a duplicate attachment, and surface the probe
			// failure so it is not silently swallowed.
			result.BeadWarnings = append(result.BeadWarnings, fmt.Sprintf(
				"could not verify molecule attachment for %s; treating --on as an idempotent no-op: %v",
				opts.BeadOrFormula, probeErr))
		case decision.NeedsAttach:
			// The bead is routed to the target but carries no molecule — an
			// earlier plain sling routed it raw. Do not treat --on as an
			// idempotent no-op; fall through so the formula attaches.
			check.Idempotent = false
		case decision.SkippedForClaim:
			// Another worker already claimed this bead and no molecule is
			// attached. Idempotency is preserved deliberately (do not re-attach
			// onto in-progress work), but say so explicitly: without this
			// warning the CLI prints only the generic "already routed" message,
			// giving no signal that the requested --on formula was never
			// attached or that --force would override the skip. opts.OnFormula
			// is empty when this was reached via the target's
			// default_sling_formula rather than an explicit --on, so fall back
			// to naming that instead of rendering an empty flag value.
			skippedFormula := opts.OnFormula
			if skippedFormula == "" {
				skippedFormula = a.EffectiveDefaultSlingFormula()
			}
			result.BeadWarnings = append(result.BeadWarnings, fmt.Sprintf(
				"bead %s is claimed by %s with no molecule attached; --on %s was skipped to avoid re-attaching onto in-progress work — rerun with --force to attach it anyway",
				opts.BeadOrFormula, decision.Assignee, skippedFormula))
		}
	}
	if !check.Idempotent {
		result.BeadWarnings = append(result.BeadWarnings, check.Warnings...)
		return false
	}
	result.Idempotent = true
	result.DryRun = opts.DryRun
	result.BeadID = opts.BeadOrFormula
	result.Method = "bead"
	// Honor --nudge even when the route is already in place. The bead is routed
	// to the target, but a warm pool slot may have missed its wake (its startup
	// nudge was swallowed, or work was routed after it went idle). Re-slinging
	// with --nudge must still deliver a wake; otherwise the idempotent
	// short-circuit silently drops it and the slot sits idle on work it never
	// began. The claim path is idempotent/CAS-safe, so a redundant nudge is
	// harmless. Suppressed for dry-run, which must not mutate or signal anything.
	if opts.Nudge && !opts.DryRun {
		result.NudgeAgent = &a
	}
	return true
}

// rigSuspended reports whether the named rig is marked suspended in config.
// The pool reconciler skips suspended rigs entirely, so a bead routed into
// one stalls silently — no worker ever spawns to claim it.
func rigSuspended(cfg *config.City, rigName string) bool {
	if cfg == nil || rigName == "" {
		return false
	}
	for _, r := range cfg.Rigs {
		if r.Name == rigName {
			return r.Suspended
		}
	}
	return false
}

func shouldValidateExistingBead(opts SlingOpts) bool {
	if opts.IsFormula || (opts.DryRun && opts.InlineText) {
		return false
	}
	return !opts.Force || usesFormulaBackedRoute(opts)
}

func usesFormulaBackedRoute(opts SlingOpts) bool {
	return opts.OnFormula != "" || (!opts.NoFormula && opts.Target.EffectiveDefaultSlingFormula() != "")
}

func shouldCheckDepCycle(opts SlingOpts) bool {
	// Only meaningful for plain-bead slinging where a bead ID is known.
	// Formula slinging creates new molecules whose deps aren't bead-graph deps.
	// Force and dry-run bypass cycle detection intentionally.
	return !opts.IsFormula && opts.OnFormula == "" && !opts.Force && !opts.DryRun && !opts.InlineText
}

func shouldGuardCrossRig(opts SlingOpts) bool {
	return !opts.IsFormula && !opts.Force && !opts.DryRun
}

func shouldCheckBeadState(opts SlingOpts) bool {
	return !opts.IsFormula && !opts.Force && !opts.Reassign && (!opts.DryRun || !opts.InlineText)
}

// attachmentDecision is the result of onFormulaNeedsAttachment: whether an
// --on formula attach should proceed on an otherwise-idempotent routed bead,
// and, when it should not, why -- so the caller can distinguish "nothing to
// do" (a molecule is already attached) from "skipped because another worker
// owns this bead" (SkippedForClaim), which needs its own warning rather than
// silently folding into the generic idempotent no-op.
type attachmentDecision struct {
	NeedsAttach bool
	// SkippedForClaim is true when the bead has no molecule but is already
	// claimed (Assignee set), so the attach was intentionally skipped rather
	// than performed. Only meaningful when NeedsAttach is false.
	SkippedForClaim bool
	// Assignee is the claiming identity when SkippedForClaim is true.
	Assignee string
}

// onFormulaNeedsAttachment reports whether this is an --on sling whose target
// bead the caller has already determined reads Idempotent (gc.routed_to ==
// target, or pool-labeled) but that has no attached molecule yet. The
// routed-idempotency check treats such a bead as a done no-op, but a bead can be
// routed raw by an earlier plain sling; a later `--on <formula>` must still
// attach the formula, or the repair root sits routed-but-unfanned. When a
// molecule is already attached, --on stays idempotent (skip), and re-attach is
// handled by the attachment path (CheckNoMoleculeChildren errors on a live
// molecule; a stale one is burned).
//
// The returned error is non-nil only when the molecule-attachment probe could
// not complete. In that case the result is (attachmentDecision{}, err): the
// caller cannot prove the bead is unmoleculed, so it must preserve the
// fail-closed idempotent state rather than clear it and risk minting a
// duplicate attachment.
func onFormulaNeedsAttachment(opts SlingOpts, querier BeadQuerier, deps SlingDeps) (attachmentDecision, error) {
	// Both formula-backed routes reach the same attach path, so the
	// routed-raw override has to apply to a target's default_sling_formula
	// as well as an explicit --on.
	if !usesFormulaBackedRoute(opts) {
		return attachmentDecision{}, nil
	}
	hasMolecule, err := HasMoleculeChildren(querier, opts.BeadOrFormula, deps.Store)
	if err != nil {
		return attachmentDecision{}, err
	}
	if hasMolecule {
		return attachmentDecision{}, nil
	}
	// No molecule attached. Only override idempotency for an UNCLAIMED bead — the
	// routed-raw footgun (gc.routed_to set, no assignee, no molecule). If a worker
	// has already claimed it (assignee set), leave it idempotent rather than
	// re-attaching a formula onto work in progress -- but report the claim so the
	// caller can warn that the attach was skipped, distinctly from "already done".
	bead, ok := BeadFromGetters(opts.BeadOrFormula, querier, deps.Store)
	if !ok {
		return attachmentDecision{}, nil
	}
	assignee := strings.TrimSpace(bead.Assignee)
	if assignee == "" {
		return attachmentDecision{NeedsAttach: true}, nil
	}
	return attachmentDecision{SkippedForClaim: true, Assignee: assignee}, nil
}

func shouldValidateBuiltInRouteStoreReachable(opts SlingOpts, deps SlingDeps) bool {
	return deps.Router != nil && !opts.IsFormula && !opts.DryRun
}

func validateExistingBead(beadID string, deps SlingDeps) error {
	querier := deps.ValidationQuerier
	if querier == nil {
		querier = deps.Store
	}
	return validateExistingBeadInQuerier(beadID, deps.StoreRef, querier)
}

func validateExistingBeadInQuerier(beadID, storeRef string, querier BeadQuerier) error {
	storeRef = strings.TrimSpace(storeRef)
	if storeRef == "" {
		storeRef = "local"
	}
	if querier == nil {
		return &BeadLookupError{BeadID: beadID, StoreRef: storeRef, Err: errors.New("store not configured")}
	}
	exists, err := probeBeadInQuerier(querier, beadID)
	if err != nil {
		return &BeadLookupError{BeadID: beadID, StoreRef: storeRef, Err: err}
	}
	if exists {
		return nil
	}
	return &MissingBeadError{BeadID: beadID, StoreRef: storeRef}
}

// rootOnlyVaporPourHint returns a sling-time diagnostic when a formula compiled
// to a root-only wisp specifically because it is a vapor formula without
// pour = true (cause (a) of the compile.go rootOnly rule). It deliberately stays
// silent for the genuinely step-less formula (cause (b), len(steps) == 0): there
// is no pour override to suggest there, so conflating the two would mislead. The
// hint surfaces via SlingResult.BeadWarnings; it changes neither routing nor the
// materialized wisp.
func rootOnlyVaporPourHint(formulaName string, recipe *formula.Recipe) string {
	if recipe == nil || !recipe.RootOnly || recipe.Pour || recipe.Phase != "vapor" {
		return ""
	}
	return fmt.Sprintf("note: %q is a vapor formula without `pour = true`; only the root step was materialized. Add `pour = true` for eager child-step expansion (see internal/formula/compile.go rootOnly rule).", formulaName)
}

// attachedBeadInstructionsDroppedHint returns a sling-time diagnostic when
// --on/default-formula attaches a formula to an existing bead whose own
// description carries real instructions. The formula wisp root's own
// description is always the FORMULA's own boilerplate
// (internal/formula/compile.go rootDesc), never the target bead's text, and
// no formula var exposes the bead's Description either — so a bead's
// instructions are otherwise silently invisible to the formula's rendered
// context, unless the caller explicitly carries them in via
// context_path/requirements_path (#3681). It changes neither routing nor
// the materialized wisp.
func attachedBeadInstructionsDroppedHint(querier BeadQuerier, beadID string, userVars []string) string {
	if querier == nil || beadID == "" {
		return ""
	}
	for _, v := range userVars {
		key, _, ok := strings.Cut(v, "=")
		if ok && (key == "context_path" || key == "requirements_path") {
			return ""
		}
	}
	bead, err := querier.Get(beadID)
	if err != nil || strings.TrimSpace(bead.Description) == "" {
		return ""
	}
	return fmt.Sprintf("note: bead %s's description is not carried into the formula's rendered context — pass --var context_path=<dir> or --var requirements_path=<doc> to include your instructions, or the formula's brainstorm will not see them.", beadID)
}

// slingPlainBead handles plain bead routing (no formula).
func slingPlainBead(opts SlingOpts, deps SlingDeps, beadID string, result SlingResult) (SlingResult, error) {
	return finalize(opts, deps, beadID, "bead", result)
}

// finalize executes the sling command, records telemetry, sets merge
// metadata, creates auto-convoy, pokes the controller, and signals nudge.
func finalize(opts SlingOpts, deps SlingDeps, beadID, method string, result SlingResult) (SlingResult, error) {
	a := opts.Target

	// Native routing and custom effect reservation consume the same source guard.
	switch {
	case IsCustomSlingQuery(a):
		if err := commitCustomPlainRoute(opts, deps, beadID); err != nil {
			return result, err
		}
	case deps.Router != nil:
		if err := validateBuiltInRouteStoreReachable(deps, beadID, a); err != nil {
			telemetry.RecordSling(context.Background(), a.QualifiedName(), TargetType(&a), method, err)
			return result, fmt.Errorf("%w", err)
		}
		req := RouteRequest{
			BeadID:     beadID,
			Target:     agentutil.RoutedToIdentity(&a),
			Force:      opts.Force,
			Conditions: opts.Conditions,
			Reassign:   opts.Reassign,
		}
		if opts.Merge != "" {
			req.Metadata = map[string]string{beadmeta.MergeStrategyMetadataKey: opts.Merge}
		}
		if err := deps.Router.Route(context.Background(), req); err != nil {
			telemetry.RecordSling(context.Background(), a.QualifiedName(), TargetType(&a), method, err)
			return result, fmt.Errorf("%w", err)
		}
	default:
		req := RouteRequest{BeadID: beadID, Target: agentutil.RoutedToIdentity(&a), Conditions: opts.Conditions, Reassign: opts.Reassign}
		if opts.Merge != "" {
			req.Metadata = map[string]string{beadmeta.MergeStrategyMetadataKey: opts.Merge}
		}
		if err := CommitRoute(context.Background(), deps.Store, deps.Cfg, deps.CityPath, req); err != nil {
			return result, err
		}
	}
	telemetry.RecordSling(context.Background(), a.QualifiedName(), TargetType(&a), method, nil)
	return finishRoute(opts, deps, beadID, method, result)
}

func finishRoute(opts SlingOpts, deps SlingDeps, beadID, method string, result SlingResult) (SlingResult, error) {
	a := opts.Target

	// Auto-convoy.
	if !opts.NoConvoy && !opts.IsFormula && deps.Store != nil {
		createAutoConvoy := true
		exists, err := ProbeBeadInStore(deps.Store, beadID)
		if err != nil {
			result.MetadataErrors = append(result.MetadataErrors,
				fmt.Sprintf("checking bead before auto-convoy: %v", err))
			createAutoConvoy = false
		} else if !exists {
			if opts.Force {
				result.MetadataErrors = append(result.MetadataErrors,
					fmt.Sprintf("forced dispatch skipped missing-bead validation for %s; no local auto-convoy created", beadID))
			} else {
				result.MetadataErrors = append(result.MetadataErrors,
					fmt.Sprintf("skipping auto-convoy: bead %s is not present in the local store", beadID))
			}
			createAutoConvoy = false
		}
		if createAutoConvoy {
			var convoyLabels []string
			if opts.Owned {
				convoyLabels = []string{"owned"}
			}
			convoy, err := deps.Store.Create(beads.Bead{
				Title:  fmt.Sprintf("sling-%s", beadID),
				Type:   "convoy",
				Labels: convoyLabels,
			})
			if err != nil {
				result.MetadataErrors = append(result.MetadataErrors,
					fmt.Sprintf("creating auto-convoy: %v", err))
			} else {
				// Use a "tracks" dep (convoy → bead) instead of parent-child
				// so the bead's existing parent (e.g. its epic) is preserved.
				// bd update --parent evicts any prior parent-child edge; the
				// tracks dep is additive and does not disturb the epic
				// rollup.
				if err := convoycore.TrackItem(deps.Store, convoy.ID, beadID); err != nil {
					result.MetadataErrors = append(result.MetadataErrors,
						fmt.Sprintf("linking bead to convoy: %v", err))
				} else {
					result.ConvoyID = convoy.ID
				}
			}
		}
	}

	result.BeadID = beadID
	result.Method = method

	// Poke controller.
	if !opts.SkipPoke && deps.Notify != nil {
		deps.Notify.PokeController(deps.CityPath)
	}

	// Signal nudge.
	if opts.Nudge {
		result.NudgeAgent = &a
	}

	return result, nil
}

func validateBuiltInRouteStoreReachable(deps SlingDeps, beadID string, a config.Agent) error {
	if deps.Cfg == nil || IsCustomSlingQuery(a) {
		return nil
	}
	if agentutil.AgentReachesWorkflowStore(deps.StoreRef, &a, deps.CityPath, deps.Cfg) {
		return nil
	}
	return &CrossStoreRouteError{
		BeadID:            beadID,
		StoreRef:          deps.StoreRef,
		Target:            a.QualifiedName(),
		ReachableStoreRef: agentutil.AgentReachableStoreLabel(&a, deps.CityPath, deps.CityName, deps.Cfg),
	}
}

type sourceWorkflowRoot struct {
	root     beads.Bead
	store    beads.Store
	storeRef string
}

func listSourceWorkflowRoots(deps SlingDeps, sourceBeadID string) ([]sourceWorkflowRoot, error) {
	sourceStoreRef := strings.TrimSpace(deps.StoreRef)
	if deps.SourceWorkflowStores == nil {
		return singleStoreSourceWorkflowRoots(deps.Store, sourceBeadID, sourceStoreRef)
	}
	stores, err := deps.SourceWorkflowStores()
	if err != nil {
		return nil, err
	}
	stores, err = ensureSelectedSourceWorkflowStorePresent(stores, deps.Store, sourceStoreRef)
	if err != nil {
		return nil, err
	}
	c := &sourceWorkflowRootCollector{
		deps:           deps,
		sourceBeadID:   sourceBeadID,
		sourceStoreRef: sourceStoreRef,
		seen:           make(map[string]struct{}, len(stores)),
	}
	for i, info := range stores {
		if err := c.scanStore(i, info); err != nil {
			return nil, err
		}
	}
	return c.result()
}

// singleStoreSourceWorkflowRoots lists live source-workflow roots when the deps
// expose only one store (no cross-store SourceWorkflowStores enumerator). Every
// scan failure is fatal here because there is no non-selected store to tolerate.
func singleStoreSourceWorkflowRoots(store beads.Store, sourceBeadID, sourceStoreRef string) ([]sourceWorkflowRoot, error) {
	roots, err := sourceworkflow.ListLiveRoots(store, sourceBeadID, sourceStoreRef, sourceStoreRef)
	if err != nil {
		return nil, err
	}
	out := make([]sourceWorkflowRoot, 0, len(roots))
	for _, root := range roots {
		out = append(out, sourceWorkflowRoot{
			root:     root,
			store:    store,
			storeRef: sourceStoreRef,
		})
	}
	return out, nil
}

// ensureSelectedSourceWorkflowStorePresent guarantees the selected source store
// is scanned. When a specific source store ref was requested but is absent from
// the enumerated stores, it prepends the deps store — the selected store is
// always strict — or fails when no such store is available to scan.
func ensureSelectedSourceWorkflowStorePresent(stores []SourceWorkflowStore, fallback beads.Store, sourceStoreRef string) ([]SourceWorkflowStore, error) {
	if sourceStoreRef == "" {
		return stores, nil
	}
	selectedPresent := slices.ContainsFunc(stores, func(info SourceWorkflowStore) bool {
		return info.Store != nil &&
			sourceworkflow.NormalizeSourceStoreRef(info.StoreRef) == sourceworkflow.NormalizeSourceStoreRef(sourceStoreRef)
	})
	if selectedPresent {
		return stores, nil
	}
	if fallback == nil {
		return nil, fmt.Errorf("source workflow store %s is unavailable to scan", sourceStoreRef)
	}
	return append([]SourceWorkflowStore{{Store: fallback, StoreRef: sourceStoreRef}}, stores...), nil
}

// sourceWorkflowRootCollector accumulates live source-workflow roots across every
// candidate store for a running sling. It tolerates unrelated (non-selected)
// store scan failures — warning through the deps sink — while keeping the
// selected source store strict, and dedups roots by store scope and root ID.
type sourceWorkflowRootCollector struct {
	deps           SlingDeps
	sourceBeadID   string
	sourceStoreRef string

	roots        []sourceWorkflowRoot
	seen         map[string]struct{}
	scanned      int
	firstScanErr error
}

// scanStore scans one candidate store for live source-workflow roots. A nil
// store is ignored. A tolerated non-selected scan failure warns and returns nil
// so the walk continues; a selected-store (or otherwise non-tolerable) failure
// returns the wrapped error to abort.
func (c *sourceWorkflowRootCollector) scanStore(index int, info SourceWorkflowStore) error {
	if info.Store == nil {
		return nil
	}
	rootStoreRef := strings.TrimSpace(info.StoreRef)
	matches, err := sourceworkflow.ListLiveRoots(info.Store, c.sourceBeadID, c.sourceStoreRef, rootStoreRef)
	if err != nil {
		return c.recordScanFailure(index, rootStoreRef, err)
	}
	c.scanned++
	c.appendRoots(index, info.Store, rootStoreRef, matches)
	return nil
}

// recordScanFailure remembers the first scan error and decides whether the
// failure may be tolerated. A tolerable failure is warned through the deps sink
// and returns nil so the store is skipped; every other failure returns the
// wrapped error so the caller aborts.
func (c *sourceWorkflowRootCollector) recordScanFailure(index int, rootStoreRef string, scanErr error) error {
	storeLabel := rootStoreRef
	if storeLabel == "" {
		storeLabel = fmt.Sprintf("store#%d", index)
	}
	wrapped := fmt.Errorf("listing live workflows in %s: %w", storeLabel, scanErr)
	if c.firstScanErr == nil {
		c.firstScanErr = wrapped
	}
	if !c.toleratesScanFailure(rootStoreRef) {
		return wrapped
	}
	c.deps.SourceWorkflowStoreScanWarning(rootStoreRef, scanErr)
	return nil
}

// toleratesScanFailure reports whether a scan failure on the given store may be
// skipped instead of aborting the walk. Tolerance requires a configured warning
// sink, resolved source and store refs, and a store that is not the strict
// selected source store — so a degraded scan is never silently swallowed.
func (c *sourceWorkflowRootCollector) toleratesScanFailure(rootStoreRef string) bool {
	if c.deps.SourceWorkflowStoreScanWarning == nil || c.sourceStoreRef == "" || rootStoreRef == "" {
		return false
	}
	return sourceworkflow.NormalizeSourceStoreRef(rootStoreRef) !=
		sourceworkflow.NormalizeSourceStoreRef(c.sourceStoreRef)
}

// appendRoots merges the live roots from one store into the result set, skipping
// duplicates keyed by store scope and root ID.
func (c *sourceWorkflowRootCollector) appendRoots(index int, store beads.Store, rootStoreRef string, matches []beads.Bead) {
	keyScope := rootStoreRef
	if keyScope == "" {
		keyScope = fmt.Sprintf("store#%d", index)
	}
	for _, root := range matches {
		key := keyScope + "\x00" + root.ID
		if _, ok := c.seen[key]; ok {
			continue
		}
		c.seen[key] = struct{}{}
		c.roots = append(c.roots, sourceWorkflowRoot{
			root:     root,
			store:    store,
			storeRef: rootStoreRef,
		})
	}
}

// result finalizes the sorted root set, applying the fail-closed fallback when
// no store could be scanned.
func (c *sourceWorkflowRootCollector) result() ([]sourceWorkflowRoot, error) {
	if c.scanned == 0 {
		if c.firstScanErr != nil {
			return nil, c.firstScanErr
		}
		return nil, fmt.Errorf("no source workflow stores were available to scan")
	}
	slices.SortFunc(c.roots, func(a, b sourceWorkflowRoot) int {
		if cmp := strings.Compare(a.storeRef, b.storeRef); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.root.ID, b.root.ID)
	})
	return c.roots, nil
}

func isGraphSlingFormula(ctx context.Context, formulaName string, searchPaths []string, vars map[string]string) (bool, error) {
	isGraph, _, err := graphv2.IsGraphV2Formula(formulaName, searchPaths)
	if err != nil {
		return false, err
	}
	if isGraph {
		return true, nil
	}
	recipe, err := formula.CompileWithoutRuntimeVarValidation(ctx, formulaName, searchPaths, vars)
	if err != nil {
		return false, err
	}
	return graphroute.IsCompiledGraphWorkflow(recipe), nil
}

func prepareGraphV2FormulaInvocation(ctx context.Context, formulaName, targetID string, opts SlingOpts, deps SlingDeps, a config.Agent) (graphv2.Invocation, bool, error) {
	searchPaths := SlingFormulaSearchPaths(deps, a)
	vars := buildGraphV2SlingFormulaVars(formulaName, targetID, opts.Vars, a, deps)
	inv, err := graphv2.PrepareInvocation(ctx, deps.Store, formulaName, searchPaths, targetID, vars)
	if err != nil {
		return graphv2.Invocation{}, false, err
	}
	isGraph := formula.UsesGraphCompiler(inv.Formula)
	return inv, isGraph, nil
}

func validateSlingFormulaRuntimeVars(ctx context.Context, formulaName string, searchPaths []string, opts molecule.Options) error {
	recipe, err := formula.CompileWithoutRuntimeVarValidation(ctx, formulaName, searchPaths, opts.Vars)
	if err != nil {
		return err
	}
	return molecule.ValidateRecipeRuntimeVars(recipe, opts)
}

func checkLegacySourceWorkflowConflict(deps SlingDeps, beadID string) error {
	roots, err := listSourceWorkflowRoots(deps, beadID)
	if err != nil {
		return fmt.Errorf("list live workflows for %s: %w", beadID, err)
	}
	if len(roots) == 0 {
		return nil
	}
	workflowIDs := make([]string, 0, len(roots))
	for _, info := range roots {
		if info.root.ID != "" {
			workflowIDs = append(workflowIDs, info.root.ID)
		}
	}
	slices.Sort(workflowIDs)
	return &sourceworkflow.ConflictError{
		SourceBeadID: beadID,
		WorkflowIDs:  workflowIDs,
	}
}

func validateBatchSlingFormulaRuntimeVars(ctx context.Context, formulaName string, searchPaths []string, opts SlingOpts, open []beads.Bead, a config.Agent, deps SlingDeps) error {
	for _, child := range open {
		childVars := BuildSlingFormulaVars(formulaName, child.ID, opts.Vars, a, deps)
		if err := validateSlingFormulaRuntimeVars(ctx, formulaName, searchPaths, molecule.Options{
			Title: opts.Title,
			Vars:  childVars,
		}); err != nil {
			return fmt.Errorf("child %s: %w", child.ID, err)
		}
	}
	return nil
}

func sourceWorkflowLockScope(deps SlingDeps) string {
	return sourceworkflow.LockScopeForStoreRef(deps.CityPath, "", deps.StoreRef, func(rigName string) (string, bool) {
		if deps.Cfg != nil {
			for _, rig := range deps.Cfg.Rigs {
				if rig.Name != rigName {
					continue
				}
				return rig.Path, true
			}
		}
		return "", false
	})
}

func listContainerChildren(querier BeadChildQuerier, containerID string, includeClosed bool) ([]beads.Bead, error) {
	if store, ok := querier.(beads.Store); ok {
		return convoycore.Members(store, containerID, includeClosed)
	}
	return querier.List(beads.ListQuery{
		ParentID:      containerID,
		IncludeClosed: includeClosed,
		Sort:          beads.SortCreatedAsc,
	})
}

// DoSlingBatch handles convoy expansion before delegating to DoSling.
func DoSlingBatch(opts SlingOpts, deps SlingDeps, querier BeadChildQuerier) (SlingResult, error) {
	a := opts.Target

	// Formula mode, nil querier → delegate directly.
	if opts.IsFormula || querier == nil {
		return DoSling(opts, deps, querier)
	}

	containerQuerier := BeadQuerier(querier)
	b, err := querier.Get(opts.BeadOrFormula)
	if err != nil {
		if !errors.Is(err, beads.ErrNotFound) {
			return SlingResult{Target: a.QualifiedName()}, &BeadLookupError{
				BeadID:   opts.BeadOrFormula,
				StoreRef: deps.StoreRef,
				Err:      err,
			}
		}
		if selected, ok := selectedStoreContainer(opts, deps); ok {
			b = selected
			// The caller's querier could not see the container, so deps.Store
			// becomes authoritative for both validation and child expansion.
			querier = deps.Store
			containerQuerier = deps.Store
		} else {
			singleOpts := opts
			singleOpts.IsFormula = false
			return DoSling(singleOpts, deps, querier)
		}
	}
	if b.Type == "epic" || beads.IsContainerType(b.Type) {
		if shouldValidateExistingBead(opts) {
			if err := validateExistingBeadInQuerier(opts.BeadOrFormula, deps.StoreRef, containerQuerier); err != nil {
				return SlingResult{Target: a.QualifiedName()}, err
			}
		}
	}
	if b.Type == "epic" {
		return SlingResult{}, fmt.Errorf("bead %s is an epic; first-class support is for convoys only", b.ID)
	}

	useFormula := opts.OnFormula
	if useFormula == "" && !opts.IsFormula && !opts.NoFormula && a.EffectiveDefaultSlingFormula() != "" {
		useFormula = a.EffectiveDefaultSlingFormula()
	}

	if !beads.IsContainerType(b.Type) {
		singleOpts := opts
		singleOpts.IsFormula = false
		singleDeps := deps
		singleDeps.ValidationQuerier = containerQuerier
		return DoSling(singleOpts, singleDeps, querier)
	}
	if !opts.DryRun && !IsCustomSlingQuery(a) {
		target := agentutil.NormalizePoolRouteTarget(deps.Cfg, agentutil.RoutedToIdentity(&a))
		if err := checkNativeAdmission(context.Background(), deps.Cfg, deps.CityPath, target, opts.Conditions != nil && opts.Conditions.Metadata[beadmeta.DispatchEffectIDMetadataKey] != ""); err != nil {
			return SlingResult{Target: a.QualifiedName()}, err
		}
	}

	if useFormula != "" {
		_, isGraph, err := prepareGraphV2FormulaInvocation(context.Background(), useFormula, b.ID, opts, deps, a)
		if err != nil {
			return SlingResult{}, fmt.Errorf("instantiating formula %q on %s %s: %w", useFormula, b.Type, b.ID, err)
		}
		if isGraph {
			singleDeps := deps
			singleDeps.ValidationQuerier = containerQuerier
			return DoSling(opts, singleDeps, containerQuerier)
		}
	}
	if opts.Conditions != nil {
		return SlingResult{Target: a.QualifiedName()}, fmt.Errorf("one guarded effect cannot expand several child dispatches; sling each exact leaf bead")
	}

	children, err := listContainerChildren(querier, b.ID, true)
	if err != nil {
		return SlingResult{}, fmt.Errorf("listing children of %s: %w", b.ID, err)
	}

	var open, skipped []beads.Bead
	for _, c := range children {
		if c.Status == "open" || opts.Reassign && c.Status == "in_progress" {
			open = append(open, c)
		} else {
			skipped = append(skipped, c)
		}
	}

	if len(open) == 0 {
		return SlingResult{}, fmt.Errorf("%s %s has no open children", b.Type, b.ID)
	}

	// Cross-rig guard on container.
	if !opts.Force && !opts.DryRun {
		if err := CrossRigRouteError(b.ID, a, deps.Cfg); err != nil {
			return SlingResult{}, err
		}
	}

	// Dry-run: return early with container preview info.
	if opts.DryRun {
		var batchResult SlingResult
		batchResult.DryRun = true
		batchResult.Target = a.QualifiedName()
		batchResult.BeadID = b.ID
		batchResult.ContainerType = b.Type
		batchResult.Method = "batch"
		batchResult.Total = len(children)
		batchResult.Routed = len(open)
		batchResult.Skipped = len(skipped)
		return batchResult, nil
	}

	// Pre-check molecule attachments.
	var batchResult SlingResult
	batchResult.Target = a.QualifiedName()
	batchResult.BeadID = b.ID
	batchResult.ContainerType = b.Type
	// Validate the whole batch before any child selects a provider.
	var isGraph bool
	if useFormula != "" {
		formulaVars := BuildSlingFormulaVars(useFormula, "", opts.Vars, a, deps)
		searchPaths := SlingFormulaSearchPaths(deps, a)
		var err error
		isGraph, err = isGraphSlingFormula(context.Background(), useFormula, searchPaths, formulaVars)
		if err != nil {
			return SlingResult{}, fmt.Errorf("instantiating formula %q on %s %s: %w", useFormula, b.Type, b.ID, err)
		}
		if err := validateBatchSlingFormulaRuntimeVars(context.Background(), useFormula, searchPaths, opts, open, a, deps); err != nil {
			return SlingResult{}, fmt.Errorf("instantiating formula %q on %s %s: %w", useFormula, b.Type, b.ID, err)
		}
		checkAttachments := CheckBatchNoMoleculeChildren
		if isGraph && opts.Force {
			checkAttachments = CheckBatchNoMoleculeChildrenAllowLiveWorkflow
		}
		if err := checkAttachments(querier, open, deps.Store, &batchResult); err != nil {
			return batchResult, fmt.Errorf("%w", err)
		}
	}

	batchMethod := "batch"
	if opts.OnFormula != "" {
		batchMethod = "batch-on"
	} else if !opts.NoFormula && a.EffectiveDefaultSlingFormula() != "" {
		batchMethod = "batch-default-on"
	}
	batchResult.Method = batchMethod
	batchResult.Total = len(children)

	routed := 0
	failed := 0
	idempotent := 0
	// childErrors preserves typed child errors so errors.As at the top-level
	// (cmdSling) can recover a *sourceworkflow.ConflictError emitted by any
	// child and map it to exit code 3 + the cleanup hint. Stringifying into
	// childResult.FailReason alone loses the type.
	var childErrors []error
	for _, child := range open {
		childResult := SlingChildResult{BeadID: child.ID}

		if !opts.Force && !opts.Reassign {
			check := CheckBeadStateWithOptions(querier, child.ID, a, deps, BeadCheckOptions{
				NoConvoy: opts.NoConvoy,
			})
			if check.Idempotent {
				childResult.Skipped = true
				batchResult.Children = append(batchResult.Children, childResult)
				idempotent++
				continue
			}
			batchResult.BeadWarnings = append(batchResult.BeadWarnings, check.Warnings...)
		}

		if shouldValidateBuiltInRouteStoreReachable(opts, deps) {
			if err := validateBuiltInRouteStoreReachable(deps, child.ID, a); err != nil {
				childResult.Failed = true
				childResult.FailReason = err.Error()
				batchResult.Children = append(batchResult.Children, childResult)
				childErrors = append(childErrors, err)
				telemetry.RecordSling(context.Background(), a.QualifiedName(), TargetType(&a), batchMethod, err)
				failed++
				continue
			}
		}

		childOpts := opts
		childOpts.BeadOrFormula = child.ID
		childOpts.Conditions = nil
		childOpts.NoConvoy, childOpts.SkipPoke = true, true
		dispatched, err := DoSling(childOpts, deps, deps.Store)
		batchResult.BeadWarnings = append(batchResult.BeadWarnings, dispatched.BeadWarnings...)
		batchResult.MetadataErrors = append(batchResult.MetadataErrors, dispatched.MetadataErrors...)
		switch {
		case err != nil:
			childResult.Failed, childResult.FailReason = true, err.Error()
			failed++
			childErrors = append(childErrors, err)
		case dispatched.Idempotent:
			childResult.Skipped = true
			idempotent++
		default:
			childResult.Routed = true
			childResult.WorkflowID, childResult.WispRootID, childResult.FormulaName = dispatched.WorkflowID, dispatched.WispRootID, dispatched.FormulaName
			routed++
		}
		batchResult.Children = append(batchResult.Children, childResult)
	}

	// Record skipped (non-open) children with their status.
	for _, child := range skipped {
		batchResult.Children = append(batchResult.Children, SlingChildResult{
			BeadID:  child.ID,
			Status:  child.Status,
			Skipped: true,
		})
	}

	batchResult.Routed = routed
	batchResult.Failed = failed
	batchResult.Skipped = idempotent + len(skipped)
	batchResult.IdempotentCt = idempotent

	if opts.Nudge && routed > 0 {
		batchResult.NudgeAgent = &a
	}

	if failed > 0 {
		summary := fmt.Errorf("%d/%d children failed", failed, len(open))
		// errors.Join threads typed child errors through Unwrap() []error so
		// errors.As at the CLI/API boundary can recover *ConflictError and map
		// it to exit 3 + the cleanup hint; the summary stays first for the
		// human-readable message.
		joined := append([]error{summary}, childErrors...)
		return batchResult, errors.Join(joined...)
	}
	return batchResult, nil
}

func selectedStoreContainer(opts SlingOpts, deps SlingDeps) (beads.Bead, bool) {
	if deps.Store == nil {
		return beads.Bead{}, false
	}
	b, err := deps.Store.Get(opts.BeadOrFormula)
	if err != nil {
		return beads.Bead{}, false
	}
	return b, b.Type == "epic" || beads.IsContainerType(b.Type)
}

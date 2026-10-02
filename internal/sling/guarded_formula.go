package sling

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"

	"github.com/gastownhall/gascity/internal/agentutil"
	"github.com/gastownhall/gascity/internal/beadmeta"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/formula"
	"github.com/gastownhall/gascity/internal/graphroute"
	"github.com/gastownhall/gascity/internal/graphv2"
	"github.com/gastownhall/gascity/internal/molecule"
	"github.com/gastownhall/gascity/internal/sourceworkflow"
)

const dispatchCandidateIDsKey = "gc.dispatch_candidate_ids"
const dispatchReplacedRootKey = "gc.dispatch_replaced_roots"
const dispatchSourceConditionsKey = "gc.dispatch_source_conditions"
const dispatchActivationConditionsKey = "gc.dispatch_activation_conditions"
const dispatchTargetKey = "gc.dispatch_target"
const customDispatchProviderKey = "gc.dispatch_provider"

func slingGuardedFormula(opts SlingOpts, deps SlingDeps, source *beads.Bead, result SlingResult) (SlingResult, error) {
	ctx := context.Background()
	a := opts.Target
	name, method, sourceID := opts.OnFormula, "on-formula", opts.BeadOrFormula
	if opts.IsFormula {
		name, method, sourceID = opts.BeadOrFormula, "formula", ""
	}
	if name == "" {
		name, method = a.EffectiveDefaultSlingFormula(), "default-on-formula"
	}
	effectID := ""
	if opts.Conditions != nil {
		effectID = opts.Conditions.Metadata[DispatchEffectIDKey]
	}
	custom := IsCustomSlingQuery(a)
	if custom && opts.IsFormula {
		reconciled, found, err := resumeUnresolvedCustomFormula(opts, deps)
		if found || err != nil {
			return reconciled, err
		}
	}
	if custom {
		if effectID == "" {
			effectID = "gc-custom-" + rand.Text()
		}
		_, _, _, err := customDeliveryCommand(opts, deps, sourceID)
		if err != nil {
			return result, err
		}
	}
	target := agentutil.NormalizePoolRouteTarget(deps.Cfg, agentutil.RoutedToIdentity(&a))
	if err := checkNativeAdmission(ctx, deps.Cfg, deps.CityPath, target, effectID != ""); err != nil {
		return result, err
	}
	if _, ok := beads.GuardedUpdateWriterFor(deps.Store); !ok {
		return result, beads.ErrConditionalWriteUnsupported
	}
	if _, ok := beads.SingleTransactionStoreFor(deps.Store); !ok {
		return result, beads.ErrConditionalWriteUnsupported
	}
	candidateStore := deps.graphStore()
	if _, ok := beads.GuardedUpdateWriterFor(candidateStore); !ok {
		return result, beads.ErrConditionalWriteUnsupported
	}
	if _, ok := beads.SingleTransactionStoreFor(candidateStore); !ok {
		return result, beads.ErrConditionalWriteUnsupported
	}
	candidate, err := originalDispatchCandidate(candidateStore, effectID, sourceID, name, target)
	if err != nil {
		return result, err
	}
	inv, graph, err := prepareGraphV2FormulaInvocation(ctx, name, sourceID, opts, deps, a)
	if err != nil {
		return result, err
	}
	vars := BuildSlingFormulaVars(name, sourceID, opts.Vars, a, deps)
	if graph {
		vars = inv.Vars
	}
	recipe, err := formula.CompileWithoutRuntimeVarValidation(ctx, name, SlingFormulaSearchPaths(deps, a), vars)
	if err != nil {
		return result, err
	}
	graph = graph || graphroute.IsCompiledGraphWorkflow(recipe)
	if len(recipe.Steps) == 0 {
		return result, fmt.Errorf("formula %q has no steps", name)
	}
	var sourcePredecessors []beads.Bead
	if source != nil {
		var providerRoots []string
		sourcePredecessors, providerRoots, err = guardedSourcePredecessors(deps, *source, graph && opts.Force)
		if err != nil {
			return result, err
		}
		for _, rootID := range providerRoots {
			if err := addReplacementRoot(recipe, rootID); err != nil {
				return result, err
			}
		}
		if !graph || !opts.Force {
			if err := checkLegacySourceWorkflowConflict(deps, sourceID); err != nil {
				return result, err
			}
		}
	}
	if opts.IsFormula && a.SupportsMultipleSessions() && !formula.RecipeHasReadySurface(recipe) {
		return result, fmt.Errorf("formula %q root is not Ready-visible work for a pool", name)
	}
	createOpts := molecule.Options{Title: opts.Title, Vars: vars, DeferAssignees: true, PriorityOverride: BeadPriorityOverride(deps.Store, sourceID)}
	if effectID != "" {
		createOpts.IdempotencyKey = effectID
	}
	if err := molecule.ValidateRecipeRuntimeVars(recipe, createOpts); err != nil {
		return result, err
	}
	if recipe.Steps[0].Metadata == nil {
		recipe.Steps[0].Metadata = map[string]string{}
	}
	recipe.Steps[0].Metadata[beadmeta.AttachFencePendingMetadataKey] = "true"
	recipe.Steps[0].Metadata[dispatchTargetKey] = target
	if effectID != "" {
		recipe.Steps[0].Metadata[DispatchEffectIDKey] = effectID
	}
	if sourceID != "" {
		recipe.Steps[0].Metadata["gc.dispatch_source_bead"] = sourceID
	}
	if source != nil {
		raw, err := encodeRouteConditions(*opts.Conditions)
		if err != nil {
			return result, err
		}
		recipe.Steps[0].Metadata[dispatchSourceConditionsKey] = raw
	}
	if candidate == nil {
		candidate, err = InstantiateCompiledSlingFormula(ctx, recipe, name, createOpts, sourceID, opts.ScopeKind, opts.ScopeRef, a, deps, opts.Force)
	}
	if err != nil {
		graphv2.CloseSyntheticInputConvoy(deps.Store, inv.InputConvoy, sourceID)
		return result, err
	}
	root, err := beads.HandlesFor(candidateStore).Live.Get(candidate.RootID)
	if err != nil {
		return result, err
	}
	if err := validateStagedProviderIntent(root, sourceID, name, target); err != nil {
		return result, err
	}
	if IsWorkflowAttachment(root) != graph {
		return result, &RouteConflictError{sourceID, "requested workflow contract differs from the original staged provider"}
	}
	if root.Metadata[beadmeta.AttachFencePendingMetadataKey] != "true" {
		return result, &RouteConflictError{sourceID, "existing provider workflow is not this attempt's pending candidate"}
	}
	if effectID != "" && root.Metadata[DispatchEffectIDKey] != effectID {
		return result, &RouteConflictError{sourceID, "existing provider workflow belongs to a different effect"}
	}
	if source != nil {
		var original beads.UpdateConditions
		if err := json.Unmarshal([]byte(root.Metadata[dispatchSourceConditionsKey]), &original); err != nil {
			return result, fmt.Errorf("original staged source conditions: %w", err)
		}
		if original.Status == nil || original.Assignee == nil || original.Title == nil || original.Description == nil || original.AcceptanceCriteria == nil || original.Labels == nil {
			return result, fmt.Errorf("original staged source conditions are incomplete")
		}
		if _, err := routeConditions(*source, &original); err != nil {
			return result, err
		}
		opts.Conditions = &original
	}
	mapping, err := json.Marshal(candidate.IDMapping)
	if err != nil {
		return result, err
	}
	if err := candidateStore.SetMetadata(candidate.RootID, dispatchCandidateIDsKey, string(mapping)); err != nil {
		return result, err
	}
	result.FormulaName, result.Method = name, method
	result.Deprecations = append(result.Deprecations, inv.Deprecations...)
	if graph {
		result.WorkflowID = candidate.RootID
	} else {
		result.WispRootID = candidate.RootID
	}
	if source == nil {
		b, err := beads.HandlesFor(candidateStore).Live.Get(candidate.RootID)
		if err != nil {
			return result, err
		}
		source, sourceID = &b, b.ID
		conditions, err := routeConditions(b, nil)
		if err != nil {
			return result, err
		}
		opts.Conditions = &conditions
	}
	metadata := map[string]string{}
	if graph {
		metadata["workflow_id"] = candidate.RootID
		metadata[beadmeta.ExecutionRoutedToMetadataKey] = target
		if opts.IsFormula {
			metadata[beadmeta.RoutedToMetadataKey] = target
		}
	} else {
		if !opts.IsFormula {
			metadata[beadmeta.MoleculeIDMetadataKey] = candidate.RootID
		}
		metadata[beadmeta.RoutedToMetadataKey] = target
	}
	if opts.Merge != "" {
		metadata[beadmeta.MergeStrategyMetadataKey] = opts.Merge
	}
	metadata[DispatchEffectStateKey] = "routed"
	if effectID != "" {
		metadata[DispatchEffectIDKey] = effectID
	}
	if opts.IsFormula {
		metadata["gc.dispatch_source_bead"] = sourceID
	}
	if custom {
		_, _, identity, err := customDeliveryCommand(opts, deps, sourceID)
		if err != nil {
			return result, err
		}
		metadata[customDispatchProviderKey] = identity
	}
	update := beads.UpdateOpts{Metadata: metadata}
	if opts.Reassign {
		empty, open := "", "open"
		update.Assignee = &empty
		if source.Status == "in_progress" {
			update.Status = &open
		}
	}
	metadata["gc.dispatch_activation_status"], metadata["gc.dispatch_activation_assignee"] = source.Status, source.Assignee
	if update.Status != nil {
		metadata["gc.dispatch_activation_status"] = *update.Status
	}
	if update.Assignee != nil {
		metadata["gc.dispatch_activation_assignee"] = *update.Assignee
	}
	// Same owner: delivery, source ownership, and all candidate activation
	// become visible together. Cross-owner: the source commits the selected
	// candidate, then the existing durable pending-marker owner activates it.
	sourceWriter, _ := beads.GuardedUpdateWriterFor(deps.Store)
	candidateWriter, _ := beads.GuardedUpdateWriterFor(candidateStore)
	sameOwner := sourceWriter == candidateWriter || opts.IsFormula
	if custom {
		sourceDeps := deps
		if opts.IsFormula {
			sourceDeps.Store = candidateStore
		}
		update.Metadata[DispatchEffectStateKey] = "committed"
		if opts.IsFormula {
			for key, expected := range activationConditions(*source).Metadata {
				opts.Conditions.Metadata[key] = expected
			}
		}
		if err := freezeActivationConditions(&update, *opts.Conditions); err != nil {
			return result, err
		}
		if err := commitSelectedSource(ctx, sourceDeps.Store, sourceID, update, *opts.Conditions, sourcePredecessors, sourceDeps, target, true); err != nil {
			return result, err
		}
		if err := deliverSelectedCustomFormula(opts, sourceDeps, sourceID, candidateStore, candidate, graph, effectID, target); err != nil {
			return result, err
		}
	} else if sameOwner {
		if err := commitFormulaTransaction(ctx, candidateStore, sourceID, update, *opts.Conditions, candidate, graph, deps, effectID != "", sourcePredecessors); err != nil {
			return result, err
		}
	} else {
		update.Metadata[DispatchEffectStateKey] = "committed"
		if err := freezeActivationConditions(&update, *opts.Conditions); err != nil {
			return result, err
		}
		if err := commitSelectedSource(ctx, deps.Store, sourceID, update, *opts.Conditions, sourcePredecessors, deps, target, effectID != ""); err != nil {
			return result, err
		}
		if err := finishCommittedFormula(deps, sourceID, candidateStore, candidate, graph, opts.IsFormula, effectID, target, "committed"); err != nil {
			return result, err
		}
	}
	if hint := rootOnlyVaporPourHint(name, recipe); hint != "" {
		result.BeadWarnings = append(result.BeadWarnings, hint)
	}
	if !opts.IsFormula {
		if hint := attachedBeadInstructionsDroppedHint(deps.Store, sourceID, opts.Vars); hint != "" {
			result.BeadWarnings = append(result.BeadWarnings, hint)
		}
	}
	result.BeadID = sourceID
	if opts.IsFormula {
		result.BeadID = candidate.RootID
	}
	if !graph {
		return finishRoute(opts, deps, sourceID, method, result)
	}
	if !opts.SkipPoke && deps.Notify != nil {
		deps.Notify.PokeController(deps.CityPath)
		if graph {
			deps.Notify.PokeControlDispatch(deps.CityPath)
		}
	}
	if opts.Nudge {
		result.NudgeAgent = &a
	}
	if graph {
		emitCurrentExecutionFacts(deps, candidateStore, candidate.RootID, a.QualifiedName(), name)
	}
	return result, nil
}

func activationConditions(b beads.Bead) beads.UpdateConditions {
	metadata := map[string]string{}
	for _, key := range []string{beadmeta.AttachFencePendingMetadataKey, DispatchEffectIDKey, DispatchEffectStateKey, customDispatchProviderKey, dispatchCandidateIDsKey, dispatchReplacedRootKey, dispatchSourceConditionsKey, dispatchActivationConditionsKey, dispatchTargetKey, "gc.dispatch_source_bead", beadmeta.RootBeadIDMetadataKey, beadmeta.FormulaNameMetadataKey, beadmeta.FormulaContractMetadataKey, beadmeta.RoutedToMetadataKey, beadmeta.ExecutionRoutedToMetadataKey, molecule.DeferredAssigneeMetadataKey, molecule.DeferredTypeMetadataKey, molecule.DeferredRoutedToMetadataKey, molecule.DeferredExecutionRoutedToMetadataKey, "handoff.conflict_state"} {
		metadata[key] = b.Metadata[key]
	}
	return beads.UpdateConditions{Status: &b.Status, Assignee: &b.Assignee, Title: &b.Title, Description: &b.Description, AcceptanceCriteria: &b.AcceptanceCriteria, Labels: &b.Labels, Metadata: metadata}
}

func commitFormulaTransaction(ctx context.Context, store beads.Store, sourceID string, sourceUpdate beads.UpdateOpts, conditions beads.UpdateConditions, candidate *molecule.Result, graph bool, deps SlingDeps, required bool, sourcePredecessors []beads.Bead) error {
	atomic, ok := beads.SingleTransactionStoreFor(store)
	if !ok {
		return beads.ErrConditionalWriteUnsupported
	}
	ids := make([]string, 0, len(candidate.IDMapping))
	for _, id := range candidate.IDMapping {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	rows := make(map[string]beads.Bead, len(ids))
	for _, id := range ids {
		b, err := beads.HandlesFor(store).Live.Get(id)
		if err != nil {
			return err
		}
		if b.Metadata["handoff.conflict_state"] == "hold" {
			return &RouteConflictError{id, "candidate hold is active"}
		}
		rows[id] = b
	}
	replaced, err := guardedReplacementRows(store, rows[candidate.RootID])
	if err != nil {
		return err
	}
	return atomic.TxSingle("gc: commit guarded sling candidate", func(tx beads.Tx) error {
		writer, ok := tx.(beads.GuardedUpdateWriter)
		if !ok {
			return beads.ErrConditionalWriteUnsupported
		}
		target := sourceUpdate.Metadata[beadmeta.RoutedToMetadataKey]
		if target == "" {
			target = sourceUpdate.Metadata[beadmeta.ExecutionRoutedToMetadataKey]
		}
		if err := checkNativeAdmission(ctx, deps.Cfg, deps.CityPath, target, required); err != nil {
			return err
		}
		update := sourceUpdate
		if sourceID == candidate.RootID {
			conditions.Metadata = maps.Clone(conditions.Metadata)
			for key, expected := range activationConditions(rows[sourceID]).Metadata {
				if _, captured := conditions.Metadata[key]; !captured {
					conditions.Metadata[key] = expected
				}
			}
			activation := molecule.DeferredRoutingActivationUpdate(rows[sourceID])
			activation.Metadata = maps.Clone(activation.Metadata)
			if activation.Metadata == nil {
				activation.Metadata = map[string]string{}
			}
			maps.Copy(activation.Metadata, sourceUpdate.Metadata)
			activation.Metadata[beadmeta.AttachFencePendingMetadataKey] = ""
			if graph {
				status := "in_progress"
				activation.Status = &status
			}
			update = activation
		}
		applied, err := writer.UpdateGuarded(sourceID, update, conditions)
		if err != nil {
			return err
		}
		if !applied {
			return &RouteConflictError{sourceID, "source ownership or hold changed before route commit"}
		}
		if err := closeGuardedReplacement(writer, replaced); err != nil {
			return err
		}
		if err := closeGuardedReplacement(writer, sourcePredecessors); err != nil {
			return err
		}
		for _, id := range ids {
			if id == sourceID {
				continue
			}
			update := molecule.DeferredRoutingActivationUpdate(rows[id])
			if update.Metadata == nil {
				update.Metadata = map[string]string{}
			}
			if id == candidate.RootID {
				update.Metadata[beadmeta.AttachFencePendingMetadataKey] = ""
				if graph {
					status := "in_progress"
					update.Status = &status
				}
			}
			applied, err := writer.UpdateGuarded(id, update, activationConditions(rows[id]))
			if err != nil {
				return err
			}
			if !applied {
				return &RouteConflictError{id, "candidate ownership or hold changed before activation"}
			}
		}
		return nil
	})
}

func finishCommittedFormula(deps SlingDeps, sourceID string, store beads.Store, candidate *molecule.Result, graph, standalone bool, effectID, target, selectedState string) error {
	root, err := beads.HandlesFor(store).Live.Get(candidate.RootID)
	if err != nil {
		return err
	}
	selfCandidate := standalone && sourceID == candidate.RootID && root.Metadata["gc.dispatch_source_bead"] == sourceID
	if root.Metadata[beadmeta.AttachFencePendingMetadataKey] != "" {
		if root.Metadata["handoff.conflict_state"] == "hold" {
			return &RouteConflictError{root.ID, "selected workflow is held"}
		}
		source, err := beads.HandlesFor(deps.Store).Live.Get(sourceID)
		if err != nil {
			return err
		}
		if source.Metadata["handoff.conflict_state"] == "hold" || source.Status != source.Metadata["gc.dispatch_activation_status"] || source.Assignee != source.Metadata["gc.dispatch_activation_assignee"] {
			return &RouteConflictError{sourceID, "selected source ownership or hold changed before provider activation"}
		}
		if source.Metadata[DispatchEffectStateKey] != selectedState || source.Metadata[DispatchEffectIDKey] != effectID ||
			(!selfCandidate && (graph && source.Metadata["workflow_id"] != candidate.RootID || !graph && source.Metadata[beadmeta.MoleculeIDMetadataKey] != candidate.RootID)) {
			return &RouteConflictError{sourceID, "source no longer selects this provider candidate"}
		}
		original, err := selectedSourceConditions(source)
		if err != nil {
			return err
		}
		if err := commitSelectedSource(context.Background(), deps.Store, sourceID, beads.UpdateOpts{Metadata: map[string]string{DispatchEffectStateKey: selectedState}}, original, nil, deps, target, effectID != ""); err != nil {
			return err
		}
		metadata := map[string]string{beadmeta.RoutedToMetadataKey: target}
		if graph {
			metadata[beadmeta.ExecutionRoutedToMetadataKey] = target
		}
		if selfCandidate {
			metadata[DispatchEffectStateKey] = "routed"
		}
		if err := commitFormulaTransaction(context.Background(), store, root.ID, beads.UpdateOpts{Metadata: metadata}, activationConditions(root), candidate, graph, deps, effectID != "", nil); err != nil {
			return err
		}
	} else if root.Status != "in_progress" && root.Status != "closed" && graph {
		return &RouteConflictError{root.ID, "selected workflow has no completed activation"}
	}
	b, err := beads.HandlesFor(deps.Store).Live.Get(sourceID)
	if err != nil {
		return err
	}
	if b.Metadata[DispatchEffectStateKey] == "routed" && b.Metadata[DispatchEffectIDKey] == effectID &&
		(graph && b.Metadata["workflow_id"] == candidate.RootID && b.Metadata[beadmeta.ExecutionRoutedToMetadataKey] == target ||
			!graph && b.Metadata[beadmeta.MoleculeIDMetadataKey] == candidate.RootID && b.Metadata[beadmeta.RoutedToMetadataKey] == target ||
			selfCandidate && b.Metadata[beadmeta.RoutedToMetadataKey] == target) {
		return nil
	}
	writer, _ := beads.GuardedUpdateWriterFor(deps.Store)
	conditions, err := selectedSourceConditions(b)
	if err != nil {
		return err
	}
	if conditions.Metadata[DispatchEffectStateKey] != "committed" || conditions.Metadata[DispatchEffectIDKey] != effectID ||
		(graph && (conditions.Metadata["workflow_id"] != candidate.RootID || conditions.Metadata[beadmeta.ExecutionRoutedToMetadataKey] != target) ||
			!graph && (conditions.Metadata[beadmeta.MoleculeIDMetadataKey] != candidate.RootID || conditions.Metadata[beadmeta.RoutedToMetadataKey] != target)) {
		return &RouteConflictError{sourceID, "original activation receipt does not select this provider"}
	}
	applied, err := writer.UpdateGuarded(sourceID, beads.UpdateOpts{Metadata: map[string]string{DispatchEffectStateKey: "routed"}}, conditions)
	if err != nil {
		return err
	}
	if !applied {
		return &RouteConflictError{sourceID, "selected effect receipt changed during activation"}
	}
	return nil
}

func resumeCommittedFormula(opts SlingOpts, deps SlingDeps, source beads.Bead) (SlingResult, error) {
	result := SlingResult{BeadID: source.ID, Target: opts.Target.QualifiedName(), Method: "on-formula", FormulaName: opts.OnFormula, WorkflowID: source.Metadata["workflow_id"]}
	if source.Metadata[DispatchEffectStateKey] != "routed" {
		if _, err := selectedSourceConditions(source); err != nil {
			return result, err
		}
	}
	target := agentutil.NormalizePoolRouteTarget(deps.Cfg, agentutil.RoutedToIdentity(&opts.Target))
	rootID := result.WorkflowID
	if rootID == "" {
		rootID = source.Metadata[beadmeta.MoleculeIDMetadataKey]
		result.WispRootID = rootID
	}
	if rootID == "" && source.Metadata["gc.dispatch_source_bead"] == source.ID && source.Metadata[dispatchCandidateIDsKey] != "" {
		rootID, result.WispRootID = source.ID, source.ID
	}
	if rootID == "" && IsCustomSlingQuery(opts.Target) {
		if opts.Conditions == nil {
			return result, &RouteConflictError{source.ID, "original effect conditions are absent"}
		}
		id := opts.Conditions.Metadata[DispatchEffectIDKey]
		if err := deliverSelectedCustomFormula(opts, deps, source.ID, nil, nil, false, id, target); err != nil {
			return result, err
		}
		result.Method, result.Idempotent = "bead", true
		return result, nil
	}
	root, err := beads.HandlesFor(deps.graphStore()).Live.Get(rootID)
	if err != nil {
		return result, err
	}
	requestedFormula := opts.OnFormula
	if opts.IsFormula {
		requestedFormula = opts.BeadOrFormula
		result.Method = "formula"
	}
	if requestedFormula == "" {
		requestedFormula = opts.Target.EffectiveDefaultSlingFormula()
	}
	if selectedFormula := root.Metadata[beadmeta.FormulaNameMetadataKey]; requestedFormula != "" && selectedFormula != "" && requestedFormula != selectedFormula {
		return result, &RouteConflictError{source.ID, "requested formula differs from the original selected provider"}
	}
	if result.FormulaName == "" {
		result.FormulaName = root.Metadata[beadmeta.FormulaNameMetadataKey]
	}
	if opts.Conditions == nil {
		return result, &RouteConflictError{source.ID, "original effect conditions are absent"}
	}
	id := opts.Conditions.Metadata[DispatchEffectIDKey]
	if root.Metadata[DispatchEffectIDKey] != id || root.Metadata["gc.dispatch_source_bead"] != source.ID {
		return result, &RouteConflictError{source.ID, "selected provider candidate does not match original effect"}
	}
	var mapping map[string]string
	if err := json.Unmarshal([]byte(root.Metadata[dispatchCandidateIDsKey]), &mapping); err != nil || len(mapping) == 0 {
		return result, fmt.Errorf("selected provider candidate identity is unreadable")
	}
	pending := root.Metadata[beadmeta.AttachFencePendingMetadataKey] != ""
	if pending {
		if source.Metadata["handoff.conflict_state"] == "hold" {
			return result, &RouteConflictError{source.ID, "source hold is active"}
		}
		if source.Status != source.Metadata["gc.dispatch_activation_status"] || source.Assignee != source.Metadata["gc.dispatch_activation_assignee"] {
			return result, &RouteConflictError{source.ID, "selected source owner changed before activation recovery"}
		}
		if _, err := routeConditions(source, opts.Conditions); err != nil {
			return result, err
		}
		if source.Metadata[DispatchEffectStateKey] == "routed" {
			if _, err := selectedSourceConditions(source); err != nil {
				return result, err
			}
		}
		if opts.Conditions.Status == nil || opts.Conditions.Assignee == nil {
			return result, &RouteConflictError{source.ID, "exact source ownership conditions are absent"}
		}
	}
	graph := IsWorkflowAttachment(root)
	candidate := &molecule.Result{RootID: root.ID, IDMapping: mapping, GraphWorkflow: graph}
	if IsCustomSlingQuery(opts.Target) {
		if err := deliverSelectedCustomFormula(opts, deps, source.ID, deps.graphStore(), candidate, graph, id, target); err != nil {
			return result, err
		}
	} else if err := finishCommittedFormula(deps, source.ID, deps.graphStore(), candidate, graph, opts.IsFormula, id, target, "committed"); err != nil {
		return result, err
	}
	if pending && deps.Notify != nil {
		deps.Notify.PokeController(deps.CityPath)
		deps.Notify.PokeControlDispatch(deps.CityPath)
	}
	result.Idempotent = true
	return result, nil
}

func customDeliveryCommand(opts SlingOpts, deps SlingDeps, sourceID string) (string, string, string, error) {
	command, warning := BuildSlingCommandForAgent("sling_query", opts.Target.EffectiveSlingQuery(), sourceID, deps.CityPath, deps.CityName, opts.Target, deps.Cfg.Rigs)
	if warning != "" {
		return "", "", "", fmt.Errorf("custom delivery identity is unreadable: %s", warning)
	}
	dir := SlingDirForBead(deps.Cfg, deps.CityPath, sourceID)
	digest := sha256.Sum256([]byte(command + "\x00" + dir + "\x00" + opts.Target.QualifiedName()))
	return command, dir, fmt.Sprintf("%x", digest), nil
}

func resumeUnresolvedCustomFormula(opts SlingOpts, deps SlingDeps) (SlingResult, bool, error) {
	roots, err := deps.graphStore().ListByMetadata(map[string]string{beadmeta.FormulaNameMetadataKey: opts.BeadOrFormula}, 0, beads.WithBothTiers, beads.IncludeClosed)
	if err != nil {
		return SlingResult{}, false, err
	}
	target := agentutil.NormalizePoolRouteTarget(deps.Cfg, agentutil.RoutedToIdentity(&opts.Target))
	var selected *beads.Bead
	for i := range roots {
		root := &roots[i]
		selectedTarget := root.Metadata[beadmeta.ExecutionRoutedToMetadataKey]
		if selectedTarget == "" {
			selectedTarget = root.Metadata[beadmeta.RoutedToMetadataKey]
		}
		if root.Metadata["gc.dispatch_source_bead"] != root.ID || root.Metadata[customDispatchProviderKey] == "" ||
			root.Metadata[DispatchEffectIDKey] == "" || selectedTarget != target {
			continue
		}
		state := root.Metadata[DispatchEffectStateKey]
		if state != "committed" && state != "attempted" && state != "unknown" &&
			!(state == "routed" && root.Metadata[beadmeta.AttachFencePendingMetadataKey] != "") {
			continue
		}
		if selected != nil {
			return SlingResult{}, true, &RouteConflictError{root.ID, "multiple unresolved original custom providers require explicit reconciliation"}
		}
		selected = root
	}
	if selected == nil {
		return SlingResult{}, false, nil
	}
	conditions, err := routeConditions(*selected, nil)
	if err != nil {
		return SlingResult{}, true, err
	}
	opts.Conditions = &conditions
	deps.Store = deps.graphStore()
	result, err := resumeCommittedFormula(opts, deps, *selected)
	return result, true, err
}

func commitCustomPlainRoute(opts SlingOpts, deps SlingDeps, sourceID string) error {
	if opts.Conditions == nil {
		return beads.ErrConditionalWriteUnsupported
	}
	_, _, provider, err := customDeliveryCommand(opts, deps, sourceID)
	if err != nil {
		return err
	}
	effectID := opts.Conditions.Metadata[DispatchEffectIDKey]
	if effectID == "" {
		effectID = "gc-custom-" + rand.Text()
	} else if opts.Conditions.Metadata[DispatchEffectStateKey] != "attempted" || opts.Conditions.Metadata[customDispatchProviderKey] != "" {
		return &RouteConflictError{sourceID, "an existing custom effect requires its original receipt, not a new delivery"}
	}
	target := agentutil.NormalizePoolRouteTarget(deps.Cfg, agentutil.RoutedToIdentity(&opts.Target))
	update := beads.UpdateOpts{Metadata: map[string]string{
		beadmeta.RoutedToMetadataKey: target, DispatchEffectIDKey: effectID,
		DispatchEffectStateKey: "committed", customDispatchProviderKey: provider,
	}}
	if opts.Merge != "" {
		update.Metadata[beadmeta.MergeStrategyMetadataKey] = opts.Merge
	}
	if opts.Reassign {
		empty := ""
		update.Assignee = &empty
		if opts.Conditions.Status != nil && *opts.Conditions.Status == "in_progress" {
			open := "open"
			update.Status = &open
		}
	}
	if err := freezeActivationConditions(&update, *opts.Conditions); err != nil {
		return err
	}
	if err := commitSelectedSource(context.Background(), deps.Store, sourceID, update, *opts.Conditions, nil, deps, target, true); err != nil {
		return err
	}
	return deliverSelectedCustomFormula(opts, deps, sourceID, nil, nil, false, effectID, target)
}

// The attempt is durable before the external command starts. Only its actual
// acknowledgment permits activation; ambiguous effects have no replay path.
func deliverSelectedCustomFormula(opts SlingOpts, deps SlingDeps, sourceID string, store beads.Store, candidate *molecule.Result, graph bool, effectID, target string) error {
	command, dir, provider, err := customDeliveryCommand(opts, deps, sourceID)
	if err != nil {
		return err
	}
	source, err := beads.HandlesFor(deps.Store).Live.Get(sourceID)
	if err != nil {
		return err
	}
	if source.Metadata[DispatchEffectIDKey] != effectID || source.Metadata[customDispatchProviderKey] != provider {
		return &RouteConflictError{sourceID, "custom provider differs from the original selected effect"}
	}
	switch source.Metadata[DispatchEffectStateKey] {
	case "attempted", "unknown":
		return &RouteConflictError{sourceID, "custom delivery " + effectID + " is unresolved; the original provider has no authoritative acknowledgment to reconcile"}
	case "committed":
		original, err := selectedSourceConditions(source)
		if err != nil {
			return err
		}
		attempt := beads.UpdateOpts{Metadata: map[string]string{DispatchEffectStateKey: "attempted"}}
		if err := freezeActivationConditions(&attempt, original); err != nil {
			return err
		}
		if err := commitSelectedSource(context.Background(), deps.Store, sourceID, attempt, original, nil, deps, target, true); err != nil {
			return err
		}
		original.Metadata = maps.Clone(original.Metadata)
		original.Metadata[DispatchEffectStateKey] = "attempted"
		original.Metadata[dispatchActivationConditionsKey] = attempt.Metadata[dispatchActivationConditionsKey]
		env := ResolveSlingEnv(opts.Target, deps, sourceID)
		if env == nil {
			env = map[string]string{}
		}
		env["GC_SLING_EFFECT_ID"] = effectID
		_, deliveryErr := deps.Runner(dir, command, env)
		state := "routed"
		if deliveryErr != nil {
			state = "unknown"
		}
		receipt := beads.UpdateOpts{Metadata: map[string]string{DispatchEffectStateKey: state}}
		if err := freezeActivationConditions(&receipt, original); err != nil {
			return errors.Join(deliveryErr, err)
		}
		receiptErr := commitSelectedSource(context.Background(), deps.Store, sourceID, receipt, original, nil, deps, target, true)
		if deliveryErr != nil || receiptErr != nil {
			return errors.Join(&RouteConflictError{sourceID, "custom delivery " + effectID + " remains unresolved; do not redeliver without an authoritative provider receipt"}, deliveryErr, receiptErr)
		}
	case "routed":
	default:
		return &RouteConflictError{sourceID, "custom delivery receipt has an unsupported state"}
	}
	if candidate == nil {
		return nil
	}
	return finishCommittedFormula(deps, sourceID, store, candidate, graph, opts.IsFormula, effectID, target, "routed")
}

func reconciledFormulaRoute(opts SlingOpts, deps SlingDeps, source *beads.Bead) (SlingResult, bool, error) {
	if source == nil || source.Metadata[DispatchEffectStateKey] != "routed" || opts.Force && source.Metadata[DispatchEffectIDKey] == "" {
		return SlingResult{}, false, nil
	}
	target := agentutil.NormalizePoolRouteTarget(deps.Cfg, agentutil.RoutedToIdentity(&opts.Target))
	rootID := source.Metadata["workflow_id"]
	if rootID == "" {
		rootID = source.Metadata[beadmeta.MoleculeIDMetadataKey]
	}
	if rootID == "" {
		return SlingResult{}, false, nil
	}
	if source.Metadata[beadmeta.RoutedToMetadataKey] != target && source.Metadata[beadmeta.ExecutionRoutedToMetadataKey] != target {
		return SlingResult{}, false, nil
	}
	root, err := beads.HandlesFor(deps.graphStore()).Live.Get(rootID)
	if err != nil {
		return SlingResult{}, true, err
	}
	requestedFormula := opts.OnFormula
	if requestedFormula == "" {
		requestedFormula = opts.Target.EffectiveDefaultSlingFormula()
	}
	if selectedFormula := root.Metadata[beadmeta.FormulaNameMetadataKey]; requestedFormula != "" && selectedFormula != "" && requestedFormula != selectedFormula {
		return SlingResult{}, true, &RouteConflictError{source.ID, "requested formula differs from the original selected provider"}
	}
	if root.Metadata[beadmeta.AttachFencePendingMetadataKey] != "" || root.Metadata[DispatchEffectIDKey] != source.Metadata[DispatchEffectIDKey] || root.Metadata["gc.dispatch_source_bead"] != source.ID {
		return SlingResult{}, true, &RouteConflictError{source.ID, "original provider activation cannot be reconciled"}
	}
	return SlingResult{BeadID: source.ID, Target: opts.Target.QualifiedName(), Method: "on-formula", FormulaName: opts.OnFormula, WorkflowID: source.Metadata["workflow_id"], WispRootID: source.Metadata[beadmeta.MoleculeIDMetadataKey], Idempotent: true}, true, nil
}

func guardedReplacementRows(store beads.Store, candidate beads.Bead) ([]beads.Bead, error) {
	raw := candidate.Metadata[dispatchReplacedRootKey]
	if raw == "" {
		return nil, nil
	}
	var roots []string
	if err := json.Unmarshal([]byte(raw), &roots); err != nil {
		return nil, fmt.Errorf("selected predecessor receipt: %w", err)
	}
	var rows []beads.Bead
	seen := map[string]bool{}
	for _, rootID := range roots {
		if rootID == candidate.ID {
			return nil, &RouteConflictError{candidate.ID, "replacement selects its own root"}
		}
		matched, err := sourceworkflow.ListWorkflowBeads(store, rootID)
		if err != nil {
			return nil, err
		}
		for _, item := range matched {
			if seen[item.ID] {
				continue
			}
			seen[item.ID] = true
			b, err := beads.HandlesFor(store).Live.Get(item.ID)
			if err != nil {
				return nil, err
			}
			if b.Metadata["handoff.conflict_state"] == "hold" {
				return nil, &RouteConflictError{b.ID, "predecessor workflow is held"}
			}
			if b.Status != "closed" {
				rows = append(rows, b)
			}
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows, nil
}

func closeGuardedReplacement(writer beads.GuardedUpdateWriter, rows []beads.Bead) error {
	closed := "closed"
	for _, b := range rows {
		applied, err := writer.UpdateGuarded(b.ID, beads.UpdateOpts{Status: &closed, Metadata: map[string]string{beadmeta.OutcomeMetadataKey: beadmeta.OutcomeSkipped, "close_reason": sourceworkflow.WorkflowSubtreeClosedReason}}, activationConditions(b))
		if err != nil {
			return err
		}
		if !applied {
			return &RouteConflictError{b.ID, "predecessor ownership or hold changed before replacement"}
		}
	}
	return nil
}

func validateStagedProviderIntent(root beads.Bead, sourceID, formulaName, target string) error {
	if root.Metadata[beadmeta.FormulaNameMetadataKey] != formulaName {
		return &RouteConflictError{sourceID, "requested formula differs from the original staged provider"}
	}
	if root.Metadata[dispatchTargetKey] != target {
		return &RouteConflictError{sourceID, "requested target differs from the original staged provider"}
	}
	return nil
}

func originalDispatchCandidate(store beads.Store, effectID, sourceID, formulaName, target string) (*molecule.Result, error) {
	if effectID == "" {
		return nil, nil
	}
	roots, err := beads.HandlesFor(store).Live.List(beads.ListQuery{Metadata: map[string]string{DispatchEffectIDKey: effectID, "gc.dispatch_source_bead": sourceID}, IncludeClosed: true, TierMode: beads.TierBoth})
	if err != nil {
		return nil, err
	}
	if len(roots) == 0 {
		return nil, nil
	}
	if len(roots) != 1 {
		return nil, &RouteConflictError{sourceID, "original effect has multiple provider candidates; reconcile its selected receipt"}
	}
	root := roots[0]
	if root.Metadata[beadmeta.AttachFencePendingMetadataKey] != "true" {
		return nil, &RouteConflictError{sourceID, "original provider effect requires reconciliation, not another materialization"}
	}
	if err := validateStagedProviderIntent(root, sourceID, formulaName, target); err != nil {
		return nil, err
	}
	var mapping map[string]string
	if err := json.Unmarshal([]byte(root.Metadata[dispatchCandidateIDsKey]), &mapping); err != nil || len(mapping) == 0 {
		return nil, fmt.Errorf("original pending provider candidate %s has no complete materialization receipt", root.ID)
	}
	return &molecule.Result{RootID: root.ID, IDMapping: mapping, GraphWorkflow: IsWorkflowAttachment(root)}, nil
}

func guardedSourcePredecessors(deps SlingDeps, source beads.Bead, allowGraph bool) ([]beads.Bead, []string, error) {
	store := deps.Store
	sourceWriter, _ := beads.GuardedUpdateWriterFor(store)
	providerWriter, _ := beads.GuardedUpdateWriterFor(deps.graphStore())
	sharedOwner := sourceWriter == providerWriter
	local := source
	local.Metadata = maps.Clone(source.Metadata)
	var providerRoots []string
	for _, key := range []string{beadmeta.MoleculeIDMetadataKey, "workflow_id"} {
		rootID := source.Metadata[key]
		if rootID == "" {
			continue
		}
		if sharedOwner {
			continue
		}
		root, err := beads.HandlesFor(deps.graphStore()).Live.Get(rootID)
		if err != nil {
			return nil, nil, err
		}
		if root.Status != "closed" {
			if IsWorkflowAttachment(root) {
				if !allowGraph || root.Metadata[beadmeta.FormulaContractMetadataKey] != beadmeta.FormulaContractGraphV2 {
					return nil, nil, &sourceworkflow.ConflictError{SourceBeadID: source.ID, WorkflowIDs: []string{root.ID}}
				}
			} else if source.Assignee != "" {
				return nil, nil, fmt.Errorf("bead %s already has attached %s %s", source.ID, AttachmentLabel(root), root.ID)
			}
			if root.Metadata["handoff.conflict_state"] == "hold" {
				return nil, nil, &RouteConflictError{root.ID, "predecessor attachment is held"}
			}
			providerRoots = append(providerRoots, root.ID)
		}
		delete(local.Metadata, key)
	}
	attachments, err := CollectAttachedBeads(local, store, store)
	if err != nil {
		return nil, nil, err
	}
	var rows []beads.Bead
	seen := map[string]bool{}
	for _, root := range attachments {
		if root.Status == "closed" {
			continue
		}
		if IsWorkflowAttachment(root) {
			if !allowGraph || root.Metadata[beadmeta.FormulaContractMetadataKey] != beadmeta.FormulaContractGraphV2 {
				return nil, nil, &sourceworkflow.ConflictError{SourceBeadID: source.ID, WorkflowIDs: []string{root.ID}}
			}
		} else if source.Assignee != "" {
			return nil, nil, fmt.Errorf("bead %s already has attached %s %s", source.ID, AttachmentLabel(root), root.ID)
		}
		matched, err := sourceworkflow.ListWorkflowBeads(store, root.ID)
		if err != nil {
			return nil, nil, err
		}
		for _, item := range matched {
			if seen[item.ID] {
				continue
			}
			seen[item.ID] = true
			b, err := beads.HandlesFor(store).Live.Get(item.ID)
			if err != nil {
				return nil, nil, err
			}
			if b.Metadata["handoff.conflict_state"] == "hold" {
				return nil, nil, &RouteConflictError{b.ID, "predecessor attachment is held"}
			}
			if b.Status != "closed" {
				rows = append(rows, b)
			}
		}
	}
	return rows, providerRoots, nil
}

func addReplacementRoot(recipe *formula.Recipe, rootID string) error {
	if recipe.Steps[0].Metadata == nil {
		recipe.Steps[0].Metadata = map[string]string{}
	}
	var roots []string
	if raw := recipe.Steps[0].Metadata[dispatchReplacedRootKey]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &roots); err != nil {
			return err
		}
	}
	for _, existing := range roots {
		if existing == rootID {
			return nil
		}
	}
	roots = append(roots, rootID)
	raw, err := json.Marshal(roots)
	if err != nil {
		return err
	}
	recipe.Steps[0].Metadata[dispatchReplacedRootKey] = string(raw)
	return nil
}

func commitSelectedSource(ctx context.Context, store beads.Store, sourceID string, update beads.UpdateOpts, conditions beads.UpdateConditions, predecessors []beads.Bead, deps SlingDeps, target string, required bool) error {
	atomic, ok := beads.SingleTransactionStoreFor(store)
	if !ok {
		return beads.ErrConditionalWriteUnsupported
	}
	return atomic.TxSingle("gc: select guarded sling candidate", func(tx beads.Tx) error {
		writer, ok := tx.(beads.GuardedUpdateWriter)
		if !ok {
			return beads.ErrConditionalWriteUnsupported
		}
		if err := checkNativeAdmission(ctx, deps.Cfg, deps.CityPath, target, required); err != nil {
			return err
		}
		applied, err := writer.UpdateGuarded(sourceID, update, conditions)
		if err != nil {
			return err
		}
		if !applied {
			return &RouteConflictError{sourceID, "source ownership or hold changed before candidate selection"}
		}
		return closeGuardedReplacement(writer, predecessors)
	})
}

func encodeRouteConditions(conditions beads.UpdateConditions) (string, error) {
	if conditions.Labels != nil && *conditions.Labels == nil {
		empty := []string{}
		conditions.Labels = &empty
	}
	raw, err := json.Marshal(conditions)
	return string(raw), err
}

func freezeActivationConditions(update *beads.UpdateOpts, conditions beads.UpdateConditions) error {
	conditions.Metadata = maps.Clone(conditions.Metadata)
	if conditions.Metadata == nil {
		conditions.Metadata = map[string]string{}
	}
	maps.Copy(conditions.Metadata, update.Metadata)
	delete(conditions.Metadata, dispatchActivationConditionsKey)
	if update.Status != nil {
		conditions.Status = update.Status
	}
	if update.Assignee != nil {
		conditions.Assignee = update.Assignee
	}
	raw, err := encodeRouteConditions(conditions)
	if err != nil {
		return err
	}
	update.Metadata[dispatchActivationConditionsKey] = raw
	return nil
}

func selectedSourceConditions(source beads.Bead) (beads.UpdateConditions, error) {
	var conditions beads.UpdateConditions
	if err := json.Unmarshal([]byte(source.Metadata[dispatchActivationConditionsKey]), &conditions); err != nil {
		return conditions, fmt.Errorf("original selected source conditions: %w", err)
	}
	if conditions.Status == nil || conditions.Assignee == nil || conditions.Title == nil || conditions.Description == nil || conditions.AcceptanceCriteria == nil || conditions.Labels == nil {
		return conditions, fmt.Errorf("original selected source conditions are incomplete")
	}
	if _, err := routeConditions(source, &conditions); err != nil {
		return conditions, err
	}
	conditions.Metadata[dispatchActivationConditionsKey] = source.Metadata[dispatchActivationConditionsKey]
	return conditions, nil
}

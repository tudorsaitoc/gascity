package main

import (
	"log"
	"path"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/agent"
	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/session"
)

// sessionBeadAssigneeIdentities returns every identifier under which work may
// be assigned to a live session. Pool workers claim through stable aliases,
// while their runtime session names are transient, so both current and prior
// aliases must participate in ownership and demand checks.
func sessionBeadAssigneeIdentities(sb beads.Bead) []string {
	identities := make([]string, 0, 5)
	for _, value := range []string{
		sb.ID,
		sb.Metadata["session_name"],
		sb.Metadata["configured_named_identity"],
		sb.Metadata["alias"],
	} {
		if value = strings.TrimSpace(value); value != "" {
			identities = append(identities, value)
		}
	}
	for _, prior := range session.AliasHistory(sb.Metadata) {
		if prior = strings.TrimSpace(prior); prior != "" {
			identities = append(identities, prior)
		}
	}
	return identities
}

type releasedPoolAssignment struct {
	ID    string
	Index int
}

// PoolSessionName derives the tmux session name for a pool worker session.
// Format: {basename(template)}-{beadID} (e.g., "claude-mc-xyz").
// Named sessions with an alias use the alias instead.
func PoolSessionName(template, beadID string) string {
	base := path.Base(template)
	return agent.SanitizeQualifiedNameForSession(base) + "-" + beadID
}

// GCSweepSessionBeads closes open session beads that have no remaining
// open/in-progress work beads anywhere — primary store OR any attached
// rig store. Work-bead assignment is verified by a live cross-store
// query inside closeSessionBeadIfUnassigned, so the caller does not
// pass a work snapshot — that pattern was retired to prevent pre-close
// tick snapshots from poisoning close decisions. Returns the IDs of
// session beads that were closed.
func GCSweepSessionBeads(store beads.Store, rigStores map[string]beads.Store, sessionBeads []beads.Bead) []string {
	var closed []string
	for _, sb := range sessionBeads {
		if sb.Status == "closed" {
			continue
		}
		if !closeSessionBeadIfUnassigned(store, rigStores, sb, "gc_swept", time.Now().UTC(), nil) {
			continue
		}
		closed = append(closed, sb.ID)
	}
	return closed
}

// releaseOrphanedPoolAssignmentsWhenSnapshotsComplete skips orphan release
// unless both the assigned-work and open-session snapshots are complete.
func releaseOrphanedPoolAssignmentsWhenSnapshotsComplete(
	store beads.Store,
	cfg *config.City,
	cityPath string,
	openSessionBeads []beads.Bead,
	result DesiredStateResult,
	rigStores map[string]beads.Store,
) []releasedPoolAssignment {
	// Partial input snapshots can make active work look orphaned for this
	// tick only: missing work affects drain decisions, and missing sessions
	// affects assigned-work orphan release.
	if result.snapshotQueryPartial() {
		return nil
	}
	return releaseOrphanedPoolAssignments(store, cfg, cityPath, openSessionBeads, result.AssignedWorkBeads, result.AssignedWorkStores, result.AssignedWorkStoreRefs, rigStores)
}

// releaseOrphanedPoolAssignments reopens active pool-routed work whose
// assignee no longer maps to any open session bead. This also recovers
// pool-routed work left in_progress with no assignee, which cannot be claimed
// again until it is moved back to open.
func releaseOrphanedPoolAssignments(
	store beads.Store,
	cfg *config.City,
	cityPath string,
	openSessionBeads []beads.Bead,
	assignedWorkBeads []beads.Bead,
	assignedWorkStores []beads.Store,
	assignedWorkStoreRefs []string,
	rigStores map[string]beads.Store,
) []releasedPoolAssignment {
	if store == nil || cfg == nil || len(assignedWorkBeads) == 0 {
		return nil
	}
	storeAware := len(assignedWorkStores) > 0
	if storeAware && len(assignedWorkStores) != len(assignedWorkBeads) {
		log.Printf("releaseOrphanedPoolAssignments: assigned work/store length mismatch: work=%d stores=%d", len(assignedWorkBeads), len(assignedWorkStores))
	}
	storeRefAware := len(assignedWorkStoreRefs) == len(assignedWorkBeads)
	if len(assignedWorkStoreRefs) > 0 && !storeRefAware {
		log.Printf("releaseOrphanedPoolAssignments: assigned work/store-ref length mismatch: work=%d storeRefs=%d", len(assignedWorkBeads), len(assignedWorkStoreRefs))
	}

	openIdentifiers := makeOpenSessionStoreRefIndex(cityPath, cfg, openSessionBeads, storeRefAware)
	legacyOpenIdentifiers := make(map[string]struct{}, len(openSessionBeads)*5)
	for _, sb := range openSessionBeads {
		if sb.Status == "closed" {
			continue
		}
		for _, id := range sessionBeadAssigneeIdentities(sb) {
			legacyOpenIdentifiers[id] = struct{}{}
		}
	}

	var released []releasedPoolAssignment
	for i, wb := range assignedWorkBeads {
		if wb.Status != "open" && wb.Status != "in_progress" {
			continue
		}
		assignee := strings.TrimSpace(wb.Assignee)
		template := strings.TrimSpace(wb.Metadata["gc.routed_to"])
		if template == "" {
			continue
		}
		agentCfg := findAgentByTemplate(cfg, template)
		if agentCfg == nil || !agentCfg.SupportsGenericEphemeralSessions() {
			continue
		}
		if assignee == "" {
			if wb.Status != "in_progress" {
				continue
			}
		} else {
			workStoreRef := ""
			if storeRefAware {
				workStoreRef = assignedWorkStoreRefs[i]
			}
			if openSessionOwnsWork(legacyOpenIdentifiers, openIdentifiers, assignee, workStoreRef, storeRefAware) {
				continue
			}
			if assigneePreservesNamedSessionRoute(cfg, cityPath, template, assignee, workStoreRef, storeRefAware) {
				continue
			}
		}

		var ownerStore beads.Store
		if storeAware {
			if i >= len(assignedWorkStores) || assignedWorkStores[i] == nil {
				log.Printf("releaseOrphanedPoolAssignments: missing owner store for assigned work %q at index %d", wb.ID, i)
				continue
			}
			ownerStore = assignedWorkStores[i]
		} else {
			ownerStore = storeForPoolAssignment(cfg, store, rigStores, wb)
			if ownerStore == nil {
				continue
			}
		}
		if !releaseOrphanedPoolAssignment(ownerStore, wb.ID) {
			continue
		}
		released = append(released, releasedPoolAssignment{ID: wb.ID, Index: i})
	}
	return released
}

const unresolvedOpenSessionStoreRef = "\x00unresolved"

func makeOpenSessionStoreRefIndex(cityPath string, cfg *config.City, openSessionBeads []beads.Bead, storeRefAware bool) map[string]map[string]struct{} {
	index := make(map[string]map[string]struct{}, len(openSessionBeads)*5)
	if !storeRefAware {
		return index
	}
	for _, sb := range openSessionBeads {
		if sb.Status == "closed" {
			continue
		}
		storeRef, ok := assignedWorkStoreRefForSession(cityPath, cfg, sb)
		if !ok {
			storeRef = unresolvedOpenSessionStoreRef
		}
		for _, id := range sessionBeadAssigneeIdentities(sb) {
			addOpenSessionStoreRef(index, id, storeRef)
		}
	}
	return index
}

func addOpenSessionStoreRef(index map[string]map[string]struct{}, identifier, storeRef string) {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return
	}
	refs := index[identifier]
	if refs == nil {
		refs = make(map[string]struct{}, 1)
		index[identifier] = refs
	}
	refs[storeRef] = struct{}{}
}

func openSessionOwnsWork(legacyIdentifiers map[string]struct{}, scopedIdentifiers map[string]map[string]struct{}, assignee, workStoreRef string, storeRefAware bool) bool {
	if !storeRefAware {
		_, ok := legacyIdentifiers[assignee]
		return ok
	}
	refs := scopedIdentifiers[assignee]
	if refs == nil {
		return false
	}
	if _, ok := refs[unresolvedOpenSessionStoreRef]; ok {
		return true
	}
	_, ok := refs[workStoreRef]
	return ok
}

func storeForPoolAssignment(cfg *config.City, cityStore beads.Store, rigStores map[string]beads.Store, wb beads.Bead) beads.Store {
	if cfg == nil || len(rigStores) == 0 {
		return cityStore
	}
	if routed := strings.TrimSpace(wb.Metadata["gc.routed_to"]); routed != "" {
		if slash := strings.IndexByte(routed, '/'); slash > 0 {
			if store := rigStores[routed[:slash]]; store != nil {
				return store
			}
		}
	}
	idPrefix := beadIDPrefix(wb.ID)
	for _, rig := range cfg.Rigs {
		if idPrefix == rig.EffectivePrefix() {
			if store := rigStores[rig.Name]; store != nil {
				return store
			}
		}
	}
	return cityStore
}

func isRecoverableUnassignedInProgressPoolWork(cfg *config.City, wb beads.Bead) bool {
	if wb.Status != "in_progress" || strings.TrimSpace(wb.Assignee) != "" {
		return false
	}
	template := strings.TrimSpace(wb.Metadata["gc.routed_to"])
	if template == "" {
		return false
	}
	agentCfg := findAgentByTemplate(cfg, template)
	return agentCfg != nil && agentCfg.SupportsGenericEphemeralSessions()
}

func beadIDPrefix(id string) string {
	trimmed := strings.TrimSpace(id)
	if dash := strings.IndexByte(trimmed, '-'); dash > 0 {
		return trimmed[:dash]
	}
	return ""
}

func releaseOrphanedPoolAssignment(store beads.Store, id string) bool {
	if store == nil || id == "" {
		return false
	}
	opts := beads.UpdateOpts{
		Assignee: stringPtr(""),
		Status:   stringPtr("open"),
	}
	return store.Update(id, opts) == nil
}

func assigneePreservesNamedSessionRoute(cfg *config.City, cityPath, template, assignee, workStoreRef string, storeRefAware bool) bool {
	if cfg == nil {
		return false
	}
	spec, ok := findNamedSessionSpec(cfg, cfg.EffectiveCityName(), assignee)
	if !ok {
		return false
	}
	if namedSessionBackingTemplate(spec) != template {
		return false
	}
	if !storeRefAware {
		return true
	}
	return assignedWorkStoreRefForAgent(cityPath, cfg, spec.Agent) == workStoreRef
}

func stringPtr(s string) *string { return &s }

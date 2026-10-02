package beads

// UpdateConditions fences an update on current field values, not a revision.
// Empty metadata values match absent keys, as in MetadataCASWriter.
type UpdateConditions struct {
	Actor               string
	Status              *string
	Assignee            *string
	Title               *string
	Description         *string
	AcceptanceCriteria  *string
	Labels              *[]string // nil skips the predicate; empty requires no labels, ignoring order.
	Metadata            map[string]string
	SetMetadataIfAbsent map[string]string
	UnsetMetadata       []string
}

func (c UpdateConditions) Requested() bool {
	return c.Actor != "" || c.Status != nil || c.Assignee != nil || c.Title != nil || c.Description != nil || c.AcceptanceCriteria != nil || c.Labels != nil || len(c.Metadata) != 0 || len(c.SetMetadataIfAbsent) != 0 || len(c.UnsetMetadata) != 0
}

// GuardedUpdateWriter checks all conditions and applies every change in one
// backend transaction. A mismatch returns false, nil and writes nothing.
// It does not require or advertise the unsound bd 1.1 revision-CAS capability.
type GuardedUpdateWriter interface {
	UpdateGuarded(string, UpdateOpts, UpdateConditions) (bool, error)
}

// GuardedUpdateWriterFor follows only explicitly declared wrapper targets.
func GuardedUpdateWriterFor(store Store) (GuardedUpdateWriter, bool) {
	if store == nil {
		return nil, false
	}
	target := followConditionalWritesResolveTarget(store)
	switch current := target.(type) {
	case *FileStore:
		return nil, false
	case *MemStore:
		current.mu.Lock()
		disabled := current.DisableConditionalWrites
		current.mu.Unlock()
		if disabled {
			return nil, false
		}
	}
	writer, ok := target.(GuardedUpdateWriter)
	return writer, ok
}

const RefineryDecisionAtKey = "refinery_decision_at"

// mergeUpdateMetadata preserves the first nonempty terminal clock, even when
// an ordinary later update tries to replace or clear it.
func mergeUpdateMetadata(current, changes map[string]string) {
	for key, value := range changes {
		if key == RefineryDecisionAtKey && current[key] != "" {
			continue
		}
		current[key] = value
	}
}

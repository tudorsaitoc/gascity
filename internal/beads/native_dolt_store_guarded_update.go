package beads

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	beadslib "github.com/steveyegge/beads"
)

var (
	_ GuardedUpdateWriter = (*NativeDoltStore)(nil)
	_ GuardedUpdateWriter = (*nativeDoltTx)(nil)
)

// Optional pinned-library capability; never emulate history after commit.
type nativeTransactionEventWriter interface {
	UpdateIssueWithEvents(context.Context, string, map[string]interface{}, string) error
}

// UpdateGuarded checks and applies the mutation in one backend transaction.
func (s *NativeDoltStore) UpdateGuarded(id string, opts UpdateOpts, conditions UpdateConditions) (bool, error) {
	storage, release, err := s.acquireStorage()
	if err != nil {
		return false, err
	}
	defer release()
	runner, ok := storage.(nativeSingleTransactionRunner)
	if !ok {
		return false, fmt.Errorf("native backend has no single-session guarded mutation capability: %w", ErrConditionalWriteUnsupported)
	}
	for attempt := 1; attempt <= nativeMetadataWriteAttempts; attempt++ {
		ctx, cancel := nativeDoltOperationContext(context.TODO())
		applied := false
		err = runner.RunInSingleTransaction(ctx, fmt.Sprintf("gc: guarded update bead %s", id), func(tx beadslib.Transaction) error {
			var err error
			applied, err = s.applyGuardedUpdateInTx(ctx, tx, id, opts, conditions)
			return err
		})
		cancel()
		if err == nil {
			return applied, nil
		}
		// Only a known rolled-back serialization conflict can retry. An
		// ambiguous transport failure may have committed and must stay visible.
		if !isNativeDoltSerializationConflict(err) || attempt == nativeMetadataWriteAttempts {
			return false, err
		}
		time.Sleep(time.Duration(attempt) * nativeMetadataWriteRetryBackoff)
	}
	return false, err
}

// UpdateGuarded participates in the caller's existing native transaction.
// A caller composing several mutations must abort its callback on any false
// verdict so earlier changes in that transaction roll back as well.
func (t *nativeDoltTx) UpdateGuarded(id string, opts UpdateOpts, conditions UpdateConditions) (bool, error) {
	return t.store.applyGuardedUpdateInTx(t.ctx, t.tx, id, opts, conditions)
}

func (s *NativeDoltStore) applyGuardedUpdateInTx(ctx context.Context, tx beadslib.Transaction, id string, opts UpdateOpts, conditions UpdateConditions) (bool, error) {
	for key := range conditions.SetMetadataIfAbsent {
		if _, exists := opts.Metadata[key]; exists {
			return false, fmt.Errorf("metadata key %q has both an ordinary and an if-absent write", key)
		}
	}
	for _, key := range conditions.UnsetMetadata {
		if _, exists := opts.Metadata[key]; exists {
			return false, fmt.Errorf("metadata key %q has both a set and an unset write", key)
		}
		if _, exists := conditions.SetMetadataIfAbsent[key]; exists {
			return false, fmt.Errorf("metadata key %q has both an if-absent and an unset write", key)
		}
	}
	if _, ok := tx.(nativeTransactionEventWriter); !ok {
		return false, fmt.Errorf("native transaction cannot record guarded update history: %w", ErrConditionalWriteUnsupported)
	}
	issue, err := tx.GetIssue(ctx, id)
	if err != nil {
		return false, nativeStoreError(id, err)
	}
	if issue == nil {
		return false, fmt.Errorf("bead %q: %w", id, ErrNotFound)
	}
	if conditions.Status != nil && string(issue.Status) != *conditions.Status ||
		conditions.Assignee != nil && issue.Assignee != *conditions.Assignee ||
		conditions.Title != nil && issue.Title != *conditions.Title ||
		conditions.Description != nil && issue.Description != *conditions.Description ||
		conditions.AcceptanceCriteria != nil && issue.AcceptanceCriteria != *conditions.AcceptanceCriteria {
		return false, nil
	}
	if conditions.Labels != nil {
		labels, err := tx.GetLabels(ctx, id)
		if err != nil {
			return false, nativeStoreError(id, err)
		}
		if len(labels) != len(*conditions.Labels) {
			return false, nil
		}
		for _, label := range labels {
			if !slices.Contains(*conditions.Labels, label) {
				return false, nil
			}
		}
	}
	metadata, err := metadataMapFromNative(issue.Metadata)
	if err != nil {
		return false, fmt.Errorf("parsing metadata for bead %q: %w", id, err)
	}
	for key, expected := range conditions.Metadata {
		if metadata[key] != expected {
			return false, nil
		}
	}
	update := opts
	if len(conditions.SetMetadataIfAbsent) != 0 {
		update.Metadata = maps.Clone(opts.Metadata)
		if update.Metadata == nil {
			update.Metadata = make(map[string]string, len(conditions.SetMetadataIfAbsent))
		}
		for key, value := range conditions.SetMetadataIfAbsent {
			if metadata[key] == "" {
				update.Metadata[key] = value
			}
		}
	}
	closing := update.Status != nil && *update.Status == "closed"
	// UpdateIssue rewrites the backend's shared row_lock cell, so a
	// concurrently changed guard conflicts even for label-only writes.
	// CloseIssue, not UpdateIssue, records the durable closed event.
	if closing {
		update.Status = nil
	}
	if update.Title == nil {
		title := issue.Title
		update.Title = &title
	}
	updates, err := s.nativeUpdates(ctx, tx, id, update)
	if err != nil {
		return false, err
	}
	if len(conditions.UnsetMetadata) != 0 {
		if metadata == nil {
			metadata = make(map[string]string, len(update.Metadata))
		}
		mergeUpdateMetadata(metadata, update.Metadata)
		for _, key := range conditions.UnsetMetadata {
			if key != RefineryDecisionAtKey || metadata[key] == "" {
				delete(metadata, key)
			}
		}
		raw, err := metadataRawFromMap(metadata)
		if err != nil {
			return false, err
		}
		if len(raw) == 0 {
			raw = []byte("{}")
		}
		updates["metadata"] = raw
	}
	actor := s.actor
	if conditions.Actor != "" {
		actor = conditions.Actor
	}
	if err := s.applyNativeUpdatesInTx(ctx, tx, id, update, updates, actor, true); err != nil {
		return false, err
	}
	if closing && string(issue.Status) != "closed" {
		current, err := tx.GetIssue(ctx, id)
		if err != nil {
			return false, nativeStoreError(id, err)
		}
		if err := tx.CloseIssue(ctx, id, nativeCloseReasonFromIssue(current), actor, ""); err != nil {
			return false, nativeStoreError(id, err)
		}
	}
	return true, nil
}

package beads

import (
	"context"
	"errors"
	"fmt"
	"strings"

	beadslib "github.com/steveyegge/beads"
)

// SingleTransactionStore commits all callback mutations in one backend SQL
// transaction, including durable and ephemeral subjects. Legacy AtomicTx/Tx is
// not evidence for this capability. Post-commit errors must not be replayed.
type SingleTransactionStore interface {
	TxSingle(string, func(Tx) error) error
}
type SingleTransactionStoreHandleProvider interface {
	SingleTransactionStoreHandle() (SingleTransactionStore, bool)
}

func SingleTransactionStoreFor(store Store) (SingleTransactionStore, bool) {
	if store == nil {
		return nil, false
	}
	store = followConditionalWritesResolveTarget(store)
	if provider, ok := store.(SingleTransactionStoreHandleProvider); ok {
		return provider.SingleTransactionStoreHandle()
	}
	writer, ok := store.(SingleTransactionStore)
	return writer, ok
}

// TxSingle gives the in-memory test double the same all-or-nothing callback
// semantics. Its sequential Tx remains unchanged.
func (m *MemStore) TxSingle(_ string, fn func(Tx) error) error {
	if fn == nil {
		return errors.New("beads single tx: nil callback")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	seq, snapshot, deps := m.snapshot()
	staged := NewMemStoreFrom(seq, snapshot, deps)
	staged.IDPrefix = m.IDPrefix
	staged.HonorExplicitIDs = m.HonorExplicitIDs
	staged.DisableConditionalWrites = m.DisableConditionalWrites
	if err := fn(staged); err != nil {
		return err
	}
	m.seq, m.beads, m.deps = staged.seq, staged.beads, staged.deps
	return nil
}

// FileStore embeds the memory test double but persists each mutation separately.
func (fs *FileStore) SingleTransactionStoreHandle() (SingleTransactionStore, bool) {
	return nil, false
}

func (fs *FileStore) TxSingle(_ string, _ func(Tx) error) error {
	return ErrConditionalWriteUnsupported
}

type nativeSingleTransactionRunner interface {
	RunInSingleTransaction(context.Context, string, func(beadslib.Transaction) error) error
}

func (s *NativeDoltStore) SingleTransactionStoreHandle() (SingleTransactionStore, bool) {
	storage, release, err := s.acquireStorage()
	if err != nil {
		return nil, false
	}
	defer release()
	if _, ok := storage.(nativeSingleTransactionRunner); !ok {
		return nil, false
	}
	return s, true
}

func (s *NativeDoltStore) TxSingle(commitMsg string, fn func(Tx) error) error {
	if fn == nil {
		return errors.New("beads single tx: nil callback")
	}
	storage, release, err := s.acquireStorage()
	if err != nil {
		return err
	}
	defer release()
	runner, ok := storage.(nativeSingleTransactionRunner)
	if !ok {
		return fmt.Errorf("native backend has no single-session transaction capability: %w", ErrConditionalWriteUnsupported)
	}
	ctx, cancel := nativeDoltOperationContext(context.TODO())
	defer cancel()
	if strings.TrimSpace(commitMsg) == "" {
		commitMsg = "gc: single transaction"
	}
	return runner.RunInSingleTransaction(ctx, commitMsg, func(tx beadslib.Transaction) error {
		return fn(&nativeDoltTx{store: s, ctx: ctx, tx: tx})
	})
}

func (c *CachingStore) SingleTransactionStoreHandle() (SingleTransactionStore, bool) {
	if _, ok := SingleTransactionStoreFor(c.conditionalBacking()); !ok {
		return nil, false
	}
	return c, true
}

func (c *CachingStore) TxSingle(commitMsg string, fn func(Tx) error) error {
	writer, ok := SingleTransactionStoreFor(c.conditionalBacking())
	if !ok {
		return fmt.Errorf("backing store has no single-session transaction capability: %w", ErrConditionalWriteUnsupported)
	}
	return c.runCachingTx(commitMsg, fn, writer.TxSingle, true)
}

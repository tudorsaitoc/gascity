package beads

import (
	"errors"
	"fmt"
	"testing"
)

type legacyAtomicStoreForSingleTest struct{ Store }

func (*legacyAtomicStoreForSingleTest) AtomicTx() bool { return true }

func TestSingleTransactionCapabilityNeverInfersLegacyAtomicTx(t *testing.T) {
	legacy := &legacyAtomicStoreForSingleTest{Store: NewMemStore()}
	cache := NewCachingStore(legacy, nil)
	if _, ok := SingleTransactionStoreFor(legacy); ok {
		t.Fatal("legacy rollback marker falsely advertised single-session commit")
	}
	if _, ok := SingleTransactionStoreFor(cache); ok {
		t.Fatal("cache falsely promoted legacy rollback into single-session commit")
	}
	entered := false
	err := cache.TxSingle("must refuse", func(Tx) error { entered = true; return nil })
	if entered || !errors.Is(err, ErrConditionalWriteUnsupported) {
		t.Fatalf("missing single-session capability entered callback or hid refusal: entered=%v err=%v", entered, err)
	}
}

type committedConflictForSingleTest struct{ error }

func (committedConflictForSingleTest) SQLCommitted() bool { return true }

func TestKnownCommittedSerializationMessageMustNotRetry(t *testing.T) {
	conflict := errors.New("Error 1213 (40001): this transaction conflicts with a committed transaction")
	if !isNativeDoltSerializationConflict(conflict) {
		t.Fatal("rolled-back native serialization conflict lost retry classification")
	}
	committed := fmt.Errorf("finalization: %w", committedConflictForSingleTest{error: conflict})
	if isNativeDoltSerializationConflict(committed) {
		t.Fatal("already committed mutation was admitted to callback replay")
	}
}

func TestMemorySingleTransactionCommitsOrDiscardsWholeCallback(t *testing.T) {
	store := NewMemStore()
	source, err := store.Create(Bead{Title: "original goal"})
	if err != nil {
		t.Fatal(err)
	}
	refused := errors.New("candidate incarnation changed")
	var discardedID string
	err = store.TxSingle("discard", func(tx Tx) error {
		title := "uncommitted goal"
		if err := tx.Update(source.ID, UpdateOpts{Title: &title}); err != nil {
			return err
		}
		candidate, err := tx.Create(Bead{Title: "uncommitted candidate"})
		discardedID = candidate.ID
		if err != nil {
			return err
		}
		return refused
	})
	if !errors.Is(err, refused) {
		t.Fatalf("callback refusal lost: %v", err)
	}
	got, err := store.Get(source.ID)
	if err != nil || got.Title != source.Title {
		t.Fatalf("aborted source update leaked: %+v, %v", got, err)
	}
	if _, err := store.Get(discardedID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("aborted candidate persisted: %v", err)
	}
	var committedID string
	if err := store.TxSingle("commit", func(tx Tx) error {
		title := "accepted goal"
		if err := tx.Update(source.ID, UpdateOpts{Title: &title}); err != nil {
			return err
		}
		candidate, err := tx.Create(Bead{Title: "accepted candidate"})
		committedID = candidate.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	got, err = store.Get(source.ID)
	if err != nil || got.Title != "accepted goal" {
		t.Fatalf("accepted source update absent: %+v, %v", got, err)
	}
	candidate, err := store.Get(committedID)
	if err != nil || candidate.Title != "accepted candidate" {
		t.Fatalf("accepted candidate absent: %+v, %v", candidate, err)
	}
}

func TestFileStoreDoesNotInheritMemorySingleTransactionCapability(t *testing.T) {
	store := &FileStore{MemStore: NewMemStore()}
	if _, ok := SingleTransactionStoreFor(store); ok {
		t.Fatal("per-write file persistence was admitted as a single transaction")
	}
	entered := false
	err := store.TxSingle("must refuse", func(Tx) error {
		entered = true
		return nil
	})
	if entered || !errors.Is(err, ErrConditionalWriteUnsupported) {
		t.Fatalf("unsupported file writer entered callback or hid refusal: entered=%v err=%v", entered, err)
	}
}

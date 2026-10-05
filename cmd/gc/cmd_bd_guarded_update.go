package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/gastownhall/gascity/internal/bdflags"
	"github.com/gastownhall/gascity/internal/beads"
)

const bdUpdatePreconditionFailedExitCode = 13

// nativeBdUpdateRequested identifies GC's transaction-only metadata flags and
// explicit clock-key mutations. Ordinary JSON documents retain BD's typed-JSON
// contract; native guarded documents use the SDK's string-valued metadata.
func nativeBdUpdateRequested(args []string) bool {
	sub, tail, resolved := bdByIDSubcommand(args)
	if !resolved || sub != "update" {
		return false
	}
	values := bdflags.ValueFlags(sub)
	for i := 0; i < len(tail); i++ {
		if tail[i] == "--" {
			break
		}
		name, value, inline := strings.Cut(tail[i], "=")
		if !values[name] {
			continue
		}
		if !inline {
			if i+1 >= len(tail) {
				return nativeBdGuardFlag(name)
			}
			i++
			value = tail[i]
		}
		switch name {
		case "--if-labels-json", "--if-title", "--if-description", "--if-acceptance":
			return true
		case "--if-metadata", "--set-metadata-if-absent":
			return true
		case "--set-metadata":
			key, _, _ := strings.Cut(value, "=")
			if key == beads.RefineryDecisionAtKey {
				return true
			}
		case "--unset-metadata":
			if value == beads.RefineryDecisionAtKey {
				return true
			}
		}
	}
	return false
}

func nativeBdGuardFlag(name string) bool {
	switch name {
	case "--if-metadata", "--set-metadata-if-absent", "--if-labels-json", "--if-title", "--if-description", "--if-acceptance":
		return true
	}
	return false
}

func parseNativeBdUpdate(args []string) (bdByIDOp, string, bool) {
	_, tail, _ := bdByIDSubcommand(args)
	op, rejected, ok := parseBdByIDUpdateArgs(tail)
	// Root JSON and actor are the root options this mutation arm represents.
	root := args[:len(args)-len(tail)-1]
	for i := 0; i < len(root); i++ {
		name, value, inline := strings.Cut(root[i], "=")
		switch name {
		case "--json":
			if inline {
				return bdByIDOp{}, name, false
			}
			op.JSON = true
		case "--actor":
			if !inline {
				if i+1 >= len(root) {
					return bdByIDOp{}, name, false
				}
				i++
				value = root[i]
			}
			op.Conditions.Actor = value
		default:
			return bdByIDOp{}, name, false
		}
	}
	return op, rejected, ok
}

func doBdGuardedUpdate(store beads.Store, op bdByIDOp, binding string, stdout, stderr io.Writer) int {
	writer, ok := beads.GuardedUpdateWriterFor(store)
	if !ok {
		fmt.Fprintln(stderr, "gc bd update: backend does not support transactional field/metadata guards; refusing unchecked mutation") //nolint:errcheck
		return 1
	}
	applied, err := writer.UpdateGuarded(op.ID, op.Update, op.Conditions)
	if err != nil {
		fmt.Fprintf(stderr, "gc bd update: %s: %v\n", op.ID, err) //nolint:errcheck
		return 1
	}
	if !applied {
		fmt.Fprintf(stderr, "gc bd update: %s: precondition failed; no fields or metadata written\n", op.ID) //nolint:errcheck
		return bdUpdatePreconditionFailedExitCode
	}
	updated, err := store.Get(op.ID)
	if err != nil {
		fmt.Fprintf(stderr, "gc bd update: %s was written and could not be re-read: %v\n", op.ID, err) //nolint:errcheck
		return 1
	}
	return printBdByIDBead(updated, op.JSON, binding, stdout, stderr)
}

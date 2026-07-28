package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/gastownhall/gascity/internal/config"
	"github.com/gastownhall/gascity/internal/shellquote"
)

func newHookCmd(stdout, stderr io.Writer) *cobra.Command {
	var inject bool
	var claim bool
	var jsonOutput bool
	var hookFormat string
	cmd := &cobra.Command{
		Use:   "hook [agent]",
		Short: "Check for available work",
		Long: `Checks for available work using the agent's work_query config.

Without --inject: prints raw output, exits 0 if work exists, 1 if empty.
With --inject: silent legacy Stop-hook compatibility; skips the work query and always exits 0.

		The agent is determined from $GC_AGENT or a positional argument.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if cmdHookWithOptions(args, hookOptions{
				inject:     inject,
				claim:      claim,
				jsonOutput: jsonOutput,
				hookFormat: hookFormat,
			}, stdout, stderr) != 0 {
				return errExit
			}
			return nil
		},
	}
	cmd.Flags().BoolVar(&inject, "inject", false, "silent legacy Stop-hook compatibility; skip work query and exit 0")
	cmd.Flags().BoolVar(&claim, "claim", false, "claim a returned candidate before printing it")
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "print JSON work items")
	cmd.Flags().StringVar(&hookFormat, "hook-format", "", "format hook output for a provider")
	if flag := cmd.Flags().Lookup("hook-format"); flag != nil {
		flag.Hidden = true
	}
	return cmd
}

// cmdHook is the CLI entry point for gc hook. Resolves the agent from
// $GC_AGENT or a positional argument, loads the city config, and runs
// the agent's work query.
func cmdHook(args []string, stdout, stderr io.Writer) int {
	return cmdHookWithFormat(args, false, "", stdout, stderr)
}

func cmdHookWithFormat(args []string, inject bool, hookFormat string, stdout, stderr io.Writer) int {
	return cmdHookWithOptions(args, hookOptions{
		inject:     inject,
		hookFormat: hookFormat,
	}, stdout, stderr)
}

type hookOptions struct {
	inject     bool
	claim      bool
	jsonOutput bool
	hookFormat string
}

func cmdHookWithOptions(args []string, opts hookOptions, stdout, stderr io.Writer) int {
	inject := opts.inject
	if inject {
		return 0
	}
	// Accepted for compatibility with installed hook commands; non-inject
	// gc hook output is intentionally raw regardless of provider format.
	_ = opts.hookFormat
	// --json is a compatibility promise for claim-mode startup prompts. The
	// configured work_query already controls the concrete output format.
	_ = opts.jsonOutput

	agentName := os.Getenv("GC_ALIAS")
	if agentName == "" {
		agentName = os.Getenv("GC_AGENT")
	}
	sessionTemplateContext := false
	if len(args) == 0 {
		template := strings.TrimSpace(os.Getenv("GC_TEMPLATE"))
		hasSessionContext := strings.TrimSpace(os.Getenv("GC_SESSION_NAME")) != "" ||
			strings.TrimSpace(os.Getenv("GC_SESSION_ID")) != ""
		if template != "" && hasSessionContext {
			agentName = template
			sessionTemplateContext = true
		}
	}
	if len(args) > 0 {
		agentName = args[0]
	}
	if agentName == "" {
		fmt.Fprintln(stderr, "gc hook: agent not specified (set $GC_AGENT or pass as argument)") //nolint:errcheck // best-effort stderr
		return 1
	}

	cityPath, err := resolveCity()
	if err != nil {
		fmt.Fprintf(stderr, "gc hook: %v\n", err) //nolint:errcheck // best-effort stderr
		return 1
	}
	cfg, err := loadCityConfig(cityPath, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "gc hook: %v\n", err) //nolint:errcheck // best-effort stderr
		return 1
	}
	// Normalize relative rig paths to absolute so downstream rig-matching
	// (agentCommandDir, bdRuntimeEnvForRig) compares apples to apples.
	// Other CLI entry points (cmd_sling, cmd_start, cmd_rig, cmd_supervisor)
	// do the same immediately after loadCityConfig.
	resolveRigPaths(cityPath, cfg.Rigs)

	if citySuspended(cfg) {
		fmt.Fprintln(stderr, "gc hook: city is suspended") //nolint:errcheck // best-effort stderr
		return 1
	}

	a, ok := resolveAgentIdentity(cfg, agentName, currentRigContext(cfg))
	if !ok {
		fmt.Fprintf(stderr, "gc hook: agent %q not found in config\n", agentName) //nolint:errcheck // best-effort stderr
		return 1
	}

	if isAgentEffectivelySuspended(cfg, &a) {
		fmt.Fprintf(stderr, "gc hook: agent %q is suspended\n", agentName) //nolint:errcheck // best-effort stderr
		return 1
	}

	cityName := loadedCityName(cfg, cityPath)
	workQuery := a.EffectiveWorkQuery()
	// Expand {{.Rig}}/{{.AgentBase}} in user-supplied work_query so agent-side
	// hook invocation sees the same rig substitution as the controller-side
	// probes in build_desired_state.go / session_reconcile.go. #793.
	workQuery = expandAgentCommandTemplate(cityPath, cityName, &a, cfg.Rigs, "work_query", workQuery, stderr)
	workDir := agentCommandDir(cityPath, &a, cfg.Rigs)

	// Build the work query subprocess environment. Rig-backed agents get
	// rig-scoped BEADS_DIR / GC_RIG_ROOT / Dolt coordinates so the query
	// reads the rig store rather than whatever BEADS_DIR the parent
	// process happens to inherit (issue #514). Many built-in work queries
	// also key off session identity. Explicit hook targets get resolved
	// names; named-session context preserves the runtime-supplied owner
	// env while selecting the backing config through GC_TEMPLATE.
	resolvedAgentName := a.QualifiedName()
	agentForQuery := resolvedAgentName
	sessionForQuery := ""
	if sessionTemplateContext {
		agentForQuery = os.Getenv("GC_ALIAS")
		if agentForQuery == "" {
			agentForQuery = os.Getenv("GC_SESSION_NAME")
		}
		if agentForQuery == "" {
			agentForQuery = os.Getenv("GC_AGENT")
		}
		sessionForQuery = os.Getenv("GC_SESSION_NAME")
	} else {
		sessionForQuery = cliSessionName(cityPath, cityName, resolvedAgentName, cfg.Workspace.SessionTemplate)
	}
	overrides := hookQueryEnv(cityPath, cfg, &a)
	overrides["GC_AGENT"] = agentForQuery
	overrides["GC_SESSION_NAME"] = sessionForQuery
	if sessionTemplateContext {
		overrides["GC_ALIAS"] = os.Getenv("GC_ALIAS")
		overrides["GC_SESSION_ID"] = os.Getenv("GC_SESSION_ID")
		overrides["GC_SESSION_ORIGIN"] = os.Getenv("GC_SESSION_ORIGIN")
		overrides["GC_TEMPLATE"] = os.Getenv("GC_TEMPLATE")
	}
	queryEnv := mergeRuntimeEnv(os.Environ(), overrides)
	runner := func(command, dir string) (string, error) {
		return shellWorkQueryWithEnv(command, dir, queryEnv)
	}
	var claimRunner HookClaimRunner
	if (opts.claim || (len(args) == 0 && sessionTemplateContext)) && len(args) == 0 {
		claimRunner = func(id, dir string) (string, error) {
			return shellWorkQueryWithEnv(hookClaimCommand(id), dir, queryEnv)
		}
	}
	return doHookWithCandidateClaim(workQuery, workDir, inject, runner, claimRunner, hookCurrentIDsFromEnv(queryEnv), stdout, stderr)
}

// hookQueryEnv returns the full work-query environment for a hook subprocess.
// It includes scope metadata (store root/scope/prefix) plus any rig-scoped
// runtime overrides so hook queries observe the same routing contract as the
// controller probes.
func hookQueryEnv(cityPath string, cfg *config.City, a *config.Agent) map[string]string {
	env := controllerWorkQueryEnv(cityPath, cfg, a)
	if env == nil {
		env = map[string]string{}
	}
	return env
}

// WorkQueryRunner runs a work query command and returns its stdout.
// dir sets the command's working directory.
type WorkQueryRunner func(command, dir string) (string, error)

// HookClaimRunner atomically claims a candidate bead ID and returns fresh JSON
// for the claimed bead. Claim failures are treated as lost races by the hook
// path, so callers should return an error when the candidate is no longer
// claimable.
type HookClaimRunner func(id, dir string) (string, error)

// shellWorkQueryWithEnv runs a work query command via sh -c and returns
// stdout. If env is non-nil it is used as the subprocess environment
// (including any rig-scoped BEADS_DIR / GC_RIG_ROOT overrides); otherwise
// the child inherits the parent process environment. Times out after 30
// seconds.
func shellWorkQueryWithEnv(command, dir string, env []string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.WaitDelay = 2 * time.Second
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = workQueryEnvForDir(env, dir)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("running work query %q: %w", command, err)
	}
	return string(out), nil
}

// workQueryEnvForDir ensures the subprocess environment does not carry a
// stale inherited PWD when exec.Cmd.Dir points somewhere else. Some shells
// (notably macOS /bin/sh) preserve the inherited PWD instead of recomputing
// it from the real working directory, which breaks hook work_query commands
// that inspect $PWD.
func workQueryEnvForDir(env []string, dir string) []string {
	if env == nil {
		env = mergeRuntimeEnv(os.Environ(), nil)
	}
	if dir == "" {
		return env
	}
	out := removeEnvKey(append([]string(nil), env...), "PWD")
	return append(out, "PWD="+dir)
}

// doHook is the pure logic for gc hook. Runs the work query and outputs
// results based on mode. Without inject: prints raw output, returns 0 if
// work, 1 if empty. With inject: skips the work query and returns 0.
func doHook(workQuery, dir string, inject bool, runner WorkQueryRunner, stdout, stderr io.Writer) int {
	return doHookWithCandidateClaim(workQuery, dir, inject, runner, nil, nil, stdout, stderr)
}

func doHookWithCandidateClaim(
	workQuery, dir string,
	inject bool,
	runner WorkQueryRunner,
	claimRunner HookClaimRunner,
	currentIDs map[string]bool,
	stdout, stderr io.Writer,
) int {
	if inject {
		return 0
	}

	attempts := 1
	if claimRunner != nil && len(currentIDs) > 0 {
		attempts = 3
	}
	for attempt := 0; attempt < attempts; attempt++ {
		output, err := runner(workQuery, dir)
		if err != nil {
			fmt.Fprintf(stderr, "gc hook: %v\n", err) //nolint:errcheck // best-effort stderr
			return 1
		}

		trimmed := strings.TrimSpace(output)
		normalized := normalizeWorkQueryOutput(trimmed)
		hasWork := workQueryHasReadyWork(normalized)

		if hasWork && claimRunner != nil && len(currentIDs) > 0 {
			if claimed, handled := resolveHookCandidateOutput(normalized, dir, claimRunner, currentIDs); handled {
				if workQueryHasReadyWork(claimed) {
					fmt.Fprint(stdout, claimed) //nolint:errcheck // best-effort stdout
					return 0
				}
				continue
			}
		}

		// Non-inject mode: print raw output. Return 0 only when work exists.
		if !hasWork {
			if normalized != "" {
				fmt.Fprint(stdout, normalized) //nolint:errcheck // best-effort stdout
			}
			return 1
		}
		fmt.Fprint(stdout, normalized) //nolint:errcheck // best-effort stdout
		return 0
	}
	if claimRunner != nil {
		fmt.Fprint(stdout, "[]") //nolint:errcheck // best-effort stdout
	}
	return 1
}

type hookCandidate struct {
	ID       string         `json:"id"`
	Status   string         `json:"status"`
	Assignee string         `json:"assignee"`
	Type     string         `json:"issue_type"`
	Metadata map[string]any `json:"metadata"`
	raw      json.RawMessage
}

func resolveHookCandidateOutput(output, dir string, claimRunner HookClaimRunner, currentIDs map[string]bool) (string, bool) {
	candidates, ok := parseHookCandidates(output)
	if !ok {
		return output, false
	}
	if len(candidates) == 0 {
		return "[]", true
	}
	recognized := false
	for _, candidate := range candidates {
		if !candidate.looksLikeBead() {
			continue
		}
		recognized = true
		if candidate.ownedBy(currentIDs) && strings.EqualFold(candidate.Status, "in_progress") {
			return marshalHookCandidates([]hookCandidate{candidate}), true
		}
		if candidate.claimableBy(currentIDs) {
			claimed, err := claimRunner(candidate.ID, dir)
			if err != nil {
				continue
			}
			normalized := normalizeWorkQueryOutput(strings.TrimSpace(claimed))
			claimedCandidates, ok := parseHookCandidates(normalized)
			if !ok {
				continue
			}
			for _, claimedCandidate := range claimedCandidates {
				if claimedCandidate.ID == candidate.ID && claimedCandidate.ownedBy(currentIDs) {
					return marshalHookCandidates([]hookCandidate{claimedCandidate}), true
				}
			}
		}
	}
	if !recognized {
		return output, false
	}
	return "[]", true
}

func parseHookCandidates(output string) ([]hookCandidate, bool) {
	output = strings.TrimSpace(output)
	if output == "" {
		return nil, false
	}
	var rawItems []json.RawMessage
	if err := json.Unmarshal([]byte(output), &rawItems); err != nil {
		var rawItem json.RawMessage
		if err := json.Unmarshal([]byte(output), &rawItem); err != nil {
			return nil, false
		}
		rawItems = []json.RawMessage{rawItem}
	}
	candidates := make([]hookCandidate, 0, len(rawItems))
	for _, raw := range rawItems {
		var candidate hookCandidate
		if err := json.Unmarshal(raw, &candidate); err != nil {
			return nil, false
		}
		candidate.ID = strings.TrimSpace(candidate.ID)
		candidate.Status = strings.TrimSpace(candidate.Status)
		candidate.Assignee = strings.TrimSpace(candidate.Assignee)
		candidate.Type = strings.TrimSpace(candidate.Type)
		candidate.raw = append(candidate.raw[:0], raw...)
		candidates = append(candidates, candidate)
	}
	return candidates, true
}

func marshalHookCandidates(candidates []hookCandidate) string {
	rawItems := make([]json.RawMessage, 0, len(candidates))
	for _, candidate := range candidates {
		if len(candidate.raw) > 0 {
			rawItems = append(rawItems, candidate.raw)
			continue
		}
		raw, err := json.Marshal(candidate)
		if err == nil {
			rawItems = append(rawItems, raw)
		}
	}
	out, err := json.Marshal(rawItems)
	if err != nil {
		return "[]"
	}
	return string(out)
}

func (c hookCandidate) looksLikeBead() bool {
	return c.ID != "" && (c.Status != "" || c.Assignee != "" || c.Type != "" || len(c.Metadata) > 0)
}

func (c hookCandidate) ownedBy(currentIDs map[string]bool) bool {
	return currentIDs[strings.TrimSpace(c.Assignee)]
}

func (c hookCandidate) claimableBy(currentIDs map[string]bool) bool {
	if c.ID == "" {
		return false
	}
	assignee := strings.TrimSpace(c.Assignee)
	if assignee != "" && !currentIDs[assignee] {
		return false
	}
	status := strings.ToLower(strings.TrimSpace(c.Status))
	return status == "" || status == "open"
}

func hookClaimCommand(id string) string {
	quotedID := shellquote.Quote(id)
	actor := `BEADS_ACTOR="${GC_ALIAS:-${GC_SESSION_NAME:-$GC_AGENT}}"`
	return "(" + actor + " bd update " + quotedID + " --claim >/dev/null 2>&1 || " + actor +
		" gc --city \"$GC_CITY\" bd update " + quotedID + " --claim >/dev/null 2>&1) && (bd show " + quotedID +
		" --json 2>/dev/null || gc --city \"$GC_CITY\" bd show " + quotedID + " --json)"
}

func hookCurrentIDsFromEnv(env []string) map[string]bool {
	values := map[string]string{}
	for _, item := range env {
		key, value, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}
		values[key] = value
	}
	ids := map[string]bool{}
	for _, key := range []string{"GC_SESSION_ID", "GC_SESSION_NAME", "GC_ALIAS", "GC_AGENT"} {
		value := strings.TrimSpace(values[key])
		if value != "" {
			ids[value] = true
		}
	}
	return ids
}

func workQueryHasReadyWork(output string) bool {
	if output == "" {
		return false
	}
	// Newer bd versions print a human-readable no-work line to stdout instead
	// of staying silent. Treat that as "no work" for hooks and WakeWork.
	if strings.Contains(output, "No ready work found") {
		return false
	}
	var decoded any
	if err := json.Unmarshal([]byte(output), &decoded); err == nil {
		switch v := decoded.(type) {
		case []any:
			return len(v) > 0
		case map[string]any:
			return len(v) > 0
		case nil:
			return false
		}
	}
	return true
}

func normalizeWorkQueryOutput(output string) string {
	if output == "" {
		return output
	}
	var decoded any
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		return output
	}
	if _, ok := decoded.(map[string]any); !ok {
		return output
	}
	normalized, err := json.Marshal([]any{decoded})
	if err != nil {
		return output
	}
	return string(normalized)
}

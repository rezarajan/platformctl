package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/rezarajan/platformctl/internal/cliutil"
	"github.com/rezarajan/platformctl/internal/domain/status"
)

// explainEntryOutput mirrors status.CatalogEntry for -o json|yaml, dropping
// the internal Area/Prefix bookkeeping fields that don't help a reader
// looking up one token.
type explainEntryOutput struct {
	Token    string   `json:"token" yaml:"token"`
	Kind     string   `json:"kind" yaml:"kind"`
	Meaning  string   `json:"meaning" yaml:"meaning"`
	Causes   []string `json:"causes,omitempty" yaml:"causes,omitempty"`
	Remedies []string `json:"remedies,omitempty" yaml:"remedies,omitempty"`
}

// explainOutput is the -o json|yaml document for `platformctl explain`
// (docs/planning/08 E4): exactly one machine-readable document, whether the
// query resolved to a single entry or fell back to a candidate list.
type explainOutput struct {
	Query      string              `json:"query" yaml:"query"`
	Matched    bool                `json:"matched" yaml:"matched"`
	Entry      *explainEntryOutput `json:"entry,omitempty" yaml:"entry,omitempty"`
	Candidates []string            `json:"candidates" yaml:"candidates"`
}

func toEntryOutput(e status.CatalogEntry) *explainEntryOutput {
	return &explainEntryOutput{
		Token: e.Token, Kind: e.Kind, Meaning: e.Meaning,
		Causes: e.Causes, Remedies: e.Remedies,
	}
}

// explainLookup resolves query against status.Catalog: exact match first
// (including a dynamic-prefix entry's Token being a literal prefix of a
// query pasted straight from `status`/`drift` output, e.g.
// "ConnectorStatePAUSED"), then a case-insensitive prefix match, then a
// case-insensitive substring match — each stage stopping as soon as it
// narrows to a unique entry. Returns (entry, candidateTokens); entry is nil
// when the match isn't unique (candidateTokens then lists what matched, 0
// or more).
func explainLookup(query string) (*status.CatalogEntry, []string) {
	// Stage 1: exact token match.
	for i := range status.Catalog {
		e := &status.Catalog[i]
		if e.Token == query {
			return e, nil
		}
	}
	// Stage 1b: dynamic-prefix entries whose Token is an exact prefix of
	// the query — the common case of pasting a live reason string
	// straight from `status`/`drift` output (e.g. "PartitionCountMismatch
	// (3!=5)").
	var prefixHits []*status.CatalogEntry
	for i := range status.Catalog {
		e := &status.Catalog[i]
		if e.Prefix && strings.HasPrefix(query, e.Token) {
			prefixHits = append(prefixHits, e)
		}
	}
	if len(prefixHits) == 1 {
		return prefixHits[0], nil
	}
	if len(prefixHits) > 1 {
		return nil, entryTokens(prefixHits)
	}

	lowerQuery := strings.ToLower(query)

	// Stage 2: case-insensitive prefix match on Token.
	var ciPrefixHits []*status.CatalogEntry
	for i := range status.Catalog {
		e := &status.Catalog[i]
		if strings.HasPrefix(strings.ToLower(e.Token), lowerQuery) {
			ciPrefixHits = append(ciPrefixHits, e)
		}
	}
	if len(ciPrefixHits) == 1 {
		return ciPrefixHits[0], nil
	}
	if len(ciPrefixHits) > 1 {
		return nil, entryTokens(ciPrefixHits)
	}

	// Stage 3: case-insensitive substring match on Token.
	var substrHits []*status.CatalogEntry
	for i := range status.Catalog {
		e := &status.Catalog[i]
		if strings.Contains(strings.ToLower(e.Token), lowerQuery) {
			substrHits = append(substrHits, e)
		}
	}
	if len(substrHits) == 1 {
		return substrHits[0], nil
	}
	return nil, entryTokens(substrHits)
}

func entryTokens(entries []*status.CatalogEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Token)
	}
	sort.Strings(out)
	return out
}

func newExplainCmd(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "explain <ConditionType|reason|error-token>",
		Short: "Look up a Condition Type, Condition Reason, or dynamic-reason token in the embedded catalog",
		Long: "Resolves a token (a ConditionType like Ready/DriftDetected, or a Reason like\n" +
			"WALNotLogical or a dynamic-prefix reason like ConnectorStatePAUSED) against\n" +
			"the embedded catalog: meaning, likely causes, and remedy commands. Tries an\n" +
			"exact match first, then a case-insensitive prefix match, then a\n" +
			"case-insensitive substring match — the first stage that narrows to exactly\n" +
			"one entry wins; an ambiguous or empty match instead lists candidates.\n\n" +
			"See `platformctl status`'s footnote — it points here whenever a resource is\n" +
			"not Ready.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			query := args[0]
			entry, candidates := explainLookup(query)
			out := explainOutput{Query: query, Candidates: candidates}
			if candidates == nil {
				out.Candidates = []string{}
			}
			if entry != nil {
				out.Matched = true
				out.Entry = toEntryOutput(*entry)
			}
			if isStructured(a.output) {
				if err := cliutil.WriteOutput(cmd.OutOrStdout(), a.output, out, nil); err != nil {
					return err
				}
			} else if entry != nil {
				printExplainEntry(cmd, *entry)
			} else if len(out.Candidates) == 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "no catalog entry matches %q\n", query)
			} else {
				fmt.Fprintf(cmd.OutOrStdout(), "%q is ambiguous; did you mean one of:\n", query)
				for _, c := range out.Candidates {
					fmt.Fprintf(cmd.OutOrStdout(), "  %s\n", c)
				}
			}
			if entry == nil {
				return cliutil.Exit(cliutil.ExitValidation, fmt.Errorf("explain %q: no unique catalog entry (see candidates above)", query))
			}
			return nil
		},
	}
	return cmd
}

func printExplainEntry(cmd *cobra.Command, e status.CatalogEntry) {
	w := cmd.OutOrStdout()
	fmt.Fprintf(w, "%s (%s)\n\n", e.Token, e.Kind)
	fmt.Fprintf(w, "Meaning:\n  %s\n", e.Meaning)
	if len(e.Causes) > 0 {
		fmt.Fprintln(w, "\nLikely causes:")
		for _, c := range e.Causes {
			fmt.Fprintf(w, "  - %s\n", c)
		}
	}
	if len(e.Remedies) > 0 {
		fmt.Fprintln(w, "\nRemedies:")
		for _, r := range e.Remedies {
			fmt.Fprintf(w, "  - %s\n", r)
		}
	}
}

// Package lint defines the design-lint finding vocabulary shared by the
// built-in engine (internal/application/lint), the DesignLinter provider
// capability (internal/ports/reconciler), and every technology that
// implements it. It lives in domain, not application, because
// reconciler.DesignLinter — a port — must return Finding values, and ports
// may import only domain (CLAUDE.md's layering invariant).
// See docs/adr/020-design-lints.md.
package lint

import (
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// Severity is a lint finding's severity. ADR 020 §2: no lint is ever
// "error" — that vocabulary belongs to validation (ADR 011) and policy
// (ADR 021). Lints only ever detect and report.
type Severity string

const (
	// Warning marks a probable mistake or operational hazard.
	Warning Severity = "warning"
	// Info marks an inert, unused, or unconventional — but not clearly
	// wrong — shape.
	Info Severity = "info"
)

// severityRank orders Warning before Info for Less below — a warning is
// more actionable, so it sorts first.
func severityRank(s Severity) int {
	switch s {
	case Warning:
		return 0
	case Info:
		return 1
	default:
		return 2
	}
}

// Finding is one design-lint result. Code is a stable, documented token
// (platformctl explain <Code>): DL001-DL021 for the built-in set (ADR 020
// §4, sparse — only the codes the table actually lists exist), DL000 for
// the one housekeeping case the ADR calls out without coding
// ("empty reason = the waiver itself is a warning"), and DL-<type>-NNN for
// provider-contributed lints (ADR 020 §5).
type Finding struct {
	Code     string
	Severity Severity
	// Resource is the resource this finding is attached to — also the
	// resource whose metadata.annotations is consulted for a waiver
	// (per-resource, per-code, ADR 020 §2).
	Resource resource.Key
	// Message is the human-readable finding text; it names every resource
	// involved (the finding's own Resource plus any others), since Message
	// is what a reader actually sees in `platformctl lint`'s table/JSON
	// output.
	Message string
	// Waived and WaiverReason are populated by the lint engine after
	// matching a metadata.annotations[WaiveAnnotation] entry against this
	// finding's Code+Resource — built-ins and DesignLinter implementations
	// never set these themselves; a Finding as returned by a check is
	// always Waived: false.
	Waived       bool
	WaiverReason string
}

// Less orders findings by (severity, code, resource key) — ADR 020's
// determinism bar: "findings sorted by (severity, code, resource key),
// byte-identical output for identical input".
func Less(a, b Finding) bool {
	if ra, rb := severityRank(a.Severity), severityRank(b.Severity); ra != rb {
		return ra < rb
	}
	if a.Code != b.Code {
		return a.Code < b.Code
	}
	return a.Resource.String() < b.Resource.String()
}

// WaiveAnnotation is the metadata.annotations key a resource sets to waive
// one or more lint findings against it (ADR 020 §2):
//
//	metadata:
//	  annotations:
//	    lint.datascape.io/waive: "DL102: reason"
//
// Multiple codes on one resource are comma-separated entries of the same
// "CODE: reason" shape — the ADR gives a single-code example; this is the
// natural, minimal extension for a resource that needs to waive more than
// one finding, not a reopened decision.
const WaiveAnnotation = "lint.datascape.io/waive"

// Waiver is one parsed "CODE: reason" entry from WaiveAnnotation.
type Waiver struct {
	Code   string
	Reason string
}

// ParseWaivers parses metadata.annotations[WaiveAnnotation] into individual
// Waivers, one per comma-separated entry. A malformed entry — no colon, or
// an empty reason after it — is still returned (Code holds whatever text
// preceded the colon, or the whole entry if there was none; Reason is
// empty) so the caller can flag it as its own finding (ADR 020: "empty
// reason = the waiver itself is a warning") rather than silently dropping
// it.
func ParseWaivers(annotations map[string]string) []Waiver {
	raw, ok := annotations[WaiveAnnotation]
	if !ok || raw == "" {
		return nil
	}
	var out []Waiver
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		code, reason, found := strings.Cut(entry, ":")
		if !found {
			out = append(out, Waiver{Code: strings.TrimSpace(entry)})
			continue
		}
		out = append(out, Waiver{Code: strings.TrimSpace(code), Reason: strings.TrimSpace(reason)})
	}
	return out
}

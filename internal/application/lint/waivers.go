package lint

import (
	"fmt"

	"github.com/rezarajan/platformctl/internal/domain/lint"
	"github.com/rezarajan/platformctl/internal/domain/resource"
)

// applyWaivers matches every finding's (Code, Resource) against the
// resource's own metadata.annotations[lint.WaiveAnnotation] entries,
// setting Waived/WaiverReason on a match with a non-empty reason. A waiver
// entry with an empty reason does NOT suppress the finding it names (ADR
// 020 §2: "empty reason = the waiver itself is a warning" — a warning, not
// a successful waiver) and additionally produces its own DL000 finding, one
// per malformed entry, regardless of whether it happened to match a real
// finding.
func applyWaivers(envelopes []resource.Envelope, findings []Finding) []Finding {
	byKey := make(map[resource.Key]resource.Envelope, len(envelopes))
	for _, e := range envelopes {
		byKey[e.Key()] = e
	}

	var malformed []Finding
	for _, e := range envelopes {
		for _, w := range lint.ParseWaivers(e.Metadata.Annotations) {
			if w.Reason != "" {
				continue
			}
			malformed = append(malformed, Finding{
				Code:     CodeMalformedWaiver,
				Severity: lint.Warning,
				Resource: e.Key(),
				Message:  fmt.Sprintf("metadata.annotations[%s] waives %q with no reason — a waiver's reason is mandatory (ADR 020 §2)", lint.WaiveAnnotation, w.Code),
			})
		}
	}

	for i := range findings {
		env, ok := byKey[findings[i].Resource]
		if !ok {
			continue
		}
		for _, w := range lint.ParseWaivers(env.Metadata.Annotations) {
			if w.Code == findings[i].Code && w.Reason != "" {
				findings[i].Waived = true
				findings[i].WaiverReason = w.Reason
				break
			}
		}
	}

	return append(findings, malformed...)
}

// Package status defines the Kubernetes-shaped Condition/Status vocabulary
// shared by every resource kind. See docs/planning/02-architecture.md §3.7.
package status

import "time"

type ConditionType string

const (
	Ready         ConditionType = "Ready"
	Progressing   ConditionType = "Progressing"
	Degraded      ConditionType = "Degraded"
	DriftDetected ConditionType = "DriftDetected"
)

// TriState mirrors Kubernetes' condition status: True, False, or Unknown.
type TriState string

const (
	True    TriState = "True"
	False   TriState = "False"
	Unknown TriState = "Unknown"
)

// ReasonLineageNotConsumed is the informational reason recorded when a
// resource declares observers but its provider does not implement
// LineageAware. Never blocks Ready. See docs/planning/02-architecture.md §5.5.
const ReasonLineageNotConsumed = "LineageEndpointDeclaredNotConsumed"

type Condition struct {
	Type               ConditionType `json:"type"`
	Status             TriState      `json:"status"`
	Reason             string        `json:"reason,omitempty"`
	Message            string        `json:"message,omitempty"`
	LastTransitionTime time.Time     `json:"lastTransitionTime"`
}

type Status struct {
	Conditions         []Condition    `json:"conditions,omitempty"`
	ObservedGeneration int64          `json:"observedGeneration"`
	ProviderState      map[string]any `json:"providerState,omitempty"`
}

// SetCondition upserts a condition by type, updating LastTransitionTime only
// when the status value actually changes.
func (s *Status) SetCondition(c Condition, now time.Time) {
	for i, existing := range s.Conditions {
		// Conditions are keyed by Type alone (Kubernetes semantics): Reason
		// and Message are payload. Matching on Reason too would accumulate
		// duplicate conditions whenever the reason changes.
		if existing.Type == c.Type {
			if existing.Status == c.Status {
				c.LastTransitionTime = existing.LastTransitionTime
			} else {
				c.LastTransitionTime = now
			}
			s.Conditions[i] = c
			return
		}
	}
	c.LastTransitionTime = now
	s.Conditions = append(s.Conditions, c)
}

// Condition returns the first condition of the given type, if present.
func (s *Status) Condition(t ConditionType) (Condition, bool) {
	for _, c := range s.Conditions {
		if c.Type == t {
			return c, true
		}
	}
	return Condition{}, false
}

// IsReady reports whether the Ready condition is True.
func (s *Status) IsReady() bool {
	c, ok := s.Condition(Ready)
	return ok && c.Status == True
}

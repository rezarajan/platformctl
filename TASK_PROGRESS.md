# K1/K2 progress (docs/planning/08 §7.10 Stage K, ADR 033)

Worktree was 6 commits behind main; fast-forwarded to 234cabe (adds ADR 033,
Stage K, doc 08 §7.10) before starting — this is the commit that first
introduces the specs this task implements.

## K1 — label grammar and validation
- [x] internal/domain/resource/resource.go: ValidateLabelKey/ValidateLabelValue
      (Kubernetes label grammar), wired into Envelope.Validate()
- [x] internal/domain/resource/resource_test.go: positive+negative fixtures
- [x] docs/planning/03-resource-model-reference.md §2: additive labels grammar note
- [x] WIP commit (c6a78d2)

## K2 — selector vocabulary in policy
- [x] schemas/policy/v1alpha1/policy.json: selector shape (matchLabels/matchExpressions),
      matchEdge.selector{from,to}, match.selector (matchResource)
- [x] internal/domain/policy/policy.go: Selector/SelectorRequirement types + Selects
      predicate + Validate wiring; EdgeMatch.Selector; Match.Selector
- [x] internal/domain/policy/policy_test.go: selector unit tests
- [x] internal/application/policy/evaluator.go: Run signature gains
      labelScopedAccessEnabled bool; evaluateEdgeSelector; gate-off skip for
      selector-bearing rules (byte-identical pin)
- [x] update all Run/RunPlan call sites (policy_test.go, pack_test.go, cmd/platformctl/policy.go)
- [x] cmd/platformctl/main.go: register LabelScopedAccess gate (Alpha, disabled)
- [x] zero-trust pack: add who-may-wear-clearance-label rule (label integrity, match.selector)
- [x] internal/domain/status/catalog.go: catalog entry for new rule id
- [x] self-claim attack fixture (FAILS) + positive fixture (PASSES) — both polarities
      (cmd/platformctl/policy_labelscoped_test.go)
- [x] docs/planning/03-resource-model-reference.md §13.1: additive selector doc
- [x] doc 04 §12 + doc 08 §8: append LabelScopedAccess gate row
- [x] gate-off byte-identical pin test (graphscoped-test shape) — both
      internal/application/policy and cmd/platformctl levels
- [x] doc 08 §7.10 K1/K2 Done-notes appended; stage exit criteria 1-2 checked off
- [ ] WIP commit

## Verify
- [ ] gofmt
- [ ] go build ./...
- [ ] go vet -tags integration ./...
- [ ] go test ./... (full log, true-exit)
- [ ] golangci-lint v2.12.2

## Open items for orchestrator
- K3 (selector-scoped wide grants), K4 (mediation label-derived attributes),
  K5 (decision audit trail) are NOT implemented — out of scope per this
  task's K1/K2-only assignment and the strict K1->K2->{K3,K4}->K5
  sequencing. Stage K exit criteria 3-5 remain open; criterion 6 ("guards
  all of it") is satisfied only for the K1/K2 slice.
- No integration suite was run (fast-tier only per instructions); K4's
  eventual dual-runtime mediator work will need `just test-integration`.

# K1/K2 progress (docs/planning/08 §7.10 Stage K, ADR 033)

Worktree was 6 commits behind main; fast-forwarded to 234cabe (adds ADR 033,
Stage K, doc 08 §7.10) before starting — this is the commit that first
introduces the specs this task implements.

## K1 — label grammar and validation
- [ ] internal/domain/resource/resource.go: ValidateLabelKey/ValidateLabelValue
      (Kubernetes label grammar), wired into Envelope.Validate()
- [ ] internal/domain/resource/resource_test.go: positive+negative fixtures
- [ ] docs/planning/03-resource-model-reference.md §2: additive labels grammar note
- [ ] WIP commit

## K2 — selector vocabulary in policy
- [ ] schemas/policy/v1alpha1/policy.json: selector shape (matchLabels/matchExpressions),
      matchEdge.selector{from,to}, match.selector (matchResource)
- [ ] internal/domain/policy/policy.go: Selector/SelectorRequirement types + Selects
      predicate + Validate wiring; EdgeMatch.Selector; Match.Selector
- [ ] internal/domain/policy/policy_test.go: selector unit tests
- [ ] internal/application/policy/evaluator.go: Run signature gains
      labelScopedAccessEnabled bool; evaluateEdgeSelector; gate-off skip for
      selector-bearing rules (byte-identical pin)
- [ ] update all Run/RunPlan call sites (policy_test.go, pack_test.go, cmd/platformctl/policy.go)
- [ ] cmd/platformctl/main.go: register LabelScopedAccess gate (Alpha, disabled)
- [ ] zero-trust pack: add who-may-wear-this-label rule (label integrity, matchResource selector)
- [ ] internal/domain/status/catalog.go: catalog entry for new rule id
- [ ] self-claim attack fixture (FAILS) + positive fixture (PASSES) — both polarities
- [ ] docs/planning/03-resource-model-reference.md §13: additive selector doc
- [ ] doc 04 §12 + doc 08 §8: append LabelScopedAccess gate row
- [ ] gate-off byte-identical pin test (graphscoped-test shape)
- [ ] WIP commit

## Verify
- [ ] gofmt
- [ ] go build ./...
- [ ] go vet -tags integration ./...
- [ ] go test ./... (full log, true-exit)
- [ ] golangci-lint v2.12.2

## Open items for orchestrator
(none yet)

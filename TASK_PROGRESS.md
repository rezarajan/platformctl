# K5 progress

Task: docs/planning/08-production-readiness-plan.md §7.10 K5 — Decision
audit trail (ADR 033 decision 5). Stage K exit criterion 5.

## Setup
- Worktree fast-forwarded to main (0456b72) via `git merge main` — clean,
  no conflicts, zero unique prior commits in this worktree.
- Read: CLAUDE.md, ADR 033 (decision 5 + self-claim/label-integrity
  framing), ADR 021 (+ 2026-07-23 severing amendment — the reported
  in-between state), doc 08 §7.10 K1-K4 Done-notes (sequencing context;
  K4 is NOT done — K5 depends only on K2, per the doc's own dependency
  line, so proceeding without K4 is correct), internal/application/policy
  (evaluator.go's Run/RunPlan — every decision site; exemption.go), the
  I11 seam (cmd/platformctl/logging.go's newEngineLogger/textLineHandler,
  engine.go's logAction — the exact shape to mirror), the A7 harness
  (output_contract_harness_test.go, cliutil.WriteOutput/isStructured),
  internal/application/graphaccess (AccessGrant — the "permitted edge's
  justification may be a grant" mechanism), ADR 027's claims table.

## Design decisions
- Structured decision events: `enforcePolicies`/`enforcePlanPolicies`
  (cmd/platformctl/policy.go) — the ONLY call sites that ever invoke
  Run/RunPlan from validate/plan/apply/destroy — gained a `*slog.Logger`
  parameter and log every decision (deny/warn, exempted or not) via
  `logPolicyDecisions`, mirroring `Engine.logAction`'s exact shape
  (message = full prose, attrs = structured facts). Deliberately did NOT
  add logging inside `evaluatePolicies` itself, since `validate`'s RunE
  calls it a SECOND time (just to count warnings for the summary) after
  `loadAndValidate` already ran `enforcePolicies` once — logging there
  too would double-log every decision for `validate`.
- Logger plumbing: new `(*app).logger(w io.Writer) *slog.Logger` reuses
  `newEngineLogger` (the SAME factory `newEngine` uses for
  `Engine.Logger`). `loadAndValidate` widened to `(w io.Writer, path
  string)` — all ~12 call sites updated to pass `cmd.ErrOrStderr()`.
  `enforcePlanPolicies` widened similarly; `plan` builds a fresh logger
  (no Engine exists there), `apply`/`destroy` pass `eng.Logger` directly
  so policy decisions and reconciliation actions share one instance.
- Audit engine (`internal/application/policy/audit.go`, same package as
  evaluator.go so it reuses `crossDomainEdges`/`message` unexported
  helpers directly): `Audit()` covers TWO edge shapes — the
  `crossDomainEdges` set (Binding/connectionRef, EdgeKindBinding/
  EdgeKindConnection) AND `graphaccess.AccessGrants` (EdgeKindGrant) —
  because `crossDomainEdges` deliberately never covers spec.access
  grants, and ADR 033 explicitly names a grant as a valid permitted-edge
  justification distinct from "no rule denies it". Four-value closed
  Justification vocabulary: no-matching-deny, deny-rule, exemption,
  grant. Deny-wins resolution (`resolveVerdict`) sorts matching deny
  rules by id for determinism, returns the first unexempted one
  (Denied) else the first exempted one (Permitted/exemption) else the
  caller's own default.
- `policy audit` command: requires PolicyEngine gate (mirrors `policy
  test`) but — unlike `policy test` — never fails on a denied edge
  (report-only) and tolerates an empty/absent policy set (a valid,
  reportable "nothing governs this yet" state, deliberately differing
  from `policy test`'s hard refusal).
- ADR 027 claims table: added a new row for the governance/auditability
  claim (independent of the Layer 1/2 network-enforcement rows).
- README CLI-surface table: added a `policy audit` row (F-003 guard).
- No new lint/status/reason codes were introduced by this task, so no
  explain-catalog entries were needed.

## Status
- [x] Read all required docs/ADRs/precedent files.
- [x] Merged main, verified clean build.
- [x] Structured decision events: logger threading (root.go, backup.go,
      lint.go, policy.go) + `logPolicyDecisions`.
- [x] `internal/application/policy/audit.go` (Audit, EdgeAudit,
      Verdict, Justification, EdgeKind).
- [x] `cmd/platformctl/policy.go`'s `newPolicyAuditCmd` + output types.
- [x] `output_contract_harness_test.go` registered "policy audit"
      (A7 completeness guard).
- [x] Tests: internal/application/policy/audit_test.go (9 cases,
      including TestAuditEveryEdgeHasNameableJustification and
      TestAuditDeterministicOrdering), cmd/platformctl/policy_audit_test.go
      (4 CLI-level cases), cmd/platformctl/policy_decision_log_test.go
      (4 cases covering json/text formats, gate-off, and the RunPlan
      half).
- [x] README CLI-surface row + TestREADMECLISurfaceInSync green.
- [x] ADR 027 claims table row.
- [x] doc 08 K5 Done-note appended (additive) + Stage K exit criterion 5
      checkbox checked.
- [x] gofmt clean; `go build ./...` clean; `go vet` (both tag sets)
      clean; golangci-lint v2.12.2 clean (0 issues); unfiltered
      `go test ./...` true-exit=0.
- [x] docs/reference regeneration not needed (no schema/Kind change —
      TestGeneratedReferenceInSync passed unchanged).
- [ ] Final commit (unsigned, per coordinator instruction).

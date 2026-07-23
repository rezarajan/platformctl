# L1 progress (docs/planning/08 §7.11 L1, ADR 034)

Worktree: agent-a43a587a83315c4fd. Work ONLY here, never `cd` into it —
use absolute paths / `git -C`.

## Design decisions (locked in, see code comments for full justification)

- Gate: `MediatedTransport` (Alpha, disabled) — registered in
  cmd/platformctl/main.go next to `MediatedConnections`.
- Schema surface: `spec.transport` (enum: unset | "direct") added to
  `Binding` and `Connection` — the two kinds that declare outbound edges
  reachable by L1's two concrete resolution surfaces (SchemaRegistryURL via
  a Binding, KafkaBootstrapServers via the Binding(s) feeding a Connect
  worker). Read generically off env.Spec by a small `transportDirect`
  helper in engine (not via full domain decode) since both Kinds share the
  same field shape.
- Port extension: `internal/ports/mediation.AddressEdge` +
  `AddressResolver` (new file address.go, additive) — a SEPARATE optional
  capability interface, not a new method on MediationProvider itself, so
  the already-shipped openziti adapter needs zero changes (mirrors
  reconciler.MediationCapableProvider sitting beside
  ConnectionCapableProvider).
- Engine seam: new `Engine.Mediation mediation.AddressResolver` field (nil
  disables, mirrors SecretStore's "nil disables" convention). Substitution
  wired at exactly the two named call sites (resolveSchemaRegistryURL,
  resolveKafkaBootstrapServers) — not a generic Facts-map-wide rewrite —
  because those are the two surfaces the Do list names and the only ones
  with a well-defined single edge per request today.
- compatibility.ResolveKafkaBootstrapTarget: new additive function
  alongside ResolveKafkaBootstrapAddress, also returning the broker
  Provider's resource.Key + contributing Binding keys (needed to compute
  the AddressEdge and check transport-direct).

## Do-list checklist

- [x] Gate registered
- [x] Schema + doc03 (same commit as schema)
- [x] Port extension (mediation.AddressEdge/AddressResolver)
- [x] Engine seam wired (SchemaRegistryURL, KafkaBootstrapServers)
- [x] Fake AddressResolver + engine-level tests (a-d)
- [x] Verify: gofmt, go build, go vet -tags integration, go test ./...,
      golangci-lint
- [x] Doc 08 L1 Done-note (additive)
- [x] Commit

## Verification log

See /home/cascadura/git/platformctl/.claude/worktrees/agent-a43a587a83315c4fd/test-output.log
(gofmt clean; go build ./... clean; go vet ./... and -tags integration
clean; go test ./... true-exit=0, 62/62 packages ok; golangci-lint v2.12.2
0 issues; git diff --stat internal/adapters/providers/ empty).

## Merge note

This worktree branched at 7072b2d, before ADR 034 / doc08 Stage L / the
MediationProvider port itself landed on main via other agents' parallel
work (H7-K2, H9, H10). Committing L1 against that stale base would have
been incoherent (referencing an ADR that didn't exist in-branch, no doc08
L1 task text to satisfy). Resolved by: `git stash -u` my WIP, `git merge
main` (clean, no conflicts — my branch had no commits of its own since
the earlier GPG failure below), `git stash pop` (2 conflicts, both in
docs/planning gate tables where upstream and I both appended a row next
to each other — resolved by keeping both rows, additive on both sides).
Full verification re-run clean post-merge.

## GPG

Every `git commit` attempt in this environment fails with `gpg: signing
failed: Timeout` (no interactive pinentry available). Did not use
--no-gpg-sign (not authorized). Final commit left staged with
COMMIT_MSG.txt if this recurs — see the final report.

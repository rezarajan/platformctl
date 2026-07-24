# L2 progress (docs/planning/08 §7.11 "Platform-owned fabric")

## Done so far

1. Merged main (L1, K4, H10, pkcs7 swap) into this worktree.
2. Live-verified (docker pull + entrypoint extraction) the pinned
   ziti-controller:1.5.14 image: the containerized entrypoint has NO
   file-based ZITI_USER/ZITI_PWD path (unlike the router's enrollment
   JWT) — confirmed by running a controller with a FileMount-only
   credential, which refused bootstrap. This grounds fabric.go's admin
   credential design (Env on first bootstrap only, then durable
   file-resident storage inside the controller's own persisted volume for
   every later read).
3. `internal/ports/mediation/fabric.go`: new `FabricRequest`/`FabricState`/
   `FabricProvisioner` port — technology-silent, mirrors `AddressResolver`.
4. `internal/adapters/providers/openziti/fabric.go`: concrete
   `FabricProvisioner`, reusing instance.go/client.go's H10 pinned-CA
   client and bootstrap/enroll/settle mechanics. Engine-minted admin
   credential (crypto/rand), Env only on first bootstrap, persisted at
   `/ziti-controller/.admin-credential` inside the controller's volume for
   every later EnsureFabric/DestroyFabric call to read back.
5. `internal/application/engine/mediation_fabric.go`: engine orchestration
   — `Engine.Fabric` field, `ensureMediationFabric` (Apply hook, before the
   main reconcile loop), `maybeDestroyMediationFabric` (Destroy hook, after
   the destroy loop), `anyMediatedEdgeDeclared` (scans every Binding/
   Connection for non-`direct` transport), `fabricRuntimeSource` (picks
   runtime type/config from the first Provider, sorted by key).
6. **Design pivot, found live by test**: originally recorded the fabric as
   a normal `state.Resources[key]` entry (for gc visibility). Broke
   immediately: `plan.Compute`'s `computeApplyDeletes` sweeps EVERY
   `state.Resources` entry absent from the current manifest's envelopes
   and marks it `ActionDelete` — since nothing ever declares a
   "MediationFabric" Kind in a manifest, the very next `apply` of ANY
   unrelated manifest would try to destroy the fabric via the normal
   resolveRequest/Reconcile path (and fail, since it has no providerRef).
   Fixed by adding a dedicated `state.State.MediationFabric
   *MediationFabricState` field, entirely outside `Resources`/
   `RawResources` and the orphan sweep. `cmd/platformctl/gc.go`'s
   `accounted` closure special-cases `kind == "MediationFabric"` against
   this new field so `gc plan` still recognizes the fabric's labeled
   objects as owned.
7. `cmd/platformctl/root.go`: wired `Engine.Fabric =
   openziti.NewFabricProvisioner()` unconditionally (mirrors Mediation's
   own nil-disables-internally convention).
8. Fast-tier tests (`internal/application/engine/mediation_fabric_test.go`):
   stand-up-once, gate-off no-op, no-Fabric no-op, no-mediated-edge no-op,
   idempotent-across-reapply (state stable, no credential keys), destroy
   tears down when no mediated edge remains, destroy KEEPS when a mediated
   edge remains. All green.

## Verification (all green, post state-field pivot)

- [x] gofmt clean; `go build ./...` clean.
- [x] `go vet ./...` and `go vet -tags integration ./...` both clean.
- [x] `go test ./...` — true-exit=0 (gotest_final2.log).
- [x] golangci-lint v2.12.2 — 0 issues.
- [x] Live: `TestFabricProvisionerLiveDocker` (bounded flock, real Docker,
      pinned images) — PASS 12.7s. EnsureFabric x2 idempotent (same
      controller container id, same router id, credential read back from
      volume), DestroyFabric removes all + idempotent no-op. Zero residue
      confirmed. Registered as `mediation-fabric` suite in test-impact.sh.
- [x] L2 Done-note appended to doc 08 (additive, +108/-0), covering the
      openziti-convergence decision (additive; L3 precondition) and the
      state-field pivot.

## DONE — L2 complete, ready for final commit.

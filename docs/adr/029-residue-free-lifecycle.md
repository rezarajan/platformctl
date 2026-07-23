# ADR 029 — Residue-free lifecycle: removal is a contract, cleanup is a component

**Status:** accepted (2026-07-23). **Prompted by:** the owner-requested
residue audit after the wave-5 sweep, which found live strays on both
runtimes and traced each to a *class*, not a one-off (doc 11,
2026-07-23 "RESIDUE AUDIT" entry; commit b74fdb9 holds the point
fixes).

## The evidence

Three classes were found live:

1. **A stranded Kubernetes namespace** (`datascape-netpol-enforce-in`,
   its listener still running an hour after the suite passed). The
   test's cleanup removed only the namespaces; `RemoveNetwork`'s
   holds-workloads refusal (the ca9d719 safety — itself correct) was
   swallowed by `_ =`. The strand recurred on *every* run of the skip
   path.
2. **A stray unmanaged container blocking a network's removal**
   (`ziti-canary`): a raw `docker run` fixture listed in an
   `rt.Remove` loop — which refuses unmanaged containers *by design* —
   with the refusal swallowed.
3. **3853 dangling anonymous volumes (8.4GB)**: Docker's
   `ContainerRemove` was never passed `RemoveVolumes`, so every managed
   container removal since the project began silently leaked its
   image-declared anonymous volumes.

Class 3 is the decisive one: it was not a test bug. It was an
**unstated port contract** — nothing said what "removed" must mean, so
an adapter could satisfy every existing test while leaking on every
call.

## What already exists (and why it did not catch this)

The system-level residue authority already exists: every managed object
carries the `io.datascape.managed-by` ownership labels, and `gc`
(cmd/platformctl/gc.go) discovers orphans by diffing `ListManaged*`
against state. It did not catch these strays because all three classes
sit exactly in its blind spots: *unlabeled* raw fixtures (invisible to
label-scoped listing, by design), *labeled objects whose removal was
refused* (still accounted in no state, but the suite that owned them
had already passed), and *anonymous volumes* (unlabeled by Docker's
own construction). The conclusion is not "gc needs to see more" — its
label scope is a safety boundary worth keeping — but that the two ends
it cannot cover need their own structure.

## Decision

Three parts, one per blind spot:

### 1. Removal is a port contract, conformance-enforced

`ContainerRuntime.Remove` (and every `Remove*`) means: **the named
object and everything the adapter derived from it are gone when the
call returns** — anonymous volumes on Docker; per-container Secrets,
Services, and PDBs on Kubernetes (which already did this via
`removeCommonContainerObjects` + synchronous foreground deletion). The
contract is stated on the port and enforced where adapters are already
proven: the Docker adapter gains an integration test that creates a
container from a volume-declaring image and asserts the daemon-wide
volume count returns to its pre-test value after `Remove`. A future
adapter that leaks derived residue fails its suite, not a
months-later audit.

### 2. Test cleanup is a shared component, not 31 hand-rolled closures

`internal/testkit` provides the one janitor every integration test
composes instead of re-deriving the rules the audit recovered by
autopsy:

- **Order**: workloads → volumes → networks/namespaces (the ca9d719
  refusal makes any other order strand).
- **Two removal channels**: managed objects through the runtime port;
  declared *raw* fixtures through `docker rm -f -v` — never the
  port's `Remove`, which refuses them by design.
- **Two loudness modes**: the pre-test invocation is silent (absent
  objects are expected); the `t.Cleanup` invocation reports every
  failure through `t.Errorf` — a cleanup that cannot clean is a test
  failure, because a swallowed refusal is precisely how class 1 and 2
  recurred invisibly.

Adoption is incremental (task J2, doc 08): the three tests fixed by
the audit adopt it in this ADR's commit as the exemplars; new
integration tests must use it; existing ones migrate as they are
touched.

### 3. gc stays scoped; raw fixtures stay declared

`gc`'s label boundary is not widened. Unlabeled objects a test creates
are the test's own liability, and the janitor's `RawContainers`/
`RawNetworks` fields are the single place that liability is declared —
greppable, ordered, and loud, instead of scattered `exec.Command`
lines.

## Consequences

- The RemoveVolumes class can never silently reopen: the contract test
  fails the adapter, and `golangci`'s errcheck posture plus the loud
  janitor surface refusals at the point of cleanup.
- Test authors state *what* they created; the janitor owns *how and in
  what order* it dies.
- `Remove`'s "refuse unmanaged" and `RemoveNetwork`'s "refuse while
  occupied" semantics are unchanged — they are safety features this
  ADR builds on, not bugs it works around.

## References

Doc 11 (2026-07-23 residue audit), b74fdb9 (point fixes), ADR 013
(safety posture), ADR 016 (provider contract), doc 06 §10 (integration
economy), doc 08 J2 (janitor adoption sweep).

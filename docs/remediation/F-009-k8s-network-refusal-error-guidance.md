# F-009: Kubernetes namespace refusal surfaces only at apply time with no remedy in the message

**Severity:** Low (safe-by-default refusal is correct; the failure mode is
operator confusion, not damage).
**Status:** RESOLVED (2026-07-17). Verified live against a real cluster:
EnsureNetwork("default") now refuses with a message naming
spec.runtime.network as the remedy.

## Claim audited

Cross-Runtime Portability section: "EnsureNetwork ensures a Namespace of
that name exists" with the ownership policy ("refuses unmanaged same-name
objects"). The policy is correctly implemented — this finding is about the
failure UX for an inevitable collision class.

## Evidence

`internal/adapters/runtime/kubernetes/kubernetes.go`, `EnsureNetwork`:

```go
if ns.Labels[runtimeport.LabelManagedBy] != runtimeport.ManagedByValue {
    return fmt.Errorf("namespace %q exists but is not managed by platformctl; refusing to reuse it", spec.Name)
}
```

Every cluster has `default`, `kube-system`, `kube-public`,
`kube-node-lease` pre-existing and unmanaged. A manifest with
`runtime: {type: kubernetes, network: default}` (a natural first guess for
a Docker user, whose `default` bridge network exists) validates cleanly and
fails mid-apply with a message that names neither the manifest field that
caused it nor the remedy.

## Root cause

The refusal message was written for the Docker adapter's collision case
(another checkout's leftovers) and reused verbatim; on Kubernetes the
overwhelmingly common collision is a system namespace, where "refusing to
reuse it" without "change spec.runtime.network" reads as a platform bug.

## Required behavior

Adapter-local error-message improvement (no new validation seam — that
would be an architectural decision, out of scope):

```go
return fmt.Errorf("namespace %q exists but is not managed by platformctl; refusing to reuse it — choose a dedicated name via the Provider's spec.runtime.network (every object of one platform joins that namespace)", spec.Name)
```

Apply the same message shape to `EnsureVolume`'s and `EnsureContainer`'s
unmanaged-refusal errors in the same file if they can be reached through a
namespace the user picked (volume/deployment collisions inside a *managed*
namespace keep the existing shorter message).

## Exact files and symbols

- `internal/adapters/runtime/kubernetes/kubernetes.go`: `EnsureNetwork`
  (required), `RemoveNetwork` (same message shape, optional).

## Implementation constraints

- Message text only; refusal semantics must not change.
- Do not special-case system namespace names in code (a denylist would rot;
  the ownership label check already covers them).

## Tests / validation commands

Unit: none needed (message text). Integration (optional, cluster present):

```
PLATFORMCTL_REQUIRE_K8S=1 go test -tags integration ./internal/adapters/runtime/kubernetes/ -run TestConformance
```

plus a one-off `EnsureNetwork(ctx, NetworkSpec{Name: "default"})` in a
scratch test asserting the message contains "spec.runtime.network".

## Dependencies / ordering

None.

## Risk

Minimal.

## Escalation conditions

Escalate if reviewers want validate-time rejection of reserved namespace
names instead — that requires a per-runtime config-validation seam that
does not exist and must not be invented ad hoc by the implementer.

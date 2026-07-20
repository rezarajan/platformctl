## Kubernetes CI Integration Test Failure

3m 45s
Run go test -tags integration -timeout 1200s \
  go test -tags integration -timeout 1200s \
    ./internal/adapters/runtime/kubernetes/... \
    ./internal/adapters/secrets/kubernetes/...
  go test -tags integration -timeout 1200s -run Kubernetes ./cmd/platformctl/...
  shell: /usr/bin/bash -e {0}
  env:
    PLATFORMCTL_KUBECONFIG: /home/runner/work/_temp/platformctl.kubeconfig
    KUBECONFIG: /home/runner/work/_temp/platformctl.kubeconfig
    PLATFORMCTL_REQUIRE_K8S: 1
  
E0720 09:30:55.970767   11959 portforward.go:351] "Unhandled Error" err="error creating error stream for port 33881 -> 80: Timeout occurred" logger="UnhandledError"
E0720 09:30:55.970769   11959 portforward.go:351] "Unhandled Error" err="error creating error stream for port 33881 -> 80: Timeout occurred" logger="UnhandledError"
E0720 09:30:59.834227   11959 portforward.go:351] "Unhandled Error" err="error creating error stream for port 34545 -> 8080: Timeout occurred" logger="UnhandledError"
E0720 09:30:59.834227   11959 portforward.go:351] "Unhandled Error" err="error creating error stream for port 34545 -> 8080: Timeout occurred" logger="UnhandledError"
warning: namespace "datascape-netpol-none-test" uses networkPolicy: none — no isolation boundary is provisioned; every pod in the cluster can reach it unless something else in the cluster restricts it
E0720 09:31:46.208415   11959 portforward.go:351] "Unhandled Error" err="error creating error stream for port 40559 -> 80: Timeout occurred" logger="UnhandledError"
--- FAIL: TestEnsureReachable (68.03s)
    --- FAIL: TestEnsureReachable/node_port_mode_is_reachable_and_observed_by_inspect (62.21s)
        reachability_integration_test.go:138: EnsureReachable: service "datascape-reach-np" (access mode "node-port") did not become dialable for port 80 within 1m0s
FAIL
FAIL	github.com/rezarajan/platformctl/internal/adapters/runtime/kubernetes	221.617s
ok  	github.com/rezarajan/platformctl/internal/adapters/secrets/kubernetes	0.049s
FAIL
Error: Process completed with exit code 1.

## Root Cause Analysis (original — DISPROVEN, see correction below)

The original analysis attributed the failure to the runtime advertising a kind
node `InternalIP` (`172.20.0.2:<nodePort>`) that is supposedly not
host-reachable, and proposed making NodePort resolution "kind-aware" (refuse or
fall back to port-forward on kind). **Direct reproduction disproved this.**

## Corrected Root Cause (validated)

The kind node `InternalIP` **is** host-reachable. On the same kind cluster the
original analysis used, a NodePort Service is dialable from the host:

```text
$ curl --max-time 6 http://172.20.0.2:32548/      # node InternalIP:nodePort
http_code=200 time=0.000383s
```

The decisive difference is **the namespace's default-deny NetworkPolicy**, not
the node address. `EnsureNetwork` provisions a `default-deny-ingress` +
`allow-same-namespace` pair (docs/planning/08 B7). The reachability test creates
its namespace through `EnsureNetwork`, so that boundary is active. NodePort
traffic reaches the backend pod SNAT'd to the node address — a source that is
not a pod in the namespace — so the `allow-same-namespace` rule never matches it
and the default-deny drops it. Applying exactly that policy pair flips the same
dial from working to timing out:

```text
# before applying the policy pair:
$ curl --max-time 5 http://172.20.0.2:32548/   ->  http_code=200
# after applying default-deny + allow-same-namespace:
$ curl --max-time 5 http://172.20.0.2:32548/   ->  curl: (28) timeout
```

`curl: (28)` timeout is precisely the CI symptom. It is reproduced with the
CNI kind ships by default (`kindnet`), which enforces NetworkPolicy in the
version under test. `port-forward` mode passes because the kubelet stream it
uses to reach the pod bypasses NetworkPolicy; `node-port`/`load-balancer` route
external traffic through the node/LB and are subject to it.

This is a **product bug, environment-independent**: platformctl's own isolation
policy blocks the very external traffic that the `node-port`/`load-balancer`
access modes exist to admit. It is not a kind-vs-minikube quirk (it reproduces
identically wherever a policy-enforcing CNI is present) and needs no
kind-detection, `extraPortMappings`, or NodePort-resolution changes.

## Remediation (applied)

`node-port`/`load-balancer` containers now get a per-container NetworkPolicy
(`datascape-allow-external-<name>`) that admits ingress from any source, but
only to their exposed ports — opening the default-deny boundary exactly where
external exposure was requested, leaving every other port default-denied.

- `internal/adapters/runtime/kubernetes/convert.go`: `buildExternalIngressPolicy`.
- `internal/adapters/runtime/kubernetes/kubernetes.go`: `ensureExternalIngressPolicy`,
  wired into `EnsureContainer`; deleted in `Remove` (by name — the minimal RBAC
  role grants `delete` but not `list` on networkpolicies). The hole is only
  provisioned when the namespace actually carries the default-deny wall, so an
  `IsolationNone` namespace is never inadvertently restricted.

Validated on the live kind cluster: `TestEnsureReachable` (all three subtests,
including `node_port_mode_is_reachable_and_observed_by_inspect`) and
`TestEnsureNetworkProvisionsIsolationBoundary` pass.

## Follow-up: two further failures (the netpol fix held; these were next in line)

With the reachability fix in place the `internal/adapters/runtime/kubernetes`
package now passes (`ok ... 148s`, `TestEnsureReachable` gone). The next CI run
surfaced two *different*, unrelated failures in `cmd/platformctl`:

### 1. CDC example: `minikube image load: exit status 14` — cluster does not exist

`TestCDCAttendanceExampleOnKubernetes` built the s3sink connector image and
loaded it onto the node with a hardcoded `minikube image load`. **CI runs on
kind** (`helm/kind-action`, `.github/workflows/ci.yml`), where no `minikube`
cluster exists, so the load aborts (`MK_USAGE: cluster "minikube" does not
exist`) before the test does anything. Purely a test-harness assumption, not a
product bug.

- Fix: `loadImageIntoCluster` dispatches on whichever CLI is actually present —
  `kind load docker-image` when `kind get clusters` reports one (CI), falling
  back to `minikube image load` for local runs.

### 2. Lakehouse example: `external-orders-db missing after destroy`

`TestLakehouseExampleOnKubernetes` stands up `external-orders-db` as a real but
*unmanaged-by-platformctl* Deployment in the shared namespace, then asserts
`destroy` leaves it untouched. It was gone afterward.

Root cause — a **product bug on the Kubernetes runtime**: every provider's
`Destroy` best-effort-calls `RemoveNetwork(network(cfg))`, and in this scenario
every provider *shares one namespace*. On Docker this is safe: `NetworkRemove`
never deletes containers and *refuses* ("network has active endpoints") while
any container is still attached, so the shared network outlives all but the
last member and the error is harmlessly ignored. On Kubernetes, `RemoveNetwork`
deleted the whole **namespace**, cascading to every object in it — so the first
provider destroyed wiped its siblings *and* the unmanaged `external-orders-db`.

- Fix (`internal/adapters/runtime/kubernetes/kubernetes.go`, `RemoveNetwork`):
  mirror Docker's "in use" refusal. A Deployment is the container analog, so
  refuse to delete the namespace while any Deployment still lives there; delete
  only once it has been emptied of workloads. `Remove` already blocks until its
  Deployment is fully gone, so the last member's namespace is still reclaimed.
- Per the doc 09 §3-F6 ratchet, a bug found only by live testing must land with
  a contract-level reproduction. This class *is* expressible at the port level
  (Docker refuses the same way — "network has active endpoints"), so it is
  pinned in the shared conformance suite as
  `RemoveNetwork_refuses_while_container_attached` — passed by the fake, Docker
  (validated live), and Kubernetes adapters — with the contract stated on
  `ContainerRuntime.RemoveNetwork`. The fake's `RemoveNetwork` was made honest
  (it previously deleted a network out from under an attached container) so the
  strict interpreter matches the real runtimes. A k8s-specific fast unit test
  (`removenetwork_test.go`, fake clientset) additionally pins the namespace-
  cascade case in `go test ./...`, where no live cluster runs. Recorded in
  docs/planning/07's per-runtime findings ledger. The lakehouse test's cleanup
  now tears down `external-orders-db` explicitly so the namespace can still be
  reclaimed under the stricter semantics.

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

# M7: Rebuild the flagship example — planes, HA, both runtimes — progress

Task: docs/planning/08-production-readiness-plan.md §7 M7 (ADR 035).
Protocol: doc 08 §2.1.

## Status: Docker leg COMPLETE and proven live; Kubernetes leg remaining

The original blocker ("platformctl cannot read a manifest set spread
across subdirectories") is RESOLVED, and the whole Docker path is proven
live end-to-end. What was done, in order:

1. **Include-members loader** (commit `feat(manifest): explicit
   include-members project composition`): a project's `datascape.yaml`
   declares `spec.resources` — the Helm/Kustomize include pattern. The
   root lists its plane directories; each plane's own `datascape.yaml`
   lists its files. No auto-discovery, no magic exclusions. Legacy flat
   layout (no `spec.resources`) is byte-identical to before.

2. **Example restructured** to the pattern: root `datascape.yaml` +
   per-plane `datascape.yaml` (platform/sources/cdc/sinks/catalog/query/
   lineage); k8s variant mirrors it. Guard test un-skipped.

3. **Live Docker proof** (all against a real `platformctl apply`):
   - 26 resources → Ready, zero-trust default-on with NO `--feature-gates`
     for it (only HighAvailability + TrinoProvider, neither about ZT).
   - `test-zero-trust.sh`: all 5 checks pass (mediated CDC RUNNING,
     unauthorized-identity dial refused with enrollment gated so it can't
     false-pass, dark-DB posture). Script had real bugs (f-string syntax,
     flat-network assumption, ziti-identity leak) — all fixed.
   - HA: 3 redpanda brokers, 4 MinIO nodes, 2 Trino workers; killing the
     data-topic LEADER broker leaves the orders CDC path serving and a
     post-kill insert is captured end-to-end.
   - Dagster endpoints (Trino pinned :16900, Kafka bootstrap, S3 lake) all
     reachable/discoverable via `platformctl inventory`.

4. **Real HA defect found and fixed by the kill-test** (commit
   `fix(debezium): replicate CDC + Connect internal topics ...`): Debezium
   hardcoded RF=1 on both the per-table CDC data topics and the Connect
   worker's internal state topics. Now derived from the target
   EventStream's declared replication; the internal-topic vars had to be
   `CONNECT_`-prefixed (the image drops the bare form). Verified live:
   topics come up REPLICAS [0 1 2].

## Remaining for full M7 acceptance

- **Kubernetes leg** (user-gated): `cp k8s/datascape.yaml datascape.yaml`
  then apply on a cluster; run the zero-trust + HA proofs there. Not run
  here — the dev cluster needs its Calico CNI recreated (a user-only step)
  and this session does not mint K8s tokens. The manifests validate for
  `runtime: kubernetes`; the swap is documented in README#kubernetes-variant.
- Once the K8s leg is proven, M7's "on BOTH runtimes" accept criterion
  (doc 08 §7 M7) is fully met.

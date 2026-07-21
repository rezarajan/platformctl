# SecretReference

`datascape.io/v1alpha1`

A named reference to secret material resolved through a backend at reconcile time. The schema has no field that could carry a secret value: manifests declare names and keys only (FR-9).

After apply, state stores a one-way fingerprint of the resolved material, not the values. Drift/status reports `SecretChanged` when the backend now resolves to different material; apply records the new fingerprint and re-reconciles dependents that reference the secret. Providers own the backing-system rotation; the Docker MySQL/MariaDB provider updates the root account to match the new resolved value, and the Docker Postgres provider does the same for its superuser role.

Because state never stores plaintext old values, automatic admin-password rotation depends on either the new secret already authenticating or the managed runtime still exposing the previous bootstrap environment. If both are lost or manually corrupted, platformctl reports that manual credential recovery is required.

For external systems, changing a SecretReference only changes the credentials platformctl passes to dependents. The external system itself must already be updated out-of-band to accept the new credentials.

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.backend` | `env` \| `file` \| `kubernetes` \| `vault` | yes | env and file are implemented; kubernetes is implemented behind the KubernetesSecretBackend gate (Beta, enabled by default); vault is implemented behind the VaultSecretBackend gate (Alpha, disabled). |
| `spec.keys` | array of string | yes | Logical key names; backend-specific configuration maps them to storage (e.g. env: DATASCAPE_SECRET_<NAME>_<KEY>; kubernetes: Secret data keys). |
| `spec.kubernetes` | object | no | backend: kubernetes only. Overrides which Kubernetes Secret object this reference resolves against; both fields are optional. |
| `spec.kubernetes.name` | string | no | Kubernetes Secret name. Defaults to metadata.name. |
| `spec.kubernetes.namespace` | string | no | Kubernetes namespace. Defaults to metadata.namespace (the same Datascape-namespace-doubles-as-Kubernetes-namespace convention the kubernetes runtime uses for Providers). |

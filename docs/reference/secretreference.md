# SecretReference

`datascape.io/v1alpha1`

A named reference to secret material resolved through a backend at reconcile time. The schema has no field that could carry a secret value: manifests declare names and keys only (FR-9).

| Field | Type | Required | Description |
|---|---|---|---|
| `metadata.name` | string | yes | Unique per Kind within a manifest set. |
| `metadata.observers[].name` | string | no | Provider names resolved to LineageEndpoints and forwarded when this resource's provider is LineageAware. |
| `spec.backend` | `env` \| `file` \| `kubernetes` \| `vault` | yes | env and file are implemented; kubernetes accepted for forward compatibility (resolution fails fast); vault lands with the VaultSecretBackend gate. |
| `spec.keys` | array of string | yes | Logical key names; backend-specific configuration maps them to storage (e.g. env: DATASCAPE_SECRET_<NAME>_<KEY>). |

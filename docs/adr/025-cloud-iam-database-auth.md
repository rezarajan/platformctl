# ADR 025 — Cloud IAM database authentication: out of scope for now, composable today via auth proxies

**Status:** accepted (2026-07-22). **Prompted by:** the 2026-07
production review (docs/planning/11, Phase A finding 3): connecting to
cloud-managed databases was named as the owner's baseline production
scenario; transport TLS is sequenced (doc 08 I2), but IAM/token-based
authentication (AWS IAM DB auth, GCP Cloud SQL IAM auth, Azure AD /
Entra auth) had no recorded decision at all — not even a scope
statement.

## Context

All three major clouds offer password-less database auth where the
"password" is a short-lived, IAM-derived token (15 min for AWS RDS,
~60 min for GCP/Azure access tokens). Anything that holds a database
connection longer than the token's life needs an in-place refresher; a
long-running CDC connector (Debezium) is exactly that.

## Decision

**IAM/token DB auth is out of scope for Datascape itself, for now.**
Reasons, in order:

1. **It requires a resident credential refresher.** A token that expires
   in 15 minutes cannot be resolved once at `apply` time (the ADR 013
   secret model: resolve at reconcile, hand to the workload, never
   persist). Keeping connectors authenticated means a process that
   continuously mints tokens — a daemon. Doc 09 §4.1's one-shot posture
   is a settled decision; a resident refresher inside platformctl would
   quietly reverse it.
2. **The clouds already ship the refresher, as a proxy.** Cloud SQL Auth
   Proxy, AWS RDS IAM helper sidecars, and Azure's equivalent all run as
   a local process that handles IAM auth + TLS outbound and presents a
   plain local socket. That shape **composes with the existing model
   today**: run the proxy, declare an External Connection at the proxy's
   address, point the Source's connectionRef at it. See doc 03 §8.2's
   cloud-managed-database walkthrough.
3. **SDK surface area.** First-class IAM auth means per-cloud SDK
   dependencies and credential-chain semantics inside providers — an
   ADR 003-class dependency decision with real weight, not to be made as
   a side effect of a connectivity feature.

## What IS in scope (now or sequenced)

- Transport TLS to any TLS-requiring database: doc 08 **I2**
  (`sslmode`/`tls` modes + CA bundles via SecretReference).
- Static-credential auth (username/password, including cloud-managed DBs
  with built-in auth): works today via SecretReference.
- The auth-proxy topology, documented as the supported IAM pattern
  (doc 03 §8.2): the proxy is the operator's process (or a container
  they declare like any other), Datascape consumes its socket as an
  External Connection.

## Future shape (recorded, not designed)

A `cloudsql-auth-proxy`-class **Provider** (a managed container running
the cloud's own proxy image, credentials via SecretReference holding a
service-account key / IAM role material) would make the proxy itself a
first-class, reconciled resource on the Connection seam — the tunnel
provider (ADR 023) is the structural precedent. If real usage demands
it, that is its own ADR + Stage I task with the per-cloud dependency
argument made explicitly. Revisit when a user cannot run the proxy
themselves (e.g. fully-managed CI environments).

## References

Doc 11 Phase A (finding 3); doc 08 I2; ADR 013 (secret model), 003
(dependency policy), 023 (provider-on-the-Connection-seam precedent);
doc 09 §4.1 (one-shot posture); doc 03 §8.2 (the walkthrough this ADR
authorizes).

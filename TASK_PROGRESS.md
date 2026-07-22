# TASK_PROGRESS — I2: outbound database TLS

Branch: worktree-agent-a1af5154b85ced63e. Spec: docs/planning/08 §7.8 I2.

## Done

- [x] `git merge main --no-edit` (fast-forward, e9189e6).
- [x] Domain: `internal/domain/connection` gains `TLS.Mode`/`TLS.CASecretRef`
      + `TLSModeRequire/VerifyCA/VerifyFull`; external validation replaced
      (mode-shape, not the managed exactly-one-of); managed validation
      unchanged in effect, now rejects Mode/CASecretRef. Pure-stdlib
      `connection.ClientTLSConfig` (crypto/tls config builder for
      require/verify-ca/verify-full) added in new `tls.go`. Unit tests
      added/updated in `connection_test.go`.
- [x] Schema: `schemas/v1alpha1/connection.json` tls block gains
      `mode`/`caSecretRef`; doc 03 §8.2.4 updated in the same commit
      (additive, guard-compliant).
- [x] Status: two new reasons (`ReasonDatabaseTLSCAInvalid`,
      `ReasonDatabaseTLSVerifyFailed`) + catalog entries.
- [x] Gate: `ExternalDatabaseTLS` (Alpha, enabled) registered in
      cmd/platformctl/main.go; doc 04 §12 row added. Engine-level check
      added (`externalDatabaseTLSGate` in engine.go, wired into
      `reconcileExternal` + the `ExternalNoProvider` probe hook in
      kind_handler.go) since a bare external Connection never reaches
      `resolveRequest` (no providerRef) — this is its equivalent
      chokepoint, mirroring TLSTermination's own resolveRequest check.
- [x] providerkit seam: new `tls.go` — `DatabaseTLS`, `ResolveDatabaseTLS`,
      `CATrustFileMounts`/`CAFilePath` (CA bundle mounted into a
      Connect-worker-style container, keyed by secretRef name — see that
      file's doc comment for why worker-level, not binding-level), and
      `VerifyDatabaseConnection` (the shared Go-side preflight dial,
      replacing byte-identical duplicates that existed in both
      debezium.go and jdbcsink.go).
- [x] Consumers wired:
      - postgres/sql.go `connStringAddr` — sslmode-aware (always nil today;
        this provider only manages self-hosted Postgres, no external
        Connection to resolve a posture from — see its doc comment for the
        explicit scope call).
      - mysql/sql.go `dsnAddr` — same treatment (go-sql-driver
        RegisterTLSConfig + `tls=` param), same "always nil today" scope.
      - debezium.go — worker mounts CA trust files; preflight uses
        providerkit.VerifyDatabaseConnection; connector properties set
        `database.sslmode`/`database.sslrootcert` (postgres, full support)
        and `database.ssl.mode` (mysql/mariadb — CA-truststore support
        explicitly out of scope, documented inline, mirrors ADR 025's
        posture on IAM auth).
      - jdbcsink.go — same worker CA mount + shared preflight; JDBC URL
        gains `sslmode`/`sslrootcert` (postgres) and `sslMode`/
        `trustCertificateKeyStoreType=PEM`/`trustCertificateKeyStoreUrl`
        (mysql/mariadb, full support via Connector/J, unlike Debezium's own
        binlog client).
- [x] `go build ./...` clean, `gofmt -l .` clean.
- [x] WIP checkpoint: staged + COMMIT_MSG.txt (GPG timed out).

## Done (continued)

- [x] Unit tests: DSN/property construction for all 4 consumers, all modes —
      postgres sql_test.go, mysql sql_test.go, debezium tls_test.go
      (applyTLSConfig + end-to-end buildDesiredConnector via external
      Connection), jdbcsink tls_test.go (jdbcURL + end-to-end
      buildDesiredConnector). Plus providerkit tls_test.go (ResolveDatabaseTLS,
      CATrustFileMounts/CAFilePath, VerifyDatabaseConnection real-dial error
      paths) and domain connection/tls_test.go (ClientTLSConfig require/
      verify-ca/verify-full against a real local TLS listener + throwaway CA).
      Engine-level gate test (external_db_tls_gate_test.go) covers
      reconcile+probe gate enforcement, mirroring TLSTerminationGate's test.
- [x] `docs/reference` regen (`platformctl docs build --out docs/reference`):
      connection.md + explain.md updated, committed alongside.
- [x] `go build ./...` / `-tags integration` both clean; `go vet` both clean;
      unfiltered `go test ./... ; echo true-exit=$?` = 0.

## Done (continued)

- [x] e2e `cmd/platformctl/external_db_tls_integration_test.go` +
      `testdata/external-db-tls-scenario/{no-tls,tls}`: real TLS-required
      Postgres (test CA + server cert, ssl=on, no plaintext pg_hba.conf
      rule) on a FIXED IP on a fixed-subnet Docker network — the design
      that makes the engine's own C10 dual-vantage reachability check and
      the TLS posture's ServerName agree on one address (a 127.0.0.1
      published-port address failed the in-network leg of that check —
      found live, first run — since a container's own loopback never
      reaches the host's published port; a Linux Docker bridge subnet is
      reachable identically from the host and from other containers on the
      same network, so one IP SAN on the cert suffices). All 3 subtests
      pass: no-tls-refused (real server error), wrong-ca-verify-fails
      (certificate/x509 error), verify-full-succeeds (CDC RUNNING +
      idempotent re-apply). Verified twice for stability; containers/
      network clean up correctly.
- [x] `scripts/test-impact.sh` suite row `external-db-tls` added;
      `TestIntegrationSuiteMapCoversEveryTest`/`TestParseSuiteMapAndCoverage`
      (archtest) both pass.
- [x] Doc 08 I2 Done-note appended (additive, guard-compliant).
- [x] Full gate re-run: gofmt clean, build/vet both tag sets clean,
      unfiltered `go test ./...` exit 0, all archtests pass (catalog
      completeness, no-loopback, charm confinement).

## Remaining

- [ ] Final squashed commit (nothing landed yet — both attempts hit the
      GPG timeout; changes staged, COMMIT_MSG.txt at repo root).
- [ ] `bash scripts/test-impact.sh --base main` sweep launch (after commit).

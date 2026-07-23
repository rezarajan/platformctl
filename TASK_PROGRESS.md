# H10 — Mediation hardening: controller CA pinning + enrollment JWT off Env

## Status: code changes complete, verifying

Worktree was branched before the H10 spec (and a large batch of J2/J5
infra) landed on main (234cabe). Merged main into this branch cleanly
(no conflicts) at the start of this session to pick up:
- docs/planning/08 §7.8's actual H10 spec text (was absent before merge)
- ADR 029-033
- testkit janitor, CI shard partition archtest, etc.

## Live R&D done before writing code (all against the pinned images)

1. **CA pinning**: `GET /.well-known/est/cacerts` on a real
   `openziti/ziti-controller:1.5.14` returns `Content-Type:
   application/pkcs7-mime`, base64-wrapped-at-64-cols DER, a degenerate
   ("certs-only") PKCS#7 SignedData with 2 certs (root + intermediate).
   `go.mozilla.org/pkcs7@v0.9.0` (MIT, zero deps) parses it into
   `[]*x509.Certificate` cleanly. Built an `x509.CertPool`, pinned it as
   `RootCAs` on a real `http.Client`, hit `/edge/client/v1/version` — 200
   OK, full TLS verification, no InsecureSkipVerify. Confirmed the
   server cert's SAN set (`localhost`, the container name, `127.0.0.1`,
   `::1`) covers every address `dialController`/`waitControllerServing`
   ever dial through (`EnsureReachable` returns a loopback address on
   both Docker and the Kubernetes port-forward path).

2. **Enrollment JWT off Env — router**: extracted `openziti/ziti-router`'s
   real `bootstrap.bash`. Its `enroll()` function already treats
   `ZITI_ENROLL_TOKEN` as a FILE PATH whenever the value names an
   existing non-empty file (falls back to literal-JWT only otherwise) —
   this is the "documented equivalent" of a `_FILE` var doc 08
   anticipated; no such var actually exists on this image. Verified live:
   FileMount at 0o600 FAILED (`could not load JWT file`) — the image runs
   its bootstrap as unprivileged `ziggy` (uid 2171), and Docker's
   `copyFilesIn` places FileMount content root-owned, so a 0600 file is
   unreadable cross-UID. 0o644 (world-readable) enrolls successfully
   (`isVerified: true` confirmed via REST after using a file-path
   `ZITI_ENROLL_TOKEN` + the JWT in a FileMount). Documented this
   deviation from the literal wireguard-precedent 0600 directly in
   instance.go.

3. **Enrollment JWT off Env — tunneler**: extracted `openziti/ziti-tunnel`'s
   `entrypoint.sh`. It searches fixed candidate dirs
   (`/var/run/secrets/netfoundry.io/enrollment-token`,
   `/enrollment-token`, `$ZITI_IDENTITY_DIR`) for
   `<ZITI_IDENTITY_BASENAME>.jwt` BEFORE ever consulting
   `ZITI_ENROLL_TOKEN` — so no env var is needed here at all. Verified
   live: FileMount at `/enrollment-token/ziti_id.jwt`, mode 0600 (this
   image runs as root, confirmed via `id`), zero Env — tunneler enrolled
   and wrote its identity to the persisted `/netfoundry` volume.

4. Both JWT FileMount paths deliberately kept OUTSIDE the container's own
   persisted named volume (`/ziti-router`, `/netfoundry`): Docker's
   `copyFilesIn` writes into the container's own writable layer, which
   the settle-pass recreate (hash mismatch -> `ContainerRemove` +
   recreate) discards along with the old container; a named volume is
   NOT removed by that recreate, so placing the JWT inside it would leave
   permanent on-disk residue the settle pass could never clean up.

## Code changes

- `client.go`: `pinnedCAPool` (TOFU bootstrap fetch, the one remaining
  `InsecureSkipVerify` in the package, narrowly scoped to fetching the CA
  itself — documented at length in the file's package doc comment).
  `newEdgeClient` now takes a `*x509.CertPool` and builds `RootCAs`, never
  `InsecureSkipVerify`.
- `instance.go`: `dialController`/`waitControllerServing` both fetch+pin
  fresh per call (stateless-provider F5 discipline — no persisted cache).
  Router's enrollment JWT: FileMount at `routerEnrollTokenPath =
  "/run/ziti-enroll/token.jwt"` mode 0o644 + `Env["ZITI_ENROLL_TOKEN"]`
  set to that PATH (not the JWT). Settle pass strips both Env value back
  out AND `Files = nil`.
- `connection.go`: dial-side tunneler's enrollment JWT: FileMount at
  `dialEnrollTokenPath = "/enrollment-token/ziti_id.jwt"` mode 0o600, NO
  Env var at all. Settle pass sets `Files = nil`.
- `client_test.go`: `newEdgeClient` call updated to pass `nil` pool (test
  server is plain HTTP via `httptest.NewServer`, TLSClientConfig never
  consulted).
- `go.mod`/`go.sum`: added `go.mozilla.org/pkcs7 v0.9.0`.

## Verification remaining

- [ ] gofmt / go build / go vet -tags integration / golangci-lint
- [ ] go test ./... unfiltered, log + true-exit
- [ ] acceptance greps
- [ ] live Docker leg: TestOpenZitiMediatedConnectionEndToEnd
- [ ] live K8s leg: check kubeconfig validity first
- [ ] doc 08 H10 Done-note (additive)
- [ ] commit

// This file holds the Docker/fake realization of the ingress provider: one
// shared Caddy container per ingress Provider (bootstrapped once via
// EnsureContainer), with every Connection's route reconciled through Caddy's
// admin API afterward (caddy.go) rather than by rewriting
// ContainerSpec.Files (docs/adr/018 Decision 3). TLS certificates
// (docs/planning/08 C8) follow the identical discipline: loaded via the
// admin API, never via ContainerSpec.Files — the one exception is the
// Provider-scoped local CA keypair itself, which persists via
// ContainerSpec.Files using the same read-before-regenerate pattern
// postgres's superuser-password rotation established (it changes as rarely
// as the bootstrap config itself, never per-Connection).
package ingress

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// Fixed container-internal ports for the shared Caddy container — the host
// side (published, auto-allocated per component name unless pinned) is what
// varies; see providerkit.HostPort.
const (
	httpContainerPort  = 80
	httpsContainerPort = 443 // Caddy's own default https_port — see caddy.go's httpsServerName doc comment
	adminContainerPort = 2019
	bootstrapPath      = "/etc/caddy/config.json"
	// caCertPath/caKeyPath are where the Provider-scoped local CA
	// (spec.tls.selfSigned) persists across separate `apply` invocations —
	// placed via ContainerSpec.Files at bootstrap, read back via
	// ContainerRuntime.ReadFile before ever regenerating (docs/planning/03
	// §8.2.2). Never read by Caddy itself — only leaf certificates it
	// signs are ever loaded into Caddy, exclusively via the admin API.
	caCertPath = "/data/datascape/ca-cert.pem"
	caKeyPath  = "/data/datascape/ca-key.pem"
)

func httpHostPort(cfg provider.Provider, name string) int {
	return providerkit.HostPort(cfg, name, "port")
}

func httpsHostPort(cfg provider.Provider, name string) int {
	return providerkit.HostPort(cfg, name, "httpsPort")
}

func adminHostPort(cfg provider.Provider, name string) int {
	return providerkit.HostPort(cfg, name, "adminPort")
}

// ensureLocalCA returns the Provider's local CA keypair, reusing whatever
// is already placed on the existing container (read via ReadFile) rather
// than generating a fresh one on every apply — a fresh CA every apply would
// force every tool that trusted the previous one to re-trust it, and would
// churn the ContainerSpec.Files content (and therefore the spec hash,
// restarting the shared container) for no reason. Only ever generates a new
// CA when nothing valid is already there (a fresh Provider, or the
// container/its files are genuinely gone).
func ensureLocalCA(ctx context.Context, rt runtime.ContainerRuntime, name string) (certPEM, keyPEM []byte, err error) {
	existingCert, certErr := rt.ReadFile(ctx, name, caCertPath)
	existingKey, keyErr := rt.ReadFile(ctx, name, caKeyPath)
	if certErr == nil && keyErr == nil {
		if _, err := parseCertPEM(existingCert); err == nil {
			if _, _, err := parseCAKeyPair(existingCert, existingKey); err == nil {
				return existingCert, existingKey, nil
			}
		}
	}
	return generateCA()
}

func reconcileInstanceDocker(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	name := containerName(req.Provider)
	caCertPEM, caKeyPEM, err := ensureLocalCA(ctx, req.Runtime, name)
	if err != nil {
		return status.Status{}, fmt.Errorf("provision local CA: %w", err)
	}
	configJSON, err := json.Marshal(bootstrapConfig(httpContainerPort, httpsContainerPort, adminContainerPort))
	if err != nil {
		return status.Status{}, fmt.Errorf("marshal bootstrap caddy config: %w", err)
	}
	ctrState, err := providerkit.EnsureInstance(ctx, req.Runtime, providerkit.InstanceSpec{
		Namespace: req.Provider.Metadata.Namespace,
		Name:      name,
		Network:   providerkit.Network(cfg),
		Container: runtime.ContainerSpec{
			Image: image(cfg),
			// No --adapter flag: the bootstrap config is already Caddy's
			// native JSON document, not a format (Caddyfile, etc.) that
			// needs converting — found live (2026-07-21): "--adapter json"
			// is not a recognized adapter name (json is the format Caddy
			// speaks with no adapter at all; "json" as an adapter name
			// caused an immediate "unrecognized config adapter" exit).
			Cmd: []string{"caddy", "run", "--config", bootstrapPath},
			Files: []runtime.FileMount{
				{Path: bootstrapPath, Content: configJSON, Mode: 0o444},
				// The CA keypair: never read by Caddy (only leaf certs it
				// signs are ever loaded into Caddy, via the admin API) —
				// placed here purely so ensureLocalCA can read it back on
				// the next apply. Mode 0o400 on the key: readable only by
				// the file's owner inside the container.
				{Path: caCertPath, Content: caCertPEM, Mode: 0o444},
				{Path: caKeyPath, Content: caKeyPEM, Mode: 0o400},
			},
			Ports: []runtime.PortBinding{
				{HostPort: httpHostPort(cfg, name), ContainerPort: httpContainerPort, Audience: runtime.AudienceHost},
				{HostPort: httpsHostPort(cfg, name), ContainerPort: httpsContainerPort, Audience: runtime.AudienceHost},
				{HostPort: adminHostPort(cfg, name), ContainerPort: adminContainerPort, Audience: runtime.AudienceHost},
			},
			HealthCheck: &runtime.HealthCheck{
				Test:     []string{"CMD-SHELL", fmt.Sprintf("wget -q --spider http://localhost:%d/config/ || exit 1", adminContainerPort)},
				Interval: 2 * time.Second,
				Timeout:  5 * time.Second,
				Retries:  30,
			},
		},
		WaitTimeout: 60 * time.Second,
	})
	if err != nil {
		return status.Status{}, err
	}

	now := time.Now()
	st := status.Status{}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonProxySurfaceReady}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{
		"containerId": ctrState.ID,
		"network":     providerkit.Network(cfg),
		"image":       image(cfg),
		"domain":      domainSuffix(cfg),
		// tls.caCert publishes the local CA's PUBLIC certificate only — the
		// private key never appears here, in logs, or in inspect output
		// (docs/planning/03 §8.2.2). `platformctl inventory` names this as
		// the CA's location for tools that need to trust it.
		"tls": map[string]any{"caCert": string(caCertPEM)},
	}
	return st, nil
}

func destroyInstanceDocker(ctx context.Context, req reconciler.Request, cfg provider.Provider) error {
	name := containerName(req.Provider)
	if err := req.Runtime.Remove(ctx, name); err != nil {
		return err
	}
	// The network may still be shared with other providers; RemoveNetwork
	// refuses while anything remains attached, and this ignores that
	// refusal — the same convention every shared-network provider's Destroy
	// follows (proxy, prometheus).
	_ = req.Runtime.RemoveNetwork(ctx, providerkit.Network(cfg))
	return nil
}

func probeInstanceDocker(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	name := containerName(req.Provider)
	st := status.Status{}
	now := time.Now()
	ctrState, found, err := req.Runtime.Inspect(ctx, name)
	if err != nil {
		return st, err
	}
	if !found || !ctrState.Healthy {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonProxySurfaceDown}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonProxySurfaceDown}, now)
		return st, nil
	}
	addr, closeAddr, err := providerkit.ReachableAddr(ctx, req.Runtime, name, adminContainerPort)
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonProxySurfaceDown, Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonProxySurfaceDown}, now)
		return st, nil
	}
	defer closeAddr()
	if !caddyReady(ctx, "http://"+addr) {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonProxySurfaceDown}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonProxySurfaceDown}, now)
		return st, nil
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonProxySurfaceReady}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	// Best-effort: republish the CA's public cert if one exists, so a
	// Provider probed (not just reconciled) after a self-signed Connection
	// provisioned it still surfaces the location — never Ready-blocking.
	if caCertPEM, err := req.Runtime.ReadFile(ctx, name, caCertPath); err == nil {
		st.ProviderState = map[string]any{"tls": map[string]any{"caCert": string(caCertPEM)}}
	}
	return st, nil
}

// tlsServer returns the Caddy server a Connection's route/cert belong on,
// and whether TLS applies at all.
func tlsServer(conn connection.Connection) (server string, isTLS bool) {
	if conn.TLS != nil {
		return httpsServerName, true
	}
	return serverName, false
}

// resolveCertDocker determines the PEM cert+key to load for a https
// Connection, per its declared spec.tls mode. proxyName is the shared
// Caddy container's own name (where a self-signed CA persists);
// baseURL is the admin API base (to read back an already-loaded,
// still-valid cert before regenerating one — the idempotency path).
func resolveCertDocker(ctx context.Context, req reconciler.Request, proxyName, baseURL, connName, host string, t *connection.TLS) (certPEM, keyPEM []byte, err error) {
	switch {
	case t.SecretRef != nil:
		refName := *t.SecretRef
		secretVals, ok := req.Secrets[refName]
		if !ok {
			return nil, nil, fmt.Errorf("Connection %q: tls.secretRef %q has no resolved credentials — declare it in the ingress Provider's own spec.secretRefs", connName, refName)
		}
		cert, key := []byte(secretVals["cert"]), []byte(secretVals["key"])
		if len(cert) == 0 || len(key) == 0 {
			return nil, nil, fmt.Errorf("Connection %q: tls.secretRef %q: SecretReference must carry both \"cert\" and \"key\" values", connName, refName)
		}
		if err := validateKeyPair(cert, key); err != nil {
			return nil, nil, fmt.Errorf("Connection %q: tls.secretRef %q: %w", connName, refName, err)
		}
		return cert, key, nil
	case t.SelfSigned:
		caCertPEM, err := req.Runtime.ReadFile(ctx, proxyName, caCertPath)
		if err != nil {
			return nil, nil, fmt.Errorf("Connection %q: ingress Provider has not provisioned its self-signed CA yet (apply the Provider first): %w", connName, err)
		}
		caKeyPEM, err := req.Runtime.ReadFile(ctx, proxyName, caKeyPath)
		if err != nil {
			return nil, nil, fmt.Errorf("Connection %q: ingress Provider's CA key is missing: %w", connName, err)
		}
		now := time.Now()
		if live, found, gerr := getCert(ctx, baseURL, certID(connName)); gerr == nil && found {
			if verr := certChainsToCA([]byte(live.Certificate), caCertPEM, host, now); verr == nil {
				// Already valid and signed by the current CA — reuse
				// verbatim (true idempotency: no regeneration, no admin
				// API write at all).
				return []byte(live.Certificate), []byte(live.Key), nil
			}
		}
		return generateLeafCert(caCertPEM, caKeyPEM, host)
	case t.SecretName != nil:
		return nil, nil, fmt.Errorf("Connection %q: tls.secretName is Kubernetes-only (a cert-manager-managed Secret reference); Docker has no cert-manager equivalent — use tls.secretRef or tls.selfSigned", connName)
	default:
		return nil, nil, fmt.Errorf("Connection %q: spec.tls set none of secretRef/selfSigned/secretName", connName)
	}
}

func reconcileConnectionDocker(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	st := status.Status{}
	conn, err := connection.FromEnvelope(req.Resource)
	if err != nil {
		return st, err
	}
	proxyName := containerName(req.Provider)
	connName := req.Resource.Metadata.Name
	host := routeHost(connName, cfg)
	route := desiredRoute(connName, host, conn.Target)
	server, isTLS := tlsServer(conn)

	adminAddr, closeAdmin, err := providerkit.ReachableAddr(ctx, req.Runtime, proxyName, adminContainerPort)
	if err != nil {
		return st, fmt.Errorf("Connection %q: reach ingress admin API: %w", connName, err)
	}
	defer closeAdmin()
	baseURL := "http://" + adminAddr
	if err := ensureRoute(ctx, baseURL, server, route); err != nil {
		return st, fmt.Errorf("Connection %q: reconcile route: %w", connName, err)
	}

	if isTLS {
		certPEM, keyPEM, cerr := resolveCertDocker(ctx, req, proxyName, baseURL, connName, host, conn.TLS)
		if cerr != nil {
			return st, cerr
		}
		cert := caddyPEMCert{ID: certID(connName), Certificate: string(certPEM), Key: string(keyPEM)}
		if err := ensureCert(ctx, baseURL, cert); err != nil {
			return st, fmt.Errorf("Connection %q: reconcile certificate: %w", connName, err)
		}
	}

	// Ready means serving (docs/planning/01 NFR-11), not just "the admin API
	// accepted the route write" — a route can be written and still 502
	// (upstream unreachable) or point at a host Caddy hasn't finished
	// applying yet (docs/planning/11 B1 finding 2). Settle to the SAME
	// dial-through-route check probeConnectionDocker verifies before
	// declaring Ready.
	if err := waitRouteServing(ctx, req.Runtime, proxyName, host, isTLS); err != nil {
		return st, fmt.Errorf("Connection %q: %w", connName, err)
	}

	// Observed binding, not intent — the shared proxy's actually-published
	// port, so the endpoint URL this reports is dialable, not guessed.
	proxyState, found, err := req.Runtime.Inspect(ctx, proxyName)
	if err != nil {
		return st, err
	}
	scheme := "http"
	containerPort := httpContainerPort
	if isTLS {
		scheme = "https"
		containerPort = httpsContainerPort
	}
	hostURL := ""
	if found {
		if hostAddr := proxyState.HostAddr(containerPort); hostAddr != "" {
			if _, portStr, splitErr := net.SplitHostPort(hostAddr); splitErr == nil {
				hostURL = scheme + "://" + host + ":" + portStr
			}
		}
	}

	now := time.Now()
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonRouteHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	st.ProviderState = map[string]any{
		"host":   host,
		"target": conn.Target,
		endpoint.Key: endpoint.List{
			{
				Name:          "route",
				Scheme:        scheme,
				Host:          hostURL,
				Internal:      scheme + "://" + proxyName + ":" + strconv.Itoa(containerPort) + " (Host: " + host + ")",
				Insecure:      !isTLS,
				RuntimeName:   proxyName,
				ContainerPort: containerPort,
				Audience:      runtime.AudienceHost,
				Network:       providerkit.Network(cfg),
			},
		}.ToState(),
	}
	return st, nil
}

// routeSettleTimeout/routeSettlePoll bound reconcileConnectionDocker's
// Ready determination to a genuinely serving route — the redpanda
// waitTopicSettled pattern (docs/planning/11 B1 findings 1-3). Vars, not
// consts: tests shrink them instead of waiting out a real 45s timeout to
// exercise the honest-failure path.
var (
	routeSettleTimeout = 45 * time.Second
	routeSettlePoll    = 2 * time.Second
)

// waitRouteServing bounds reconcileConnectionDocker's Ready determination
// to the SAME check probeConnectionDocker uses for Ready — a
// dial-through-route to the upstream (probeThroughRoute/
// probeThroughRouteTLS), not just the admin API accepting the route write.
// A healthy route passes on the first attempt (zero added latency); on
// timeout, reconcile fails honestly with the last observed error instead of
// setting Ready from the route write alone (docs/planning/11 B1 finding 2).
func waitRouteServing(ctx context.Context, rt runtime.ContainerRuntime, proxyName, host string, isTLS bool) error {
	containerPort := httpContainerPort
	if isTLS {
		containerPort = httpsContainerPort
	}
	deadline := time.Now().Add(runtime.ScaledWait(routeSettleTimeout))
	var lastErr error
	for {
		lastErr = probeRouteOnce(ctx, rt, proxyName, containerPort, host, isTLS)
		if lastErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("route for host %q did not settle to a serving state within %s: %w", host, routeSettleTimeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(routeSettlePoll):
		}
	}
}

// probeRouteOnce is one attempt of waitRouteServing's poll: reach the
// shared proxy's published http/https port and dial through the route,
// exactly like probeConnectionDocker does.
func probeRouteOnce(ctx context.Context, rt runtime.ContainerRuntime, proxyName string, containerPort int, host string, isTLS bool) error {
	addr, closeAddr, err := providerkit.ReachableAddr(ctx, rt, proxyName, containerPort)
	if err != nil {
		return err
	}
	defer closeAddr()
	if isTLS {
		return probeThroughRouteTLS(ctx, addr, host)
	}
	return probeThroughRoute(ctx, addr, host)
}

func destroyConnectionDocker(ctx context.Context, req reconciler.Request, cfg provider.Provider) error {
	proxyName := containerName(req.Provider)
	connName := req.Resource.Metadata.Name
	adminAddr, closeAdmin, err := providerkit.ReachableAddr(ctx, req.Runtime, proxyName, adminContainerPort)
	if err != nil {
		// The shared proxy is already gone (e.g. its own Provider was
		// destroyed first in the same run) — nothing to delete a route or
		// certificate from.
		return nil
	}
	defer closeAdmin()
	baseURL := "http://" + adminAddr
	if err := deleteRoute(ctx, baseURL, routeID(connName)); err != nil {
		return err
	}
	// Always attempt cert cleanup, not just when the current spec declares
	// TLS — covers a Connection that used to be https and was edited back
	// to plaintext, leaving an orphaned cert; deleteCert is a no-op if none
	// was ever loaded.
	return deleteCert(ctx, baseURL, certID(connName))
}

func probeConnectionDocker(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	conn, err := connection.FromEnvelope(req.Resource)
	if err != nil {
		return st, err
	}
	proxyName := containerName(req.Provider)
	connName := req.Resource.Metadata.Name
	host := routeHost(connName, cfg)
	_, isTLS := tlsServer(conn)

	adminAddr, closeAdmin, err := providerkit.ReachableAddr(ctx, req.Runtime, proxyName, adminContainerPort)
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonProxySurfaceDown, Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonProxySurfaceDown}, now)
		return st, nil
	}
	defer closeAdmin()
	baseURL := "http://" + adminAddr

	live, found, err := getRoute(ctx, baseURL, routeID(connName))
	if err != nil {
		return st, err
	}
	if !found {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonRouteMissing}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonRouteMissing}, now)
		return st, nil
	}
	desired := desiredRoute(connName, host, conn.Target)
	if !routesEquivalent(live, desired) {
		msg := fmt.Sprintf("live route host/upstream differs from declared Connection %q", connName)
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonRouteConfigDrift, Message: msg}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonRouteConfigDrift, Message: msg}, now)
		return st, nil
	}

	if isTLS {
		if driftSt, ok := probeCertDocker(ctx, req, proxyName, baseURL, connName, host, conn.TLS, now); !ok {
			return driftSt, nil
		}
	}

	containerPort := httpContainerPort
	if isTLS {
		containerPort = httpsContainerPort
	}
	proxyAddr, closeProxy, err := providerkit.ReachableAddr(ctx, req.Runtime, proxyName, containerPort)
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonProxySurfaceDown, Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonProxySurfaceDown}, now)
		return st, nil
	}
	defer closeProxy()
	var probeErr error
	if isTLS {
		probeErr = probeThroughRouteTLS(ctx, proxyAddr, host)
	} else {
		probeErr = probeThroughRoute(ctx, proxyAddr, host)
	}
	if probeErr != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonIngressUpstreamUnreachable, Message: probeErr.Error()}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonIngressUpstreamUnreachable, Message: probeErr.Error()}, now)
		return st, nil
	}

	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonRouteHealthy}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st, nil
}

// probeCertDocker checks a https Connection's loaded certificate: present,
// structurally valid, covers host, and (provided-secretRef mode only,
// where the desired content is fully deterministic) byte-matches the
// current SecretReference value. Returns ok=false with the drift/missing
// status already populated when unhealthy, so the caller returns it
// directly.
func probeCertDocker(ctx context.Context, req reconciler.Request, proxyName, baseURL, connName, host string, t *connection.TLS, now time.Time) (status.Status, bool) {
	st := status.Status{}
	live, found, err := getCert(ctx, baseURL, certID(connName))
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonProxySurfaceDown, Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonProxySurfaceDown}, now)
		return st, false
	}
	if !found {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertMissing}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCertMissing}, now)
		return st, false
	}
	switch {
	case t.SecretRef != nil:
		refName := *t.SecretRef
		secretVals, ok := req.Secrets[refName]
		if !ok {
			msg := fmt.Sprintf("tls.secretRef %q has no resolved credentials", refName)
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCertInvalid, Message: msg}, now)
			return st, false
		}
		if !certMatchesSecret([]byte(live.Certificate), []byte(secretVals["cert"])) {
			msg := fmt.Sprintf("live certificate no longer matches SecretReference %q", refName)
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertConfigDrift, Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCertConfigDrift, Message: msg}, now)
			return st, false
		}
	case t.SelfSigned:
		caCertPEM, err := req.Runtime.ReadFile(ctx, proxyName, caCertPath)
		if err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: err.Error()}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCertInvalid, Message: err.Error()}, now)
			return st, false
		}
		if verr := certChainsToCA([]byte(live.Certificate), caCertPEM, host, now); verr != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: verr.Error()}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCertInvalid, Message: verr.Error()}, now)
			return st, false
		}
	}
	return st, true
}

// routesEquivalent compares exactly the two fields this provider ever sets
// — Host match and upstream dial — the same "drifted names, never values"
// bar prometheus/debezium/s3sink hold for their own config-drift checks.
func routesEquivalent(live, desired caddyRoute) bool {
	if len(live.Match) != 1 || len(desired.Match) != 1 {
		return false
	}
	if len(live.Match[0].Host) != 1 || len(desired.Match[0].Host) != 1 || live.Match[0].Host[0] != desired.Match[0].Host[0] {
		return false
	}
	if len(live.Handle) != 1 || len(desired.Handle) != 1 {
		return false
	}
	if len(live.Handle[0].Upstreams) != 1 || len(desired.Handle[0].Upstreams) != 1 {
		return false
	}
	return live.Handle[0].Upstreams[0].Dial == desired.Handle[0].Upstreams[0].Dial
}

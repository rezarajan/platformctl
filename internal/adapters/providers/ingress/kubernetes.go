// This file holds the Kubernetes realization of the ingress provider: one
// networking.k8s.io/v1 Ingress object per Connection, via the
// runtime.IngressCapableRuntime capability (internal/adapters/runtime/
// kubernetes/ingress.go). No shared container — the cluster's own ingress
// controller does the proxying, so there is no reload-without-restart
// problem to solve on this runtime (docs/adr/018 Decision 2). TLS
// (docs/planning/08 C8) is Ingress.spec.tls referencing a kubernetes.io/tls
// Secret — materialized by this provider for secretRef/selfSigned, or
// referenced only (never created/deleted) for a cert-manager-managed
// secretName.
package ingress

import (
	"context"
	"fmt"
	"time"

	"github.com/rezarajan/platformctl/internal/adapters/providers/providerkit"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/endpoint"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/status"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// ingressRuntime type-asserts req.Runtime against the optional
// IngressCapableRuntime capability (docs/adr/018 "Layering") — the provider
// never imports the concrete Kubernetes adapter package.
func ingressRuntime(rt runtime.ContainerRuntime) (runtime.IngressCapableRuntime, error) {
	ic, ok := rt.(runtime.IngressCapableRuntime)
	if !ok {
		return nil, fmt.Errorf("ingress provider: runtime does not implement IngressCapableRuntime (expected on a Kubernetes-runtime Provider)")
	}
	return ic, nil
}

// tlsSecretName/caSecretName are the deterministic Secret names this
// provider materializes — deterministic from the Connection/Provider's own
// name so reconcile is idempotent and Destroy can address them without
// state, exactly like routeID/certID on the Docker side.
func tlsSecretName(connName string) string { return "tls-" + connName }
func caSecretName(provName string) string  { return provName + "-ca" }

// reconcileInstanceKubernetes anchors the shared network only — mirroring
// proxy's own Provider-level reconcile, and the fact that there is no
// central Kubernetes object of this provider's own (every Connection's
// Ingress is self-contained).
func reconcileInstanceKubernetes(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	name := containerName(req.Provider)
	labels := runtime.ManagedLabels(req.Provider.Metadata.Namespace, "Provider", name, name)
	if err := req.Runtime.EnsureNetwork(ctx, runtime.NetworkSpec{Name: providerkit.Network(cfg), Labels: labels}); err != nil {
		return status.Status{}, err
	}
	now := time.Now()
	st := status.Status{}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonProxySurfaceReady}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	providerState := map[string]any{"network": providerkit.Network(cfg), "domain": domainSuffix(cfg)}
	// Best-effort: republish the local CA's public cert if a self-signed
	// Connection already provisioned one — never Ready-blocking, and never
	// generated here (lazily created by the first self-signed Connection's
	// own reconcile, see resolveCertKubernetes below).
	if ic, err := ingressRuntime(req.Runtime); err == nil {
		if caCert, _, found, _ := ic.GetTLSSecret(ctx, providerkit.Network(cfg), caSecretName(name)); found {
			providerState["tls"] = map[string]any{"caCert": string(caCert)}
		}
	}
	st.ProviderState = providerState
	return st, nil
}

func destroyInstanceKubernetes(ctx context.Context, req reconciler.Request, cfg provider.Provider) error {
	_ = req.Runtime.RemoveNetwork(ctx, providerkit.Network(cfg))
	return nil
}

func probeInstanceKubernetes(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	now := time.Now()
	st := status.Status{}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonProxySurfaceReady}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st, nil
}

// resolveCertKubernetes determines the TLSSecretName EnsureIngress should
// reference for a https Connection, materializing it first when this
// provider owns the cert (secretRef/selfSigned). ok=false with a status
// already set means "not ready yet" (e.g. a cert-manager Secret not yet
// issued) — not necessarily an error, so the caller still creates/updates
// the Ingress object (cert-manager-style flows commonly issue the Secret
// only after seeing the Ingress/Certificate reference it).
func resolveCertKubernetes(ctx context.Context, req reconciler.Request, ic runtime.IngressCapableRuntime, ns, provName, connName, host string, t *connection.TLS, now time.Time) (secretName string, st status.Status, ok bool) {
	switch {
	case t.SecretRef != nil:
		refName := *t.SecretRef
		secretVals, found := req.Secrets[refName]
		if !found {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid,
				Message: fmt.Sprintf("tls.secretRef %q has no resolved credentials — declare it in the ingress Provider's own spec.secretRefs", refName)}, now)
			return "", st, false
		}
		cert, key := []byte(secretVals["cert"]), []byte(secretVals["key"])
		if len(cert) == 0 || len(key) == 0 {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid,
				Message: fmt.Sprintf("tls.secretRef %q: SecretReference must carry both \"cert\" and \"key\" values", refName)}, now)
			return "", st, false
		}
		if err := validateKeyPair(cert, key); err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: err.Error()}, now)
			return "", st, false
		}
		name := tlsSecretName(connName)
		if err := ic.EnsureTLSSecret(ctx, ns, name, cert, key, runtime.ManagedLabels(ns, "Connection", connName, connName)); err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: err.Error()}, now)
			return "", st, false
		}
		return name, st, true

	case t.SelfSigned:
		caName := caSecretName(provName)
		caCertPEM, caKeyPEM, found, err := ic.GetTLSSecret(ctx, ns, caName)
		if err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: err.Error()}, now)
			return "", st, false
		}
		if !found {
			// Lazily provisioned by the first self-signed Connection —
			// never at Provider-level reconcile, which has no visibility
			// into which Connections declare TLS.
			caCertPEM, caKeyPEM, err = generateCA()
			if err != nil {
				st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: err.Error()}, now)
				return "", st, false
			}
			if err := ic.EnsureTLSSecret(ctx, ns, caName, caCertPEM, caKeyPEM, runtime.ManagedLabels(ns, "Provider", provName, provName)); err != nil {
				st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: err.Error()}, now)
				return "", st, false
			}
		}
		leafName := tlsSecretName(connName)
		if liveCert, _, lfound, lerr := ic.GetTLSSecret(ctx, ns, leafName); lerr == nil && lfound {
			if verr := certChainsToCA(liveCert, caCertPEM, host, now); verr == nil {
				return leafName, st, true // reuse — no regeneration, no write
			}
		}
		leafCert, leafKey, err := generateLeafCert(caCertPEM, caKeyPEM, host)
		if err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: err.Error()}, now)
			return "", st, false
		}
		if err := ic.EnsureTLSSecret(ctx, ns, leafName, leafCert, leafKey, runtime.ManagedLabels(ns, "Connection", connName, connName)); err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: err.Error()}, now)
			return "", st, false
		}
		return leafName, st, true

	case t.SecretName != nil:
		name := *t.SecretName
		if _, _, found, err := ic.GetTLSSecret(ctx, ns, name); err != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: err.Error()}, now)
			return name, st, false
		} else if !found {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertMissing,
				Message: fmt.Sprintf("cert-manager Secret %q not found in namespace %q yet", name, ns)}, now)
			return name, st, false // still return the name: the Ingress references it so cert-manager's own controller can pick it up
		}
		return name, st, true

	default:
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: "spec.tls set none of secretRef/selfSigned/secretName"}, now)
		return "", st, false
	}
}

func reconcileConnectionKubernetes(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	st := status.Status{}
	ic, err := ingressRuntime(req.Runtime)
	if err != nil {
		return st, err
	}
	conn, err := connection.FromEnvelope(req.Resource)
	if err != nil {
		return st, err
	}
	targetHost, targetPort, err := parseTarget(conn.Target)
	if err != nil {
		return st, fmt.Errorf("Connection %q: %w", req.Resource.Metadata.Name, err)
	}
	connName := req.Resource.Metadata.Name
	host := routeHost(connName, cfg)
	name := routeID(connName)
	ns := req.Resource.Metadata.Namespace
	if ns == "" {
		ns = "default"
	}
	labels := runtime.ManagedLabels(ns, "Connection", connName, connName)
	now := time.Now()

	tlsSecret := ""
	certReady := true
	var certStatus status.Status
	if conn.TLS != nil {
		provName := containerName(req.Provider)
		tlsSecret, certStatus, certReady = resolveCertKubernetes(ctx, req, ic, providerkit.Network(cfg), provName, connName, host, conn.TLS, now)
	}

	ingState, err := ic.EnsureIngress(ctx, runtime.IngressSpec{
		Name:          name,
		Namespace:     providerkit.Network(cfg),
		Host:          host,
		TargetName:    targetHost,
		TargetPort:    targetPort,
		Labels:        labels,
		TLSSecretName: tlsSecret,
	})
	if err != nil {
		return st, fmt.Errorf("Connection %q: %w", connName, err)
	}

	scheme := "http"
	if conn.TLS != nil {
		scheme = "https"
	}
	hostURL := ""
	if ingState.Address != "" || ingState.Host != "" {
		hostURL = scheme + "://" + host
	}
	st.ProviderState = map[string]any{
		"host":   host,
		"target": conn.Target,
		endpoint.Key: endpoint.List{
			{
				Name:     "route",
				Scheme:   scheme,
				Host:     hostURL,
				Internal: fmt.Sprintf("http://%s:%d", targetHost, targetPort),
				Insecure: conn.TLS == nil,
				Network:  providerkit.Network(cfg),
			},
		}.ToState(),
	}
	if conn.TLS != nil && !certReady {
		// The route object itself is correctly declared — surfaced above —
		// but the endpoint isn't truly reachable over TLS yet (e.g. a
		// cert-manager Secret not yet issued). Report the cert condition,
		// not a fabricated Ready:true.
		if c, ok := certStatus.Condition(status.Ready); ok {
			st.SetCondition(c, now)
		}
		st.SetCondition(status.Condition{Type: status.Progressing, Status: status.True, Reason: status.ReasonCertMissing}, now)
		return st, nil
	}
	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonRouteHealthy}, now)
	st.SetCondition(status.Condition{Type: status.Progressing, Status: status.False, Reason: status.ReasonReconcileComplete}, now)
	return st, nil
}

func destroyConnectionKubernetes(ctx context.Context, req reconciler.Request, cfg provider.Provider) error {
	ic, err := ingressRuntime(req.Runtime)
	if err != nil {
		return err
	}
	connName := req.Resource.Metadata.Name
	if err := ic.RemoveIngress(ctx, providerkit.Network(cfg), routeID(connName)); err != nil {
		return err
	}
	// Only remove a Secret this provider could have materialized
	// (secretRef/selfSigned) — never a cert-manager-managed secretName,
	// which platformctl only ever references. Safe to always attempt
	// (RemoveTLSSecret is a no-op if nothing was ever created), covering a
	// Connection that used to be https and was edited back to plaintext.
	return ic.RemoveTLSSecret(ctx, providerkit.Network(cfg), tlsSecretName(connName))
}

func probeConnectionKubernetes(ctx context.Context, req reconciler.Request, cfg provider.Provider) (status.Status, error) {
	st := status.Status{}
	now := time.Now()
	ic, err := ingressRuntime(req.Runtime)
	if err != nil {
		return st, err
	}
	conn, err := connection.FromEnvelope(req.Resource)
	if err != nil {
		return st, err
	}
	targetHost, targetPort, err := parseTarget(conn.Target)
	if err != nil {
		return st, fmt.Errorf("Connection %q: %w", req.Resource.Metadata.Name, err)
	}
	connName := req.Resource.Metadata.Name
	host := routeHost(connName, cfg)
	ns := providerkit.Network(cfg)

	live, found, err := ic.GetIngress(ctx, ns, routeID(connName))
	if err != nil {
		return st, err
	}
	if !found {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonRouteMissing}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonRouteMissing}, now)
		return st, nil
	}
	if live.Host != host || live.TargetName != targetHost || live.TargetPort != targetPort {
		msg := fmt.Sprintf("live Ingress (host=%q, backend=%s:%d) differs from declared Connection %q (host=%q, backend=%s:%d)",
			live.Host, live.TargetName, live.TargetPort, connName, host, targetHost, targetPort)
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonRouteConfigDrift, Message: msg}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonRouteConfigDrift, Message: msg}, now)
		return st, nil
	}

	if conn.TLS == nil {
		if live.TLSSecretName != "" {
			msg := fmt.Sprintf("live Ingress carries spec.tls (secret %q) but Connection %q no longer declares tls", live.TLSSecretName, connName)
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonRouteConfigDrift, Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonRouteConfigDrift, Message: msg}, now)
			return st, nil
		}
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonRouteHealthy}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
		return st, nil
	}

	desiredSecret := ""
	switch {
	case conn.TLS.SecretRef != nil:
		desiredSecret = tlsSecretName(connName)
	case conn.TLS.SelfSigned:
		desiredSecret = tlsSecretName(connName)
	case conn.TLS.SecretName != nil:
		desiredSecret = *conn.TLS.SecretName
	}
	if live.TLSSecretName != desiredSecret {
		msg := fmt.Sprintf("live Ingress tls.secretName %q differs from declared %q", live.TLSSecretName, desiredSecret)
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonRouteConfigDrift, Message: msg}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonRouteConfigDrift, Message: msg}, now)
		return st, nil
	}

	certPEM, _, secretFound, err := ic.GetTLSSecret(ctx, ns, desiredSecret)
	if err != nil {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: err.Error()}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCertInvalid}, now)
		return st, nil
	}
	if !secretFound {
		st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertMissing}, now)
		st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCertMissing}, now)
		return st, nil
	}
	if conn.TLS.SecretRef != nil {
		secretVals := req.Secrets[*conn.TLS.SecretRef]
		if !certMatchesSecret(certPEM, []byte(secretVals["cert"])) {
			msg := fmt.Sprintf("live certificate no longer matches SecretReference %q", *conn.TLS.SecretRef)
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertConfigDrift, Message: msg}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCertConfigDrift, Message: msg}, now)
			return st, nil
		}
	} else if conn.TLS.SelfSigned {
		provName := containerName(req.Provider)
		caCertPEM, _, caFound, cerr := ic.GetTLSSecret(ctx, ns, caSecretName(provName))
		if cerr != nil || !caFound {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: "local CA not found"}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCertInvalid}, now)
			return st, nil
		}
		if verr := certChainsToCA(certPEM, caCertPEM, host, now); verr != nil {
			st.SetCondition(status.Condition{Type: status.Ready, Status: status.False, Reason: status.ReasonCertInvalid, Message: verr.Error()}, now)
			st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.True, Reason: status.ReasonCertInvalid, Message: verr.Error()}, now)
			return st, nil
		}
	}
	// secretName (cert-manager) mode: existence + Ingress-reference
	// agreement (already checked above) is the whole contract — this
	// provider never validates a cert-manager-issued certificate's own
	// content (referencing only, never operating cert-manager).

	st.SetCondition(status.Condition{Type: status.Ready, Status: status.True, Reason: status.ReasonRouteHealthy}, now)
	st.SetCondition(status.Condition{Type: status.DriftDetected, Status: status.False, Reason: status.ReasonNoDrift}, now)
	return st, nil
}

// This file holds the shared outbound-database-TLS plumbing every
// database-dialing provider consumes (docs/planning/08 I2, docs/adr/025):
// resolving an external Connection's spec.tls.mode into a usable posture,
// mounting CA bundles into a Connect-worker-style container (debezium,
// jdbcsink), and the Go-side preflight dial debezium/jdbcsink's connector
// registration verifies before ever registering a connector (ADR 011 —
// never mid-apply).
package providerkit

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	// Registers the "mysql" database/sql driver.
	godriver "github.com/go-sql-driver/mysql"

	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// DatabaseTLS is the outbound TLS posture resolved from an external
// Connection's spec.tls (docs/planning/08 I2) — nil throughout this seam
// means "no posture": either the Connection is managed, or it's external
// with spec.tls absent, both preserving the pre-I2 plaintext behavior
// byte-for-byte.
type DatabaseTLS struct {
	// Mode is connection.TLSModeRequire/TLSModeVerifyCA/TLSModeVerifyFull.
	Mode string
	// CAPEM is the resolved CA bundle content, when spec.tls.caSecretRef
	// was declared; nil otherwise (verify-ca/verify-full then trust the
	// dialing process's system root CAs, same as an unset libpq
	// sslrootcert).
	CAPEM []byte
	// CASecretRefName is spec.tls.caSecretRef's own name, when declared —
	// carried alongside CAPEM so a caller mounting the CA into a
	// Connect-worker-style container (CATrustFileMounts/CAFilePath) can
	// name the same deterministic path without re-deriving it.
	CASecretRefName string
}

// ResolveDatabaseTLS resolves ep's outbound TLS posture (nil when the
// Source's Connection is managed or declares no spec.tls — see
// EndpointResolution.TLS). spec.tls.caSecretRef, when declared, must also
// appear in the consuming Provider's own spec.secretRefs — the same
// discipline every other Connection-carried secretRef already follows
// (mirrors ResolveEndpointCredentials); its absence is a clear
// configuration error surfaced here, not a silent skip.
func ResolveDatabaseTLS(req reconciler.Request, cfg provider.Provider, ep EndpointResolution) (*DatabaseTLS, error) {
	if ep.TLS == nil {
		return nil, nil
	}
	posture := &DatabaseTLS{Mode: ep.TLS.Mode}
	if ep.TLS.CASecretRef != nil {
		ref := *ep.TLS.CASecretRef
		if !cfg.HasSecretRef(ref) {
			return nil, fmt.Errorf("spec.tls.caSecretRef %q must also be listed in this Provider's spec.secretRefs for the engine to resolve it", ref)
		}
		creds, ok := req.Secrets[ref]
		if !ok {
			return nil, fmt.Errorf("no resolved credentials for spec.tls.caSecretRef %q", ref)
		}
		ca := creds["ca"]
		if ca == "" {
			return nil, fmt.Errorf("SecretReference %q (spec.tls.caSecretRef) must provide a \"ca\" key (a PEM-encoded CA bundle)", ref)
		}
		posture.CAPEM = []byte(ca)
		posture.CASecretRefName = ref
	}
	return posture, nil
}

// TLSConfig builds the *tls.Config a Go-side database driver (pgx,
// go-sql-driver/mysql) dials with for d — nil (both the receiver and the
// return value) means plaintext, unchanged. serverName is the host being
// dialed (verify-full's hostname check only). Delegates to
// connection.ClientTLSConfig, the pure/stdlib-only implementation shared
// with internal/application/engine so both layers build byte-identical
// tls.Config values without providerkit becoming an application-layer
// import (CLAUDE.md's layering rule).
func (d *DatabaseTLS) TLSConfig(serverName string) (*tls.Config, error) {
	if d == nil {
		return nil, nil
	}
	return connection.ClientTLSConfig(d.Mode, d.CAPEM, serverName)
}

// CATrustDir is where a Connect-worker-style provider (debezium, jdbcsink)
// mounts every CA bundle its own spec.secretRefs resolve a "ca" key for —
// content-addressed by secretRef name, not by which Binding/Connection
// consumes it, since a worker's own secretRefs list must already declare
// every CA any Binding it realizes will need (the same discipline every
// other Connection-carried secret follows — docs/planning/08 I2). Mounted
// once at the worker's own Reconcile via ContainerSpec.Files (the same
// hash-triggered-recreate mechanism every other file mount already uses),
// read back deterministically by connector properties built later at
// Binding-reconcile time — no coordination between the two calls beyond
// the shared secretRef name.
const CATrustDir = "/run/datascape/tls"

// CAFilePath returns the fixed, deterministic path a CA bundle named by
// secretRefName resolves to inside a Connect-worker-style container, once
// CATrustFileMounts placed it there.
func CAFilePath(secretRefName string) string {
	return CATrustDir + "/" + secretRefName + ".ca.pem"
}

// CATrustFileMounts returns one runtime.FileMount per cfg-declared
// secretRef whose resolved secrets carry a non-empty "ca" key — see
// CATrustDir's doc comment for why this mounts every CA the Provider
// might need, not just ones a currently-known Binding references.
func CATrustFileMounts(cfg provider.Provider, secrets map[string]map[string]string) []runtime.FileMount {
	var mounts []runtime.FileMount
	for _, ref := range cfg.SecretRefs {
		if ca := secrets[ref]["ca"]; ca != "" {
			mounts = append(mounts, runtime.FileMount{Path: CAFilePath(ref), Content: []byte(ca), Mode: 0o444})
		}
	}
	return mounts
}

// VerifyDatabaseConnection is the shared CDC/sink preflight dial — a real,
// credentialed connection attempt against the actual database before a
// Debezium source connector or a JDBC sink connector is ever registered
// (ADR 011: never mid-apply). Identical logic previously lived duplicated
// in debezium.go and jdbcsink.go verbatim; this is that duplication
// resolved into the providerkit seam, now TLS-aware (docs/planning/08 I2).
// tlsPosture == nil dials plaintext, byte-for-byte the pre-I2 behavior.
func VerifyDatabaseConnection(ctx context.Context, engine, host string, port int, dbName, user, pass string, tlsPosture *DatabaseTLS) error {
	// Bounded transient retry (doc 02 §4.1, found live at the 2026-07-23
	// gate): a single-shot preflight through freshly-built infrastructure
	// (a mediated entrypoint's first Ziti circuit, a just-started
	// forwarder) races setup latency — "unexpected EOF" mid-handshake is
	// transport, not a verdict. Transport-class failures retry within the
	// window; auth/TLS verdicts (wrong password, certificate rejection)
	// return immediately — retrying a real refusal would only delay the
	// honest error (and hammer a lockout counter).
	deadline := time.Now().Add(runtime.ScaledWait(20 * time.Second))
	for {
		err := verifyDatabaseConnectionOnce(ctx, engine, host, port, dbName, user, pass, tlsPosture)
		if err == nil || !isTransientConnError(err) || time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(2 * time.Second):
		}
	}
}

func verifyDatabaseConnectionOnce(ctx context.Context, engine, host string, port int, dbName, user, pass string, tlsPosture *DatabaseTLS) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	switch engine {
	case "postgres":
		return verifyPostgresConnection(ctx, host, port, dbName, user, pass, tlsPosture)
	case "mysql", "mariadb":
		return verifyMySQLConnection(ctx, host, port, dbName, user, pass, tlsPosture)
	default:
		return nil
	}
}

// isTransientConnError classifies transport-layer failures (retriable
// within VerifyDatabaseConnection's window) apart from server verdicts
// (auth failures, TLS rejections — immediate). String matching is the
// honest option here: the drivers wrap syscall errors inconsistently.
func isTransientConnError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, marker := range []string{
		"unexpected EOF",
		"connection refused",
		"connection reset",
		"broken pipe",
		"i/o timeout",
		"no such host",
	} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

func verifyPostgresConnection(ctx context.Context, host string, port int, dbName, user, pass string, tlsPosture *DatabaseTLS) error {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(user, pass),
		Host:   host + ":" + strconv.Itoa(port),
		Path:   "/" + dbName,
	}
	pgCfg, err := pgx.ParseConfig(u.String())
	if err != nil {
		return fmt.Errorf("parse postgres connection string for %s:%d/%s: %w", host, port, dbName, err)
	}
	if tlsPosture == nil {
		pgCfg.TLSConfig = nil
	} else {
		tlsCfg, err := tlsPosture.TLSConfig(host)
		if err != nil {
			return fmt.Errorf("postgres %s:%d/%s: invalid CA bundle for spec.tls.caSecretRef: %w", host, port, dbName, err)
		}
		pgCfg.TLSConfig = tlsCfg
	}
	conn, err := pgx.ConnectConfig(ctx, pgCfg)
	if err != nil {
		if tlsPosture != nil {
			return fmt.Errorf("connect to postgres %s:%d/%s as %q (tls mode %s): %w", host, port, dbName, user, tlsPosture.Mode, err)
		}
		return fmt.Errorf("connect to postgres %s:%d/%s as %q: %w", host, port, dbName, user, err)
	}
	defer conn.Close(ctx)
	return nil
}

func verifyMySQLConnection(ctx context.Context, host string, port int, dbName, user, pass string, tlsPosture *DatabaseTLS) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=10s", user, pass, host, port, dbName)
	if tlsPosture != nil {
		tlsCfg, err := tlsPosture.TLSConfig(host)
		if err != nil {
			return fmt.Errorf("mysql %s:%d/%s: invalid CA bundle for spec.tls.caSecretRef: %w", host, port, dbName, err)
		}
		name := mysqlTLSConfigName(host, port, tlsPosture)
		if err := godriver.RegisterTLSConfig(name, tlsCfg); err != nil {
			return fmt.Errorf("mysql %s:%d/%s: register TLS config: %w", host, port, dbName, err)
		}
		defer godriver.DeregisterTLSConfig(name)
		dsn += "&tls=" + name
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("connect to mysql %s:%d/%s as %q: %w", host, port, dbName, user, err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		if tlsPosture != nil {
			return fmt.Errorf("connect to mysql %s:%d/%s as %q (tls mode %s): %w", host, port, dbName, user, tlsPosture.Mode, err)
		}
		return fmt.Errorf("connect to mysql %s:%d/%s as %q: %w", host, port, dbName, user, err)
	}
	return nil
}

// mysqlTLSConfigName derives a stable go-sql-driver RegisterTLSConfig key
// for one (host, port, TLS posture) triple — deterministic so repeated
// preflights (every Reconcile/Probe) re-register idempotently rather than
// growing the driver's global config registry without bound.
func mysqlTLSConfigName(host string, port int, tlsPosture *DatabaseTLS) string {
	return fmt.Sprintf("platformctl-%s-%d-%s", host, port, tlsPosture.Mode)
}

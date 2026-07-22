package trino

import (
	"bufio"
	"bytes"
	"fmt"
	"sort"
	"strings"
)

// lakehouseCatalogPath is the coordinator-internal path of the one catalog
// this provider manages (docs/planning/08 D10 — Trino's own connectors
// beyond Iceberg are an explicit follow-up, docs/adr/006). Trino discovers
// any file under etc/catalog/*.properties as a catalog named after the file
// (minus the .properties suffix); "lakehouse" is the fixed catalog name a
// Binding-adjacent query ("SELECT * FROM lakehouse.<schema>.<table>")
// addresses, matching the paste-ready snippet's own catalog name in
// cmd/platformctl/toolconfig.go's renderTrino.
const lakehouseCatalogPath = "/etc/trino/catalog/lakehouse.properties"

// renderCatalogConfig is the desired etc/catalog/lakehouse.properties
// content for the given facts (reconciler.CatalogFacts) plus resolved S3
// credentials — deterministic key order so a repeated render of the same
// facts is byte-identical (idempotency: EnsureContainer hashes ContainerSpec
// including Files content, so a non-deterministic render would recreate the
// coordinator on every apply even with unchanged facts). restURL is
// CatalogFacts.RestInternal used verbatim: the Catalog's own published
// "iceberg-rest" endpoint fact already carries a full "http://host:port/
// iceberg" URL (see nessie.go's endpoint publishing) — this package must
// not reconstruct a URL from a bare host:port the way it does for
// s3HostPort below (ADR 015: publish, don't guess a fact's shape).
// s3HostPort is CatalogFacts.S3Internal, a bare "host:port" (the s3
// provider's own "s3" endpoint fact convention, no scheme). s3AccessKey/
// s3SecretKey are the resolved credential *values*; they are written into
// the container's own file content (never into providerState/state — the
// same "config file, not env/state" placement debezium/s3sink already use
// for secret-bearing connector config).
func renderCatalogConfig(restURL, s3HostPort, s3AccessKey, s3SecretKey string) []byte {
	kv := catalogProperties(restURL, s3HostPort, s3AccessKey, s3SecretKey)
	return encodeProperties(kv)
}

// catalogProperties is the key/value set renderCatalogConfig writes —
// factored out so drift-checking (diffCatalogConfig) can compare the
// *desired* set against a *live* set parsed from the same key space,
// without secret values ever entering the comparison (see
// catalogConfigDrift's doc comment).
func catalogProperties(restURL, s3HostPort, s3AccessKey, s3SecretKey string) map[string]string {
	return map[string]string{
		// Exactly the key set cmd/platformctl/toolconfig.go's renderTrino
		// already documents as the paste-ready snippet (proven prose this
		// package now also makes real): connector.name/iceberg.catalog.type/
		// iceberg.rest-catalog.uri/fs.native-s3.enabled/s3.endpoint/
		// s3.path-style-access, plus the two credential keys the snippet
		// leaves as a comment for a human to fill in — this provider embeds
		// the actual resolved values instead (never in state/providerState;
		// see the package doc comment). s3.region is a required addition
		// found live (docs/adr/006's Implementation notes): Trino's S3
		// filesystem factory falls back to the AWS SDK's default region
		// provider chain when unset, which — with no AWS_REGION env var, no
		// profile, and no EC2 metadata service reachable from a container —
		// spends about three minutes exhausting every provider in the chain
		// before failing catalog initialization outright. MinIO ignores the
		// region value; any syntactically valid AWS region satisfies the
		// SDK's own requirement that one be present.
		"connector.name":           "iceberg",
		"iceberg.catalog.type":     "rest",
		"iceberg.rest-catalog.uri": restURL,
		"fs.native-s3.enabled":     "true",
		"s3.endpoint":              "http://" + s3HostPort,
		"s3.region":                "us-east-1",
		"s3.path-style-access":     "true",
		"s3.aws-access-key":        s3AccessKey,
		"s3.aws-secret-key":        s3SecretKey,
	}
}

// encodeProperties renders a Java .properties file with keys sorted
// lexically, one "key=value" line each — deterministic output for the
// idempotency/spec-hash reason documented on renderCatalogConfig.
func encodeProperties(kv map[string]string) []byte {
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	for _, k := range keys {
		fmt.Fprintf(&buf, "%s=%s\n", k, kv[k])
	}
	return buf.Bytes()
}

// parseProperties reads a Java .properties file (the simple "key=value"
// subset this package ever writes: no continuation lines, no escapes) back
// into a map — used to parse the live file read back via
// ContainerRuntime.ReadFile for drift comparison.
func parseProperties(data []byte) map[string]string {
	out := map[string]string{}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out
}

// diffCatalogConfig compares desired against live by KEY only (the
// debezium connectorConfigDrift / prometheus diffScrapeConfig bar this
// package's doc comments reference): a value difference on a secret key
// (s3.aws-access-key/s3.aws-secret-key) is real drift too, so those are
// compared like any other key — the *comparison* seeing a secret value is
// unavoidable to detect a rotated-out-of-band credential, but the value
// itself never leaves this process (the returned list names only the
// mismatched keys, never their values), matching the DriftDetected
// Condition/Message convention (docs/planning/08's config-drift bar): a
// human sees "what changed", never "changed from X to Y" for secret-bearing
// keys.
func diffCatalogConfig(desired, live map[string]string) []string {
	var drifted []string
	for k, dv := range desired {
		if lv, ok := live[k]; !ok || lv != dv {
			drifted = append(drifted, k)
		}
	}
	for k := range live {
		if _, ok := desired[k]; !ok {
			drifted = append(drifted, k)
		}
	}
	sort.Strings(drifted)
	return dedupe(drifted)
}

func dedupe(in []string) []string {
	if len(in) == 0 {
		return in
	}
	out := in[:1]
	for _, v := range in[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

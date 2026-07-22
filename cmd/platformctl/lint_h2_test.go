package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/rezarajan/platformctl/internal/adapters/providers/debezium"
	"github.com/rezarajan/platformctl/internal/adapters/providers/redpanda"
	"github.com/rezarajan/platformctl/internal/adapters/providers/s3sink"
)

// writeManifests writes files (name -> YAML content) under a fresh temp
// dir and returns its path — the positive/negative fixture pattern docs/
// planning/08 H2 asks for, exercised through the real provider registry
// (this package already imports every adapter, unlike
// internal/application/lint's own tests).
func writeManifests(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// lintCodes runs `platformctl lint -o json` against dir and returns the set
// of codes present in the findings.
func lintCodes(t *testing.T, dir string, extraArgs ...string) map[string]bool {
	t.Helper()
	args := append([]string{"lint", dir, "-o", "json"}, extraArgs...)
	out, _, err := runSplit(t, args...)
	if err != nil {
		t.Fatalf("lint %s: %v\n%s", dir, err, out)
	}
	var parsed struct {
		Findings []struct {
			Code string `json:"code"`
		} `json:"findings"`
	}
	if jsonErr := json.Unmarshal([]byte(out), &parsed); jsonErr != nil {
		t.Fatalf("lint %s -o json: %v\n%s", dir, jsonErr, out)
	}
	codes := map[string]bool{}
	for _, f := range parsed.Findings {
		codes[f.Code] = true
	}
	return codes
}

// --- debezium ----------------------------------------------------------------

const debeziumFixtureCommon = `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: broker
spec:
  type: redpanda
  runtime: {type: fake}
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: cdc
spec:
  type: debezium
  runtime: {type: fake}
`

func TestDebeziumReplicationSlotPressureLint(t *testing.T) {
	t.Run("positive: two connectors against one physical database", func(t *testing.T) {
		dir := writeManifests(t, map[string]string{
			"common.yaml": debeziumFixtureCommon,
			"db.yaml": `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: db
spec:
  type: noop
  runtime: {type: fake}
`,
			"sources.yaml": `
apiVersion: datascape.io/v1alpha1
kind: Source
metadata:
  name: src1
spec:
  engine: postgres
  providerRef: {name: db}
  deletionPolicy: retain
---
apiVersion: datascape.io/v1alpha1
kind: Source
metadata:
  name: src2
spec:
  engine: postgres
  providerRef: {name: db}
  deletionPolicy: retain
`,
			"streams.yaml": `
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: es1
spec:
  providerRef: {name: broker}
---
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: es2
spec:
  providerRef: {name: broker}
`,
			"bindings.yaml": `
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: cdc1
spec:
  mode: cdc
  sourceRef: {name: src1}
  targetRef: {name: es1}
  providerRef: {name: cdc}
  options: {tables: [orders]}
---
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: cdc2
spec:
  mode: cdc
  sourceRef: {name: src2}
  targetRef: {name: es2}
  providerRef: {name: cdc}
  options: {tables: [users]}
`,
		})
		if codes := lintCodes(t, dir); !codes[debezium.LintCodeReplicationSlotPressure] {
			t.Errorf("expected %s to fire; got %v", debezium.LintCodeReplicationSlotPressure, codes)
		}
	})

	t.Run("negative: two connectors against two different databases", func(t *testing.T) {
		dir := writeManifests(t, map[string]string{
			"common.yaml": debeziumFixtureCommon,
			"db.yaml": `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: db1
spec:
  type: noop
  runtime: {type: fake}
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: db2
spec:
  type: noop
  runtime: {type: fake}
`,
			"sources.yaml": `
apiVersion: datascape.io/v1alpha1
kind: Source
metadata:
  name: src1
spec:
  engine: postgres
  providerRef: {name: db1}
  deletionPolicy: retain
---
apiVersion: datascape.io/v1alpha1
kind: Source
metadata:
  name: src2
spec:
  engine: postgres
  providerRef: {name: db2}
  deletionPolicy: retain
`,
			"streams.yaml": `
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: es1
spec:
  providerRef: {name: broker}
---
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: es2
spec:
  providerRef: {name: broker}
`,
			"bindings.yaml": `
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: cdc1
spec:
  mode: cdc
  sourceRef: {name: src1}
  targetRef: {name: es1}
  providerRef: {name: cdc}
  options: {tables: [orders]}
---
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: cdc2
spec:
  mode: cdc
  sourceRef: {name: src2}
  targetRef: {name: es2}
  providerRef: {name: cdc}
  options: {tables: [users]}
`,
		})
		if codes := lintCodes(t, dir); codes[debezium.LintCodeReplicationSlotPressure] {
			t.Errorf("expected %s NOT to fire (different physical databases); got %v", debezium.LintCodeReplicationSlotPressure, codes)
		}
	})
}

func TestDebeziumOverlappingPatternCaptureLint(t *testing.T) {
	common := debeziumFixtureCommon + `
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: db
spec:
  type: noop
  runtime: {type: fake}
---
apiVersion: datascape.io/v1alpha1
kind: Source
metadata:
  name: src
spec:
  engine: postgres
  providerRef: {name: db}
  deletionPolicy: retain
---
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: es1
spec:
  providerRef: {name: broker}
---
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: es2
spec:
  providerRef: {name: broker}
`

	t.Run("positive: a regex pattern overlaps a literal table on the same Source", func(t *testing.T) {
		dir := writeManifests(t, map[string]string{
			"common.yaml": common,
			"bindings.yaml": `
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: cdc1
spec:
  mode: cdc
  sourceRef: {name: src}
  targetRef: {name: es1}
  providerRef: {name: cdc}
  options: {tables: [orders]}
---
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: cdc2
spec:
  mode: cdc
  sourceRef: {name: src}
  targetRef: {name: es2}
  providerRef: {name: cdc}
  options: {tables: ["ord.*"]}
`,
		})
		if codes := lintCodes(t, dir); !codes[debezium.LintCodeOverlappingPatternCapture] {
			t.Errorf("expected %s to fire; got %v", debezium.LintCodeOverlappingPatternCapture, codes)
		}
	})

	t.Run("negative: disjoint literal tables, no pattern involved", func(t *testing.T) {
		dir := writeManifests(t, map[string]string{
			"common.yaml": common,
			"bindings.yaml": `
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: cdc1
spec:
  mode: cdc
  sourceRef: {name: src}
  targetRef: {name: es1}
  providerRef: {name: cdc}
  options: {tables: [orders]}
---
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: cdc2
spec:
  mode: cdc
  sourceRef: {name: src}
  targetRef: {name: es2}
  providerRef: {name: cdc}
  options: {tables: [users]}
`,
		})
		if codes := lintCodes(t, dir); codes[debezium.LintCodeOverlappingPatternCapture] {
			t.Errorf("expected %s NOT to fire (disjoint literal tables); got %v", debezium.LintCodeOverlappingPatternCapture, codes)
		}
	})
}

// --- redpanda ------------------------------------------------------------------

func TestRedpandaReplicationBelowBrokersLint(t *testing.T) {
	fixture := func(replication string) string {
		return `
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: broker
spec:
  type: redpanda
  runtime: {type: fake}
  configuration: {brokers: 3}
---
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: es
spec:
  providerRef: {name: broker}
` + replication
	}

	t.Run("positive: replication below broker count", func(t *testing.T) {
		dir := writeManifests(t, map[string]string{"m.yaml": fixture("  replication: 1\n")})
		if codes := lintCodes(t, dir, "--feature-gates", "HighAvailability=true"); !codes[redpanda.LintCodeReplicationBelowBrokers] {
			t.Errorf("expected %s to fire; got %v", redpanda.LintCodeReplicationBelowBrokers, codes)
		}
	})

	t.Run("negative: replication matches broker count", func(t *testing.T) {
		dir := writeManifests(t, map[string]string{"m.yaml": fixture("  replication: 3\n")})
		if codes := lintCodes(t, dir, "--feature-gates", "HighAvailability=true"); codes[redpanda.LintCodeReplicationBelowBrokers] {
			t.Errorf("expected %s NOT to fire (replication == brokers); got %v", redpanda.LintCodeReplicationBelowBrokers, codes)
		}
	})
}

// --- s3sink ----------------------------------------------------------------

const s3sinkFixtureCommon = `
apiVersion: datascape.io/v1alpha1
kind: SecretReference
metadata:
  name: creds
spec:
  backend: env
  keys: [accessKey, secretKey]
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: broker
spec:
  type: redpanda
  runtime: {type: fake}
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: lake
spec:
  type: noop
  runtime: {type: fake}
---
apiVersion: datascape.io/v1alpha1
kind: Provider
metadata:
  name: sink
spec:
  type: s3sink
  runtime: {type: fake}
  configuration:
    image: "datascape-s3sink-connect:local"
    credentialsSecretRef: creds
  secretRefs: [creds]
---
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: es1
spec:
  providerRef: {name: broker}
---
apiVersion: datascape.io/v1alpha1
kind: EventStream
metadata:
  name: es2
spec:
  providerRef: {name: broker}
`

func s3sinkDatasetsAndBindings(prefix1, prefix2 string) string {
	return `
apiVersion: datascape.io/v1alpha1
kind: Dataset
metadata:
  name: ds1
spec:
  providerRef: {name: lake}
  bucket: raw
  prefix: ` + prefix1 + `
  format: json
  deletionPolicy: retain
---
apiVersion: datascape.io/v1alpha1
kind: Dataset
metadata:
  name: ds2
spec:
  providerRef: {name: lake}
  bucket: raw
  prefix: ` + prefix2 + `
  format: json
  deletionPolicy: retain
---
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: sink1
spec:
  mode: sink
  sourceRef: {name: es1}
  targetRef: {name: ds1}
  providerRef: {name: sink}
---
apiVersion: datascape.io/v1alpha1
kind: Binding
metadata:
  name: sink2
spec:
  mode: sink
  sourceRef: {name: es2}
  targetRef: {name: ds2}
  providerRef: {name: sink}
`
}

func TestS3SinkPrefixHierarchyCollisionLint(t *testing.T) {
	t.Run("positive: one prefix contains the other", func(t *testing.T) {
		dir := writeManifests(t, map[string]string{
			"common.yaml":  s3sinkFixtureCommon,
			"targets.yaml": s3sinkDatasetsAndBindings("events/", "events/raw/"),
		})
		if codes := lintCodes(t, dir); !codes[s3sink.LintCodePrefixHierarchyCollision] {
			t.Errorf("expected %s to fire; got %v", s3sink.LintCodePrefixHierarchyCollision, codes)
		}
	})

	t.Run("negative: disjoint prefixes", func(t *testing.T) {
		dir := writeManifests(t, map[string]string{
			"common.yaml":  s3sinkFixtureCommon,
			"targets.yaml": s3sinkDatasetsAndBindings("events/", "logs/"),
		})
		if codes := lintCodes(t, dir); codes[s3sink.LintCodePrefixHierarchyCollision] {
			t.Errorf("expected %s NOT to fire (disjoint prefixes); got %v", s3sink.LintCodePrefixHierarchyCollision, codes)
		}
	})
}

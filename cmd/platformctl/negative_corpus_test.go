package main

import (
	"path/filepath"
	"strings"
	"testing"
)

// TestNegativeCorpus is the docs/planning/08 E5 exit-criterion evidence:
// "every schema-legal misconfiguration class in the negative-test corpus
// produces a named, actionable validate error." Each
// testdata/negative-corpus/*.yaml fixture is schema-legal against the core
// Kind schemas (schemas/v1alpha1/*.json — deliberately open,
// additionalProperties: true/an open engine or options block) but violates
// exactly one provider-owned fragment or SpecValidator/BindingOptionsValidator
// cross-field rule, mined from the providers' own reconcile-time error paths
// (doc 07 §3.1) plus the two real reconcile-time-only gaps this task found
// (Source engine blocks' missing "database", see fragment.go's doc comment
// and docs/planning/03 §5.2). "Named, actionable" is checked as: `validate`
// exits non-zero, and the error text names both the offending resource
// (Kind + metadata.name) and the specific field/value at fault.
func TestNegativeCorpus(t *testing.T) {
	t.Parallel()
	cases := []struct {
		file  string
		gates []string // extra --feature-gates entries, beyond the CLI's own defaults
		want  []string // substrings the error must contain (resource name + field/value)
	}{
		{
			file: "redpanda-schemaregistry-typo.yaml",
			want: []string{"broker", "schemaRegistry"},
		},
		{
			file: "redpanda-brokers-not-integer.yaml",
			want: []string{"broker", "brokers"},
		},
		{
			file: "redpanda-unknown-key-typo.yaml",
			want: []string{"broker", "kafkaPrt"},
		},
		{
			file: "postgres-metrics-typo.yaml",
			want: []string{"db", "metrics"},
		},
		{
			file: "mysql-metrics-typo.yaml",
			want: []string{"db", "metrics"},
		},
		{
			file: "debezium-workers-not-integer.yaml",
			want: []string{"cdc", "workers"},
		},
		{
			file: "s3-nodes-unsupported-topology.yaml",
			want: []string{"store", "nodes", "topology"},
		},
		{
			file: "s3sink-missing-image.yaml",
			want: []string{"sink", "image"},
		},
		{
			file:  "jdbcsink-missing-image.yaml",
			gates: []string{"JDBCSinkProvider=true"},
			want:  []string{"jsink", "image"},
		},
		{
			file:  "s3source-missing-credentialssecretref.yaml",
			gates: []string{"IngestProvider=true"},
			want:  []string{"ssrc", "credentialsSecretRef"},
		},
		{
			file:  "wireguard-missing-required-fields.yaml",
			gates: []string{"TunnelProvider=true"},
			want:  []string{"tun", "peerPublicKey"},
		},
		{
			file:  "nessie-catalog-unknown-key-typo.yaml",
			gates: []string{"NessieProvider=true"},
			want:  []string{"lakehouse-catalog", "defaultBrnach"},
		},
		{
			file: "source-postgres-missing-database.yaml",
			want: []string{"students", "database"},
		},
		{
			file: "source-mysql-missing-database.yaml",
			want: []string{"orders", "database"},
		},
		{
			file: "source-mariadb-missing-database.yaml",
			want: []string{"orders", "database"},
		},
		{
			file: "binding-cdc-debezium-snapshotmode-typo.yaml",
			want: []string{"students-to-events", "snapshotMode"},
		},
		{
			file: "binding-sink-s3sink-format-typo.yaml",
			want: []string{"events-to-raw", "format"},
		},
		{
			file:  "binding-sink-jdbcsink-format-missing.yaml",
			gates: []string{"JDBCSinkProvider=true"},
			want:  []string{"events-to-warehouse", "format"},
		},
		{
			file:  "binding-ingest-s3source-unknown-key-typo.yaml",
			gates: []string{"IngestProvider=true"},
			want:  []string{"landing-to-events", "convertor"},
		},
		{
			file: "binding-cdc-debezium-tables-empty.yaml",
			want: []string{"students-to-events", "tables"},
		},
	}

	for _, c := range cases {
		t.Run(c.file, func(t *testing.T) {
			path := filepath.Join("testdata", "negative-corpus", c.file)
			args := []string{"validate", path}
			if len(c.gates) > 0 {
				args = append(args, "--feature-gates", strings.Join(c.gates, ","))
			}
			out, err, code := run(t, args...)
			if err == nil || code == 0 {
				t.Fatalf("validate %s: want a non-zero-exit error, got code=%d out=%s", c.file, code, out)
			}
			msg := err.Error()
			for _, want := range c.want {
				if !strings.Contains(msg, want) {
					t.Errorf("validate %s: error does not contain %q\ngot: %s", c.file, want, msg)
				}
			}
		})
	}
}

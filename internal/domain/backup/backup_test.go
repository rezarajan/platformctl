package backup

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestManifestNeverEmbedsPlaintextCredentials pins docs/planning/08 C6's
// accept criterion mechanically, not just by convention: a Location's
// credentials must not survive into a Manifest under any serialization a
// caller (the CLI's -o json|yaml, or a future log line) might use.
func TestManifestNeverEmbedsPlaintextCredentials(t *testing.T) {
	const accessKey = "AKIAVERYSECRETACCESSKEY"
	const secretKey = "super-secret-key-value-do-not-leak"
	loc := Location{
		Endpoint:  "http://s3:9000",
		Bucket:    "backups",
		Prefix:    "orders",
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
	m := Manifest{
		Kind:         "Source",
		Name:         "orders",
		Namespace:    "default",
		ProviderType: "postgres",
		Format:       "postgres/pg_dump-plain",
		Destination:  RefOf(loc, "orders/orders-20260101T000000Z.sql"),
	}

	jsonBytes, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if strings.Contains(string(jsonBytes), accessKey) || strings.Contains(string(jsonBytes), secretKey) {
		t.Fatalf("Manifest JSON embeds plaintext credentials:\n%s", jsonBytes)
	}

	yamlBytes, err := yaml.Marshal(m)
	if err != nil {
		t.Fatalf("yaml.Marshal: %v", err)
	}
	if strings.Contains(string(yamlBytes), accessKey) || strings.Contains(string(yamlBytes), secretKey) {
		t.Fatalf("Manifest YAML embeds plaintext credentials:\n%s", yamlBytes)
	}

	// Ref itself (what Destination holds) has no field capable of carrying
	// a credential at all — structurally, not just by this instance's data.
	refFields := []string{"Endpoint", "Bucket", "Key", "Prefix"}
	gotFields := structFieldNames(m.Destination)
	if !equalSets(refFields, gotFields) {
		t.Fatalf("backup.Ref fields = %v, want exactly %v (a new field here needs the same no-credentials scrutiny)", gotFields, refFields)
	}
}

func structFieldNames(v any) []string {
	b, _ := json.Marshal(v)
	var m map[string]json.RawMessage
	_ = json.Unmarshal(b, &m)
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func equalSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]bool, len(a))
	for _, s := range a {
		set[strings.ToLower(s)] = true
	}
	for _, s := range b {
		if !set[strings.ToLower(s)] {
			return false
		}
	}
	return true
}

package debezium

import "testing"

// TestApplyConverterConfigJSON covers the default/unset format: unchanged
// pre-D1 behavior (JsonConverter, schemas disabled), no registry needed.
func TestApplyConverterConfigJSON(t *testing.T) {
	for _, format := range []string{"", "json"} {
		config := map[string]string{}
		if err := applyConverterConfig(config, format, "", ""); err != nil {
			t.Fatalf("format %q: %v", format, err)
		}
		if config["key.converter"] != "org.apache.kafka.connect.json.JsonConverter" {
			t.Errorf("format %q: key.converter = %q", format, config["key.converter"])
		}
		if config["key.converter.schemas.enable"] != "false" {
			t.Errorf("format %q: schemas.enable = %q, want false", format, config["key.converter.schemas.enable"])
		}
		if _, ok := config["key.converter.schema.registry.url"]; ok {
			t.Errorf("format %q: unexpected schema.registry.url key set for json", format)
		}
	}
}

// TestApplyConverterConfigAvro covers docs/planning/08 D1's Avro path: the
// Confluent Avro converter class, wired to the registry URL the engine
// resolved (never a guessed address) — and a clear error, not a silently
// wrong config, when the registry URL is empty (dependency-graph ordering
// should prevent this in practice; this is the defensive fallback).
func TestApplyConverterConfigAvro(t *testing.T) {
	config := map[string]string{}
	if err := applyConverterConfig(config, "avro", "", "http://kafka-cluster:8081"); err != nil {
		t.Fatalf("applyConverterConfig: %v", err)
	}
	if config["key.converter"] != "io.confluent.connect.avro.AvroConverter" {
		t.Errorf("key.converter = %q", config["key.converter"])
	}
	if config["value.converter"] != "io.confluent.connect.avro.AvroConverter" {
		t.Errorf("value.converter = %q", config["value.converter"])
	}
	if config["key.converter.schema.registry.url"] != "http://kafka-cluster:8081" {
		t.Errorf("key.converter.schema.registry.url = %q", config["key.converter.schema.registry.url"])
	}
	if config["value.converter.schema.registry.url"] != "http://kafka-cluster:8081" {
		t.Errorf("value.converter.schema.registry.url = %q", config["value.converter.schema.registry.url"])
	}
	if _, ok := config["key.converter.schemas.enable"]; ok {
		t.Error("schemas.enable should not be set for avro (implicit in the converter)")
	}

	if err := applyConverterConfig(map[string]string{}, "avro", "", ""); err == nil {
		t.Error("want an error when format is avro but no registry URL resolved")
	}
}

// TestApplyConverterConfigProtobuf mirrors the avro case for protobuf.
func TestApplyConverterConfigProtobuf(t *testing.T) {
	config := map[string]string{}
	if err := applyConverterConfig(config, "protobuf", "", "http://kafka-cluster:8081"); err != nil {
		t.Fatalf("applyConverterConfig: %v", err)
	}
	if config["key.converter"] != "io.confluent.connect.protobuf.ProtobufConverter" {
		t.Errorf("key.converter = %q", config["key.converter"])
	}
	if err := applyConverterConfig(map[string]string{}, "protobuf", "", ""); err == nil {
		t.Error("want an error when format is protobuf but no registry URL resolved")
	}
}

// TestApplyConverterConfigConverterOverride: an explicit options.converter
// wins over the format-derived default class, for both json and
// schema-carrying formats.
func TestApplyConverterConfigConverterOverride(t *testing.T) {
	config := map[string]string{}
	if err := applyConverterConfig(config, "avro", "com.example.CustomAvroConverter", "http://kafka-cluster:8081"); err != nil {
		t.Fatalf("applyConverterConfig: %v", err)
	}
	if config["key.converter"] != "com.example.CustomAvroConverter" || config["value.converter"] != "com.example.CustomAvroConverter" {
		t.Errorf("converter override not applied: %+v", config)
	}
}

// TestApplyConverterConfigUnknownFormat: a format outside json/avro/protobuf
// is rejected — compatibility.Check should already have caught this at
// validate time, but the provider defends its own invariant too.
func TestApplyConverterConfigUnknownFormat(t *testing.T) {
	if err := applyConverterConfig(map[string]string{}, "xml", "", ""); err == nil {
		t.Error("want an error for an unrecognized format")
	}
}

// TestValidateBindingOptionsFormat covers the shape half of docs/planning/08
// D1 (registry availability is compatibility.Check's job, not this one's).
func TestValidateBindingOptionsFormat(t *testing.T) {
	p := New()
	for _, format := range []string{"json", "avro", "protobuf"} {
		if err := p.ValidateBindingOptions("cdc", map[string]any{"format": format}); err != nil {
			t.Errorf("format %q rejected: %v", format, err)
		}
	}
	if err := p.ValidateBindingOptions("cdc", map[string]any{"format": "xml"}); err == nil {
		t.Error("want an error for an unrecognized options.format")
	}
	if err := p.ValidateBindingOptions("cdc", map[string]any{"converter": ""}); err == nil {
		t.Error("want an error for an empty options.converter")
	}
	if err := p.ValidateBindingOptions("cdc", map[string]any{"converter": "com.example.Custom"}); err != nil {
		t.Errorf("valid options.converter rejected: %v", err)
	}
}

// TestServerIDUniquePerConnector guards docs/planning/07 §2.2: two MySQL
// connectors on the same server must not share a replication server id (the
// previous formula was constant per engine, so they kicked each other's
// binlog session off).
func TestServerIDUniquePerConnector(t *testing.T) {
	a := serverID("orders-cdc")
	b := serverID("customers-cdc")
	if a == b {
		t.Fatalf("serverID collided for distinct connectors: %d", a)
	}
	if a < 100000 || b < 100000 {
		t.Errorf("serverID below floor: %d, %d", a, b)
	}
	if a != serverID("orders-cdc") {
		t.Error("serverID not deterministic for the same name")
	}
}

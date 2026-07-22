package compose

import "fmt"

// renderBrokerProvider renders a new managed Redpanda broker Provider.
func renderBrokerProvider(command, name string) string {
	explain := "Managed Redpanda broker. No image/port configured — the default\npinned image and an auto-assigned host port are used."
	lines := []string{"type: redpanda", "runtime:", "  type: docker"}
	return renderDoc(command, explain, "Provider", name, lines)
}

// renderEventStream renders a new EventStream on brokerName.
func renderEventStream(command, name, brokerName string, partitions int, retention string) string {
	var lines []string
	lines = append(lines, refBlock("providerRef", brokerName)...)
	lines = append(lines, fmt.Sprintf("partitions: %d", partitions))
	lines = append(lines, "retention:", "  duration: "+retention)
	return renderDoc(command, "", "EventStream", name, lines)
}

// renderCDCWorkerProvider renders a new managed Debezium Connect worker.
// spec.configuration.bootstrapServers is deliberately omitted — inferred
// from the manifest graph the same way the cdc-to-lake blueprint's
// provider-cdc.yaml is (docs/planning/08 E2,
// compatibility.ResolveKafkaBootstrapAddress): correct as soon as this
// worker is named by exactly one Binding whose EventStream endpoint
// resolves to one broker address.
func renderCDCWorkerProvider(command, name, replSecretRef string) string {
	explain := "The Debezium Kafka Connect worker realizing the cdc Binding below.\nspec.configuration.bootstrapServers is omitted: inferred from the\nmanifest graph."
	lines := []string{
		"type: debezium",
		"runtime:",
		"  type: docker",
		"configuration:",
		"  replicationSecretRef: " + replSecretRef,
		"secretRefs: " + flowList([]string{replSecretRef}),
	}
	return renderDoc(command, explain, "Provider", name, lines)
}

// renderCDCBinding renders a cdc-mode Binding from sourceName into
// streamName, realized by workerName.
func renderCDCBinding(command, name, sourceName, streamName, workerName string, tables []string, snapshotMode string) string {
	var lines []string
	lines = append(lines, "mode: cdc")
	lines = append(lines, refBlock("sourceRef", sourceName)...)
	lines = append(lines, refBlock("targetRef", streamName)...)
	lines = append(lines, refBlock("providerRef", workerName)...)
	lines = append(lines, "options:", "  tables: "+flowList(tables), "  snapshotMode: "+snapshotMode)
	return renderDoc(command, "", "Binding", name, lines)
}

// renderLakeProvider renders a new managed MinIO object store.
func renderLakeProvider(command, name, rootSecretRef string) string {
	explain := "Managed MinIO — the object store the sink Binding lands objects in."
	lines := []string{
		"type: minio",
		"runtime:",
		"  type: docker",
		"configuration:",
		"  rootSecretRef: " + rootSecretRef,
		"secretRefs: " + flowList([]string{rootSecretRef}),
	}
	return renderDoc(command, explain, "Provider", name, lines)
}

// renderSinkWorkerProvider renders a new managed s3sink Connect worker.
// Unlike debezium, no stock Connect image ships the S3 sink plugin, so
// spec.configuration.image has no usable default (mirrors the cdc-to-lake
// blueprint's provider-sink.yaml exactly).
func renderSinkWorkerProvider(command, name, lakeSecretRef string) string {
	explain := "The S3-sink Kafka Connect worker realizing the sink Binding below.\n" +
		"No stock Connect image ships the S3 sink plugin, so\n" +
		"spec.configuration.image is required — build one (see\n" +
		"internal/application/blueprint/templates/cdc-to-lake/s3sink-image)\n" +
		"and set it here before `platformctl apply`."
	lines := []string{
		"type: s3sink",
		"runtime:",
		"  type: docker",
		"configuration:",
		"  image: datascape-s3sink-connect:local",
		"  credentialsSecretRef: " + lakeSecretRef,
		"secretRefs: " + flowList([]string{lakeSecretRef}),
	}
	return renderDoc(command, explain, "Provider", name, lines)
}

// renderDataset renders a new Dataset on lakeProviderName. prefix is
// quoted (%q) even when empty — an empty unquoted YAML scalar decodes as
// null, which the Dataset schema rejects (spec.prefix must be a string).
func renderDataset(command, name, lakeProviderName, bucket, prefix, format string) string {
	var lines []string
	lines = append(lines, refBlock("providerRef", lakeProviderName)...)
	lines = append(lines, "bucket: "+bucket, fmt.Sprintf("prefix: %q", prefix), "format: "+format)
	return renderDoc(command, "", "Dataset", name, lines)
}

// renderSinkBinding renders a sink-mode Binding from streamName into
// targetName (a Dataset or, for the database-sink pairing, a Source),
// realized by workerName.
func renderSinkBinding(command, name, streamName, targetName, workerName string) string {
	return renderBinding(command, "sink", name, streamName, targetName, workerName)
}

// renderBinding renders a mode/sourceRef/targetRef/providerRef Binding with
// no options block — every mode wire.go generates besides cdc (which needs
// its own options.tables/snapshotMode) shares this exact shape.
func renderBinding(command, mode, name, sourceName, targetName, providerName string) string {
	var lines []string
	lines = append(lines, "mode: "+mode)
	lines = append(lines, refBlock("sourceRef", sourceName)...)
	lines = append(lines, refBlock("targetRef", targetName)...)
	lines = append(lines, refBlock("providerRef", providerName)...)
	return renderDoc(command, "", "Binding", name, lines)
}

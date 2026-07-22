// Package compatibility implements the Binding mode↔Kind rules and provider
// capability checks — the concrete mechanism behind FR-18, run at
// validate/plan time before anything is scheduled.
// See docs/planning/02-architecture.md §5.2.
package compatibility

import (
	"fmt"
	"sort"
	"strings"

	"github.com/rezarajan/platformctl/internal/domain/binding"
	"github.com/rezarajan/platformctl/internal/domain/catalog"
	"github.com/rezarajan/platformctl/internal/domain/connection"
	"github.com/rezarajan/platformctl/internal/domain/dataset"
	"github.com/rezarajan/platformctl/internal/domain/eventstream"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/domain/source"
	"github.com/rezarajan/platformctl/internal/ports/reconciler"
)

// ProviderResolver returns the reconciler implementation for a provider type.
// Abstracted so validate can run without constructing real adapters beyond
// their capability declarations.
type ProviderResolver func(providerType string) (reconciler.Provider, error)

// Check validates every Binding in the set: structural Kind pairing first,
// then provider capability. Error messages match the format specified in
// docs/planning/02-architecture.md §5.2 exactly.
func Check(envelopes []resource.Envelope, resolve ProviderResolver) error {
	idx := newIndex(envelopes)
	for _, e := range envelopes {
		if err := validateNameRefs(e); err != nil {
			return err
		}
	}
	if err := checkResourceCapabilities(envelopes, idx, resolve); err != nil {
		return err
	}

	for _, e := range envelopes {
		if e.Kind != "Binding" {
			continue
		}
		b, err := binding.FromEnvelope(e)
		if err != nil {
			return err
		}
		bindingName := e.Metadata.Name

		pairs, ok := binding.AllowedKindPairs[b.Mode]
		if !ok {
			return fmt.Errorf("Binding %q: mode %q has no allowed Kind pairing", bindingName, b.Mode)
		}

		srcEnv, ok, ambiguous := idx.resolve(e, resource.RefFromSpec(e.Spec, "sourceRef"))
		if ambiguous {
			return fmt.Errorf("Binding %q: sourceRef %q is ambiguous in namespace %q", bindingName, b.SourceRef, resource.RefNamespace(e.Spec, "sourceRef", e.Metadata.Namespace))
		}
		if !ok {
			return fmt.Errorf("Binding %q: sourceRef %q does not resolve to any resource in namespace %q", bindingName, b.SourceRef, resource.RefNamespace(e.Spec, "sourceRef", e.Metadata.Namespace))
		}
		tgtEnv, ok, ambiguous := idx.resolve(e, resource.RefFromSpec(e.Spec, "targetRef"))
		if ambiguous {
			return fmt.Errorf("Binding %q: targetRef %q is ambiguous in namespace %q", bindingName, b.TargetRef, resource.RefNamespace(e.Spec, "targetRef", e.Metadata.Namespace))
		}
		if !ok {
			return fmt.Errorf("Binding %q: targetRef %q does not resolve to any resource in namespace %q", bindingName, b.TargetRef, resource.RefNamespace(e.Spec, "targetRef", e.Metadata.Namespace))
		}
		matched := false
		var pair binding.KindPair
		for _, p := range pairs {
			if srcEnv.Kind == p.SourceKind && tgtEnv.Kind == p.TargetKind {
				pair, matched = p, true
				break
			}
		}
		if !matched {
			allowed := make([]string, len(pairs))
			for i, p := range pairs {
				allowed[i] = p.SourceKind + "->" + p.TargetKind
			}
			return fmt.Errorf("Binding %q: mode %q does not connect %s %q to %s %q (allowed pairings: %s)",
				bindingName, b.Mode, srcEnv.Kind, b.SourceRef, tgtEnv.Kind, b.TargetRef, strings.Join(allowed, ", "))
		}

		provEnv, ok := idx.resolveKind(e, resource.RefFromSpec(e.Spec, "providerRef"), "Provider")
		if !ok || provEnv.Kind != "Provider" {
			return fmt.Errorf("Binding %q: providerRef %q does not resolve to a Provider in namespace %q", bindingName, b.ProviderRef, resource.RefNamespace(e.Spec, "providerRef", e.Metadata.Namespace))
		}
		p, err := provider.FromEnvelope(provEnv)
		if err != nil {
			return err
		}
		impl, err := resolve(p.Type)
		if err != nil {
			return fmt.Errorf("Binding %q: %w", bindingName, err)
		}

		// Capability is checked per matched pairing: the same mode makes
		// different demands of a provider depending on the endpoint kinds.
		// datasetSinkFormat feeds checkSchemaFormat below (docs/planning/08
		// D2): only the sink-to-Dataset pairing sets it, since only that
		// pairing has a Dataset.spec.format to check against the stream's
		// registry availability.
		var datasetSinkFormat string
		switch {
		case b.Mode == binding.ModeCDC:
			src, err := source.FromEnvelope(srcEnv)
			if err != nil {
				return err
			}
			cdc, ok := impl.(reconciler.CDCCapableProvider)
			if !ok {
				return fmt.Errorf("Binding %q: Provider %q (type: %s)\ndoes not support mode \"cdc\" (provider implements no CDC capability)", bindingName, b.ProviderRef, p.Type)
			}
			engines := cdc.SupportedSourceEngines()
			if !contains(engines, src.Engine) {
				return fmt.Errorf("Binding %q: Provider %q (type: %s)\ndoes not support source engine %q (supported: %s)", bindingName, b.ProviderRef, p.Type, src.Engine, joinSorted(engines))
			}
		case b.Mode == binding.ModeSink && pair.TargetKind == "Dataset":
			ds, err := dataset.FromEnvelope(tgtEnv)
			if err != nil {
				return err
			}
			sink, ok := impl.(reconciler.SinkCapableProvider)
			if !ok {
				return fmt.Errorf("Binding %q: Provider %q (type: %s)\ndoes not support mode \"sink\" (provider implements no sink capability)", bindingName, b.ProviderRef, p.Type)
			}
			formats := sink.SupportedSinkFormats()
			if !contains(formats, ds.Format) {
				return fmt.Errorf("Binding %q: Provider %q (type: %s)\ndoes not support sink format %q (supported: %s)", bindingName, b.ProviderRef, p.Type, ds.Format, joinSorted(formats))
			}
			datasetSinkFormat = ds.Format
		case b.Mode == binding.ModeSink && pair.TargetKind == "Source":
			src, err := source.FromEnvelope(tgtEnv)
			if err != nil {
				return err
			}
			dbSink, ok := impl.(reconciler.DatabaseSinkCapableProvider)
			if !ok {
				return fmt.Errorf("Binding %q: Provider %q (type: %s)\ndoes not support mode \"sink\" into a Source (provider implements no database-sink capability)", bindingName, b.ProviderRef, p.Type)
			}
			engines := dbSink.SupportedSinkEngines()
			if !contains(engines, src.Engine) {
				return fmt.Errorf("Binding %q: Provider %q (type: %s)\ndoes not support sink engine %q (supported: %s)", bindingName, b.ProviderRef, p.Type, src.Engine, joinSorted(engines))
			}
		case b.Mode == binding.ModeIngest:
			ds, err := dataset.FromEnvelope(srcEnv)
			if err != nil {
				return err
			}
			ing, ok := impl.(reconciler.IngestCapableProvider)
			if !ok {
				return fmt.Errorf("Binding %q: Provider %q (type: %s)\ndoes not support mode \"ingest\" (provider implements no ingest capability)", bindingName, b.ProviderRef, p.Type)
			}
			formats := ing.SupportedIngestFormats()
			if !contains(formats, ds.Format) {
				return fmt.Errorf("Binding %q: Provider %q (type: %s)\ndoes not support ingest format %q (supported: %s)", bindingName, b.ProviderRef, p.Type, ds.Format, joinSorted(formats))
			}
		}

		// Schema-carrying options.format (docs/planning/08 D1) and, since D2,
		// a parquet-format Dataset sink target: both checked against the
		// EventStream endpoint's own realizing Provider, not b.ProviderRef —
		// registry availability (configuration.schemaRegistry: enabled) is a
		// fact of the stream backend a CDC/sink connector writes into or
		// reads from, not a capability of the connector itself. Purely a
		// manifest-declared, forward-looking check (no live infra touched) —
		// the same "validate before apply schedules anything" contract as
		// every other capability check here.
		if err := checkSchemaFormat(bindingName, srcEnv, tgtEnv, b, datasetSinkFormat, idx, resolve); err != nil {
			return err
		}

		// spec.options.deadLetter.stream must resolve to an EventStream
		// in-graph (docs/planning/08 D6). binding.FromEnvelope already
		// refused deadLetter on a non-sink Binding and enforced its shape
		// (stream required, tolerance ∈ {all,none}) — this is the one
		// check only compatibility.Check can make: whether the named
		// stream actually exists in this manifest set.
		if err := checkDeadLetterQueue(bindingName, e, b, idx); err != nil {
			return err
		}

		// Provider-specific option-block validation (the Binding half of the
		// SpecValidator DX contract): apply-time-only option errors are
		// validate-time regressions.
		if bv, ok := impl.(reconciler.BindingOptionsValidator); ok {
			if err := bv.ValidateBindingOptions(string(b.Mode), b.Options); err != nil {
				return fmt.Errorf("Binding %q: %w", bindingName, err)
			}
		}
	}
	return nil
}

// checkSchemaFormat validates two related, EventStream-registry-backed
// facts against the EventStream endpoint's own realizing Provider —
// whichever of srcEnv/tgtEnv resolved to Kind EventStream, role-neutral per
// docs/planning/03 §7.1 — never b.ProviderRef, since registry availability
// is a fact of the stream backend a CDC/sink connector writes into or reads
// from, not a capability of the connector itself:
//
//  1. (docs/planning/08 D1) a Binding's spec.options.format, when
//     schema-carrying (avro, protobuf): the EventStream's Provider must
//     support that exact format.
//  2. (docs/planning/08 D2) datasetSinkFormat == "parquet" — set by the
//     caller only for the sink-to-Dataset pairing: the Aiven S3 sink
//     connector's parquet writer requires schema-carrying Connect records
//     (SinkCapableProvider.SupportedSinkFormats' doc comment on s3sink),
//     which this platform only wires via the registry-backed Avro
//     converters D1 introduced — so a parquet Dataset target requires the
//     EventStream's Provider to support "avro", exactly like case 1 but
//     keyed on Dataset.spec.format instead of Binding.spec.options.format.
//     format.output.type values other than parquet (json, jsonl, csv) are
//     schemaless and impose no registry requirement.
//
// Neither check fires for an unset/json options.format with a non-parquet
// (or absent) datasetSinkFormat — not a schema-registry concern, left to the
// realizing provider's own ValidateBindingOptions, exactly as before D1/D2.
// This keeps the SchemaRegistrySupport gate boundary clean: gate off ⇒ zero
// behavior change for manifests that don't use avro/protobuf/parquet (doc 02
// §11's Alpha/disabled convention).
func checkSchemaFormat(bindingName string, srcEnv, tgtEnv resource.Envelope, b binding.Binding, datasetSinkFormat string, idx manifestIndex, resolve ProviderResolver) error {
	format, _ := b.Options["format"].(string)
	schemaCarrying := format == "avro" || format == "protobuf"
	parquetSink := datasetSinkFormat == "parquet"
	if !schemaCarrying && !parquetSink {
		return nil
	}

	var esEnv resource.Envelope
	switch {
	case srcEnv.Kind == "EventStream":
		esEnv = srcEnv
	case tgtEnv.Kind == "EventStream":
		esEnv = tgtEnv
	default:
		// No EventStream endpoint on this Binding at all (shouldn't happen
		// for any mode this version implements — every allowed pairing has
		// one) — nothing to check the format against.
		return nil
	}

	esProvEnv, ok := idx.resolveKind(esEnv, resource.RefFromSpec(esEnv.Spec, "providerRef"), "Provider")
	if !ok || esProvEnv.Kind != "Provider" {
		return fmt.Errorf("Binding %q: EventStream %q providerRef does not resolve to a Provider in namespace %q", bindingName, esEnv.Metadata.Name, resource.RefNamespace(esEnv.Spec, "providerRef", esEnv.Metadata.Namespace))
	}
	esProv, err := provider.FromEnvelope(esProvEnv)
	if err != nil {
		return err
	}
	esImpl, err := resolve(esProv.Type)
	if err != nil {
		return fmt.Errorf("Binding %q: %w", bindingName, err)
	}

	supported := []string{"json"}
	if schemaCapable, ok := esImpl.(reconciler.SchemaRegistryCapableProvider); ok {
		supported = schemaCapable.SupportedSchemaFormats(esProv)
	}
	if schemaCarrying && !contains(supported, format) {
		return fmt.Errorf("Binding %q: Provider %q (type: %s)\ndoes not support format %q (supported: %s)",
			bindingName, esProvEnv.Metadata.Name, esProv.Type, format, joinSorted(supported))
	}
	if parquetSink && !contains(supported, "avro") {
		return fmt.Errorf("Binding %q: Provider %q (type: %s)\ndoes not support format %q (supported: %s)",
			bindingName, esProvEnv.Metadata.Name, esProv.Type, "parquet", joinSorted(supported))
	}
	return nil
}

// checkDeadLetterQueue is docs/planning/08 D6's structural half: a
// sink-mode Binding's spec.options.deadLetter.stream must name an
// EventStream that actually exists in this manifest set. It resolves the
// same way sourceRef/targetRef do (idx.resolve, namespace-defaulted to the
// Binding's own), and the error message matches that exact family
// ("Binding %q: <field> %q does not resolve to <kind> in namespace %q").
//
// Ordering note, recorded here per this task's own instruction (deviation
// from doc 08's "compatibility resolves the named EventStream in-graph
// with ordering" wording): this is an *existence* check only, not a
// dependency edge. internal/domain/graph.Build's refFields
// (providerRef/sourceRef/targetRef/connectionRef/secretRef) are a fixed,
// generic, top-level list shared uniformly by every Kind; deadLetter.stream
// is nested inside spec.options, sink-mode-scoped, and provider-consumed
// (only s3sink translates it today) — special-casing the generic graph
// walker for one nested field of one Kind/mode is exactly the
// "engine-block introspection" this task's own text says to avoid when it
// would require it. Consequence: graph.TopologicalLevels does not
// guarantee the DLQ EventStream reconciles before the sink Binding that
// references it — under ParallelReconciliation they may land in the same
// level. This is safe in practice because Kafka Connect's own framework
// creates the DLQ topic itself (via the worker's internal AdminClient) the
// first time a poison record needs it, using
// errors.deadletterqueue.topic.replication.factor (s3sink.
// applyDeadLetterConfig resolves this from the named EventStream's own
// spec.replication when the engine has it in req.Resources — which it
// always does, Resources is the full validated set regardless of reconcile
// order — else "1"); the platform-managed EventStream's own
// partition/retention config "wins" once it reconciles (same apply or a
// later one). A future task wiring destructive/ordering intent through
// reconciler.Request could upgrade this to a real edge; until then this is
// a known, documented limitation, not a silent gap.
func checkDeadLetterQueue(bindingName string, e resource.Envelope, b binding.Binding, idx manifestIndex) error {
	if b.DeadLetter == nil {
		return nil
	}
	ref := resource.NameRef{Name: b.DeadLetter.Stream}
	dlqEnv, ok, ambiguous := idx.resolve(e, ref, "EventStream")
	if ambiguous {
		return fmt.Errorf("Binding %q: options.deadLetter.stream %q is ambiguous in namespace %q", bindingName, b.DeadLetter.Stream, resource.NormalizeNamespace(e.Metadata.Namespace))
	}
	if !ok || dlqEnv.Kind != "EventStream" {
		return fmt.Errorf("Binding %q: options.deadLetter.stream %q does not resolve to an EventStream in namespace %q", bindingName, b.DeadLetter.Stream, resource.NormalizeNamespace(e.Metadata.Namespace))
	}
	return nil
}

// checkResourceCapabilities validates the provider capability behind
// non-Binding, engine-discriminated kinds: a Catalog's provider must declare
// its engine, a managed Connection's provider its scheme. External resources
// are skipped — nothing realizes them.
func checkResourceCapabilities(envelopes []resource.Envelope, idx manifestIndex, resolve ProviderResolver) error {
	resolveProviderImpl := func(env resource.Envelope, kind, name string, ref resource.NameRef) (reconciler.Provider, string, error) {
		provEnv, ok := idx.resolveKind(env, ref, "Provider")
		if !ok || provEnv.Kind != "Provider" {
			return nil, "", fmt.Errorf("%s %q: providerRef %q does not resolve to a Provider in namespace %q", kind, name, ref.Name, ref.NamespaceOr(env.Metadata.Namespace))
		}
		p, err := provider.FromEnvelope(provEnv)
		if err != nil {
			return nil, "", err
		}
		impl, err := resolve(p.Type)
		if err != nil {
			return nil, "", fmt.Errorf("%s %q: %w", kind, name, err)
		}
		return impl, p.Type, nil
	}

	for _, e := range envelopes {
		if ext, _ := e.Spec["external"].(bool); ext {
			ref := resource.RefFromSpec(e.Spec, "providerRef")
			if ref.Name != "" {
				impl, provType, err := resolveProviderImpl(e, e.Kind, e.Metadata.Name, ref)
				if err != nil {
					return err
				}
				if _, ok := impl.(reconciler.ExternalConfigurer); !ok {
					return fmt.Errorf("%s %q: providerRef %q (type: %s) cannot configure External resources (provider implements no ExternalConfigurer capability)", e.Kind, e.Metadata.Name, ref.Name, provType)
				}
			}
		}
		// connectionRef, wherever it appears, must point at a Connection or
		// a SecretReference (the v1.0.0 shorthand) — anything else resolves
		// at apply time to nothing.
		if ref, ok := e.Spec["connectionRef"].(map[string]any); ok {
			if name, _ := ref["name"].(string); name != "" {
				target, ok, ambiguous := idx.resolve(e, resource.RefFromSpec(e.Spec, "connectionRef"), "Connection", "SecretReference")
				if ambiguous {
					return fmt.Errorf("%s %q: connectionRef %q is ambiguous in namespace %q", e.Kind, e.Metadata.Name, name, resource.RefNamespace(e.Spec, "connectionRef", e.Metadata.Namespace))
				}
				if !ok {
					if wrong, wrongOK, wrongAmbiguous := idx.resolve(e, resource.RefFromSpec(e.Spec, "connectionRef")); wrongAmbiguous {
						return fmt.Errorf("%s %q: connectionRef %q is ambiguous in namespace %q", e.Kind, e.Metadata.Name, name, resource.RefNamespace(e.Spec, "connectionRef", e.Metadata.Namespace))
					} else if wrongOK {
						return fmt.Errorf("%s %q: connectionRef %q must resolve to a Connection or SecretReference, got %s", e.Kind, e.Metadata.Name, name, wrong.Kind)
					}
					return fmt.Errorf("%s %q: connectionRef %q does not resolve to a Connection or SecretReference in namespace %q", e.Kind, e.Metadata.Name, name, resource.RefNamespace(e.Spec, "connectionRef", e.Metadata.Namespace))
				}
				if target.Kind != "Connection" && target.Kind != "SecretReference" {
					return fmt.Errorf("%s %q: connectionRef %q must resolve to a Connection or SecretReference, got %s", e.Kind, e.Metadata.Name, name, target.Kind)
				}
			}
		}

		switch e.Kind {
		case "Provider":
			p, err := provider.FromEnvelope(e)
			if err != nil {
				return err
			}
			impl, err := resolve(p.Type)
			if err != nil {
				return fmt.Errorf("Provider %q: %w", e.Metadata.Name, err)
			}
			if sv, ok := impl.(reconciler.SpecValidator); ok {
				// Configuration-minimization (docs/planning/08 E2): a
				// graph-inferable bootstrapServers satisfies ValidateSpec
				// exactly like an explicit one — the same value
				// reconciler.Request.KafkaBootstrapServers supplies at apply
				// time (engine.resolveRequest) — so validate-time and
				// apply-time completeness stay in lockstep (ADR 011: a
				// manifest that validates must not half-apply). Only
				// synthesized when the field is genuinely absent, and only
				// passed to ValidateSpec, never mutates p itself.
				validated := p
				if _, has := p.Configuration["bootstrapServers"]; !has {
					if addr := ResolveKafkaBootstrapAddress(e, envelopes, resolve); addr != "" {
						cfg := make(map[string]any, len(p.Configuration)+1)
						for k, v := range p.Configuration {
							cfg[k] = v
						}
						cfg["bootstrapServers"] = addr
						validated.Configuration = cfg
					}
				}
				if err := sv.ValidateSpec(validated); err != nil {
					return fmt.Errorf("Provider %q (type: %s): %w", e.Metadata.Name, p.Type, err)
				}
			}
			// A versioned provider's configuration.version must resolve, and
			// an image override requires a pinned version — guaranteed here
			// even if the provider skips it in ValidateSpec.
			if vp, ok := impl.(reconciler.VersionedProvider); ok {
				if err := vp.VersionCatalog(p).ValidateConfig(p.Configuration); err != nil {
					return fmt.Errorf("Provider %q (type: %s): %w", e.Metadata.Name, p.Type, err)
				}
			}
		case "EventStream":
			es, err := eventstream.FromEnvelope(e)
			if err != nil {
				return err
			}
			if es.External || es.ReplicationFactor() <= 1 {
				// Unset/1 is the pre-C2 single-copy default — nothing to
				// bound; a provider without the capability keeps validating
				// exactly as before docs/adr/017.
				continue
			}
			impl, provType, err := resolveProviderImpl(e, "EventStream", e.Metadata.Name, resource.RefFromSpec(e.Spec, "providerRef"))
			if err != nil {
				return err
			}
			if srv, ok := impl.(reconciler.StreamReplicationValidator); ok {
				provEnv, _ := idx.resolveKind(e, resource.RefFromSpec(e.Spec, "providerRef"), "Provider")
				p, err := provider.FromEnvelope(provEnv)
				if err != nil {
					return err
				}
				if err := srv.ValidateStreamReplication(p, es.ReplicationFactor()); err != nil {
					return fmt.Errorf("EventStream %q: Provider %q (type: %s): %w", e.Metadata.Name, es.ProviderRef, provType, err)
				}
			}
		case "Catalog":
			c, err := catalog.FromEnvelope(e)
			if err != nil {
				return err
			}
			if c.External {
				continue
			}
			impl, provType, err := resolveProviderImpl(e, "Catalog", e.Metadata.Name, resource.RefFromSpec(e.Spec, "providerRef"))
			if err != nil {
				return err
			}
			capable, ok := impl.(reconciler.CatalogCapableProvider)
			if !ok {
				return fmt.Errorf("Catalog %q: Provider %q (type: %s)\ndoes not support catalogs (provider implements no catalog capability)", e.Metadata.Name, *c.ProviderRef, provType)
			}
			engines := capable.SupportedCatalogEngines()
			if !contains(engines, c.Engine) {
				return fmt.Errorf("Catalog %q: Provider %q (type: %s)\ndoes not support catalog engine %q (supported: %s)", e.Metadata.Name, *c.ProviderRef, provType, c.Engine, joinSorted(engines))
			}
		case "Connection":
			c, err := connection.FromEnvelope(e)
			if err != nil {
				return err
			}
			if c.SecretRef != nil {
				target, ok := idx.resolveKind(e, resource.RefFromSpec(e.Spec, "secretRef"), "SecretReference")
				if !ok || target.Kind != "SecretReference" {
					return fmt.Errorf("Connection %q: secretRef %q must resolve to a SecretReference in namespace %q", e.Metadata.Name, *c.SecretRef, resource.RefNamespace(e.Spec, "secretRef", e.Metadata.Namespace))
				}
			}
			if c.TLS != nil && c.TLS.SecretRef != nil {
				tlsBlock, _ := e.Spec["tls"].(map[string]any)
				target, ok := idx.resolveKind(e, resource.RefFromSpec(tlsBlock, "secretRef"), "SecretReference")
				if !ok || target.Kind != "SecretReference" {
					return fmt.Errorf("Connection %q: tls.secretRef %q must resolve to a SecretReference in namespace %q", e.Metadata.Name, *c.TLS.SecretRef, resource.RefNamespace(tlsBlock, "secretRef", e.Metadata.Namespace))
				}
			}
			if c.External {
				continue
			}
			impl, provType, err := resolveProviderImpl(e, "Connection", e.Metadata.Name, resource.RefFromSpec(e.Spec, "providerRef"))
			if err != nil {
				return err
			}
			capable, ok := impl.(reconciler.ConnectionCapableProvider)
			if !ok {
				return fmt.Errorf("Connection %q: Provider %q (type: %s)\ndoes not support connections (provider implements no connection capability)", e.Metadata.Name, *c.ProviderRef, provType)
			}
			schemes := capable.SupportedConnectionSchemes()
			if !contains(schemes, c.Scheme) {
				return fmt.Errorf("Connection %q: Provider %q (type: %s)\ndoes not support connection scheme %q (supported: %s)", e.Metadata.Name, *c.ProviderRef, provType, c.Scheme, joinSorted(schemes))
			}
			if c.Via != nil {
				// A structural + capability check only (docs/adr/023's
				// Scope section): the named Provider must exist and be
				// tunnel-capable. Not resolveProviderImpl (that closure's
				// error message names "providerRef" — this is a different
				// field), so a dedicated lookup with its own error text.
				viaRef := resource.RefFromSpec(e.Spec, "via")
				viaProvEnv, ok := idx.resolveKind(e, viaRef, "Provider")
				if !ok || viaProvEnv.Kind != "Provider" {
					return fmt.Errorf("Connection %q: via %q does not resolve to a Provider in namespace %q", e.Metadata.Name, viaRef.Name, viaRef.NamespaceOr(e.Metadata.Namespace))
				}
				viaProv, err := provider.FromEnvelope(viaProvEnv)
				if err != nil {
					return err
				}
				viaImpl, err := resolve(viaProv.Type)
				if err != nil {
					return fmt.Errorf("Connection %q: %w", e.Metadata.Name, err)
				}
				if _, ok := viaImpl.(reconciler.TunnelCapableProvider); !ok {
					return fmt.Errorf("Connection %q: via %q (type: %s)\ndoes not support tunnel chaining (provider implements no tunnel capability)", e.Metadata.Name, *c.Via, viaProv.Type)
				}
			}
		}
	}
	return nil
}

func contains(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func joinSorted(list []string) string {
	sorted := append([]string(nil), list...)
	sort.Strings(sorted)
	return strings.Join(sorted, ", ")
}

type manifestIndex struct {
	byName map[string][]resource.Envelope
	byKey  map[resource.Key]resource.Envelope
}

func newIndex(envelopes []resource.Envelope) manifestIndex {
	idx := manifestIndex{
		byName: make(map[string][]resource.Envelope, len(envelopes)),
		byKey:  make(map[resource.Key]resource.Envelope, len(envelopes)),
	}
	for _, e := range envelopes {
		key := e.Key()
		idx.byKey[key] = e
		idx.byName[indexKey(key.Namespace, key.Name)] = append(idx.byName[indexKey(key.Namespace, key.Name)], e)
	}
	return idx
}

func (idx manifestIndex) resolveKind(from resource.Envelope, ref resource.NameRef, kind string) (resource.Envelope, bool) {
	key := ref.Key(from.Metadata.Namespace, kind)
	env, ok := idx.byKey[key]
	return env, ok
}

func (idx manifestIndex) resolve(from resource.Envelope, ref resource.NameRef, kinds ...string) (resource.Envelope, bool, bool) {
	allowed := map[string]bool{}
	for _, kind := range kinds {
		allowed[kind] = true
	}
	var matches []resource.Envelope
	for _, env := range idx.byName[indexKey(ref.NamespaceOr(from.Metadata.Namespace), ref.Name)] {
		if len(allowed) == 0 || allowed[env.Kind] {
			matches = append(matches, env)
		}
	}
	switch len(matches) {
	case 0:
		return resource.Envelope{}, false, false
	case 1:
		return matches[0], true, false
	default:
		return resource.Envelope{}, false, true
	}
}

func indexKey(namespace, name string) string {
	return resource.NormalizeNamespace(namespace) + "\x00" + name
}

func validateNameRefs(e resource.Envelope) error {
	for _, field := range []string{"providerRef", "sourceRef", "targetRef", "connectionRef", "secretRef"} {
		ref := resource.RefFromSpec(e.Spec, field)
		if ref.Name == "" {
			continue
		}
		if err := resource.ValidateDNSLabel("spec."+field+".name", ref.Name); err != nil {
			return fmt.Errorf("%s: %w", e.Key(), err)
		}
		if ref.Namespace != "" {
			if err := resource.ValidateDNSLabel("spec."+field+".namespace", ref.Namespace); err != nil {
				return fmt.Errorf("%s: %w", e.Key(), err)
			}
		}
	}
	if refs, ok := e.Spec["secretRefs"].([]any); ok {
		for _, item := range refs {
			name, _ := item.(string)
			if name == "" {
				continue
			}
			if err := resource.ValidateDNSLabel("spec.secretRefs.name", name); err != nil {
				return fmt.Errorf("%s: %w", e.Key(), err)
			}
		}
	}
	return nil
}

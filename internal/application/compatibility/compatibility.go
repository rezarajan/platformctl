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
	byName := make(map[string]resource.Envelope)
	for _, e := range envelopes {
		byName[e.Metadata.Name] = e
	}

	if err := checkResourceCapabilities(envelopes, byName, resolve); err != nil {
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

		srcEnv, ok := byName[b.SourceRef]
		if !ok {
			return fmt.Errorf("Binding %q: sourceRef %q does not resolve to any resource", bindingName, b.SourceRef)
		}
		tgtEnv, ok := byName[b.TargetRef]
		if !ok {
			return fmt.Errorf("Binding %q: targetRef %q does not resolve to any resource", bindingName, b.TargetRef)
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

		provEnv, ok := byName[b.ProviderRef]
		if !ok || provEnv.Kind != "Provider" {
			return fmt.Errorf("Binding %q: providerRef %q does not resolve to a Provider", bindingName, b.ProviderRef)
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
	}
	return nil
}

// checkResourceCapabilities validates the provider capability behind
// non-Binding, engine-discriminated kinds: a Catalog's provider must declare
// its engine, a managed Connection's provider its scheme. External resources
// are skipped — nothing realizes them.
func checkResourceCapabilities(envelopes []resource.Envelope, byName map[string]resource.Envelope, resolve ProviderResolver) error {
	resolveProviderImpl := func(kind, name, refName string) (reconciler.Provider, string, error) {
		provEnv, ok := byName[refName]
		if !ok || provEnv.Kind != "Provider" {
			return nil, "", fmt.Errorf("%s %q: providerRef %q does not resolve to a Provider", kind, name, refName)
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
		// connectionRef, wherever it appears, must point at a Connection or
		// a SecretReference (the v1.0.0 shorthand) — anything else resolves
		// at apply time to nothing.
		if ref, ok := e.Spec["connectionRef"].(map[string]any); ok {
			if name, _ := ref["name"].(string); name != "" {
				target := byName[name]
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
				if err := sv.ValidateSpec(p); err != nil {
					return fmt.Errorf("Provider %q (type: %s): %w", e.Metadata.Name, p.Type, err)
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
			impl, provType, err := resolveProviderImpl("Catalog", e.Metadata.Name, *c.ProviderRef)
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
				if target := byName[*c.SecretRef]; target.Kind != "SecretReference" {
					return fmt.Errorf("Connection %q: secretRef %q must resolve to a SecretReference, got %s", e.Metadata.Name, *c.SecretRef, target.Kind)
				}
			}
			if c.External {
				continue
			}
			impl, provType, err := resolveProviderImpl("Connection", e.Metadata.Name, *c.ProviderRef)
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

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

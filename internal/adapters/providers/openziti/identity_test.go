package openziti

import "testing"

func TestIdentityRoleAttributeSanitizesSPIFFEURI(t *testing.T) {
	t.Parallel()
	got := identityRoleAttribute("spiffe://datascape/payments/source/orders-db")
	want := "spiffe-datascape-payments-source-orders-db"
	if got != want {
		t.Errorf("identityRoleAttribute = %q, want %q", got, want)
	}
}

func TestIdentityRoleAttributeIsDeterministic(t *testing.T) {
	t.Parallel()
	uri := "spiffe://datascape/analytics/binding/cdc-orders"
	first := identityRoleAttribute(uri)
	for i := 0; i < 5; i++ {
		if got := identityRoleAttribute(uri); got != first {
			t.Fatalf("not deterministic: call %d = %q, first = %q", i, got, first)
		}
	}
}

func TestIdentityRoleAttributeDistinctForDistinctURIs(t *testing.T) {
	t.Parallel()
	a := identityRoleAttribute("spiffe://datascape/payments/source/orders-db")
	b := identityRoleAttribute("spiffe://datascape/analytics/source/orders-db")
	if a == b {
		t.Fatalf("collision: both URIs mapped to %q", a)
	}
}

func TestLabelRoleAttributeEncoding(t *testing.T) {
	t.Parallel()
	got := labelRoleAttribute("tier", "gold")
	want := "label.tier.gold"
	if got != want {
		t.Errorf("labelRoleAttribute(tier, gold) = %q, want %q", got, want)
	}
}

func TestLabelRoleAttributeSanitizesKeyAndValue(t *testing.T) {
	t.Parallel()
	// A prefixed label key ("example.com/tier") and a value containing
	// underscores/dots both sanitize through the same charset filter
	// identityRoleAttribute uses, never leaking a stray "." from the
	// sanitized segments themselves that could be confused with the
	// "label.<key>.<value>" joiner.
	got := labelRoleAttribute("example.com/tier", "a_b.c")
	want := "label.example-com-tier.a-b-c"
	if got != want {
		t.Errorf("labelRoleAttribute = %q, want %q", got, want)
	}
}

func TestLabelRoleAttributeIsDeterministic(t *testing.T) {
	t.Parallel()
	first := labelRoleAttribute("tier", "gold")
	for i := 0; i < 5; i++ {
		if got := labelRoleAttribute("tier", "gold"); got != first {
			t.Fatalf("not deterministic: call %d = %q, first = %q", i, got, first)
		}
	}
}

func TestLabelRoleAttributesSortedByKey(t *testing.T) {
	t.Parallel()
	got := labelRoleAttributes(map[string]string{"team": "platform", "tier": "gold"})
	want := []string{"label.team.platform", "label.tier.gold"}
	if len(got) != len(want) {
		t.Fatalf("labelRoleAttributes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("labelRoleAttributes[%d] = %q, want %q (must be sorted by key for determinism)", i, got[i], want[i])
		}
	}
}

func TestLabelRoleAttributesNilForEmptyLabels(t *testing.T) {
	t.Parallel()
	if got := labelRoleAttributes(nil); got != nil {
		t.Errorf("labelRoleAttributes(nil) = %v, want nil", got)
	}
	if got := labelRoleAttributes(map[string]string{}); got != nil {
		t.Errorf("labelRoleAttributes(empty) = %v, want nil", got)
	}
}

func TestIdentityMintRoleAttributesGateOff(t *testing.T) {
	t.Parallel()
	got := identityMintRoleAttributes(map[string]string{"tier": "gold"}, false)
	want := []string{"datascape-mediated"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("identityMintRoleAttributes(gate off) = %v, want %v (byte-identical to pre-K4)", got, want)
	}
}

func TestIdentityMintRoleAttributesGateOnAppendsLabelAttributes(t *testing.T) {
	t.Parallel()
	got := identityMintRoleAttributes(map[string]string{"tier": "gold"}, true)
	want := []string{"datascape-mediated", "label.tier.gold"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("identityMintRoleAttributes(gate on) = %v, want %v", got, want)
	}
}

func TestIdentityMintRoleAttributesGateOnUnlabeledIsByteIdentical(t *testing.T) {
	t.Parallel()
	got := identityMintRoleAttributes(nil, true)
	want := []string{"datascape-mediated"}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("identityMintRoleAttributes(gate on, unlabeled) = %v, want %v (unlabeled stays byte-identical even with the gate on)", got, want)
	}
}

func TestServiceRoleAttributesGateOffIsNil(t *testing.T) {
	t.Parallel()
	if got := serviceRoleAttributes(map[string]string{"tier": "gold"}, false); got != nil {
		t.Errorf("serviceRoleAttributes(gate off) = %v, want nil", got)
	}
}

func TestServiceRoleAttributesGateOnDerivesFromLabels(t *testing.T) {
	t.Parallel()
	got := serviceRoleAttributes(map[string]string{"tier": "gold"}, true)
	if len(got) != 1 || got[0] != "label.tier.gold" {
		t.Fatalf("serviceRoleAttributes(gate on) = %v, want [label.tier.gold]", got)
	}
}

func TestDialRoleRefsGateOffReturnsExactIDRef(t *testing.T) {
	t.Parallel()
	refs, allOf := dialRoleRefs(map[string]string{"tier": "gold"}, "ident123", false)
	if allOf {
		t.Error("allOf = true, want false when the gate is off")
	}
	if len(refs) != 1 || refs[0] != "@ident123" {
		t.Fatalf("refs = %v, want [@ident123] (gate off: exact @id, byte-identical to pre-K4)", refs)
	}
}

func TestDialRoleRefsUnlabeledIsExactIDRefEvenWithGateOn(t *testing.T) {
	t.Parallel()
	refs, allOf := dialRoleRefs(nil, "ident123", true)
	if allOf {
		t.Error("allOf = true, want false for an unlabeled endpoint")
	}
	if len(refs) != 1 || refs[0] != "@ident123" {
		t.Fatalf("refs = %v, want [@ident123] (unlabeled endpoint: exact @id even with the gate on)", refs)
	}
}

func TestDialRoleRefsLabeledUsesAttributeRefsWithAllOf(t *testing.T) {
	t.Parallel()
	refs, allOf := dialRoleRefs(map[string]string{"team": "platform", "tier": "gold"}, "ident123", true)
	if !allOf {
		t.Error("allOf = false, want true for a labeled endpoint (matches K2's matchLabels conjunction)")
	}
	want := []string{"#label.team.platform", "#label.tier.gold"}
	if len(refs) != len(want) || refs[0] != want[0] || refs[1] != want[1] {
		t.Fatalf("refs = %v, want %v", refs, want)
	}
}

func TestParseInstanceConfigDefaults(t *testing.T) {
	t.Parallel()
	ic := parseInstanceConfig(configOf(nil))
	if ic.ControllerPort != 1280 {
		t.Errorf("ControllerPort = %d, want 1280", ic.ControllerPort)
	}
	if ic.RouterPort != 3022 {
		t.Errorf("RouterPort = %d, want 3022", ic.RouterPort)
	}
	if len(ic.TargetNetworks) != 0 {
		t.Errorf("TargetNetworks = %v, want empty", ic.TargetNetworks)
	}
}

func TestParseInstanceConfigOverrides(t *testing.T) {
	t.Parallel()
	ic := parseInstanceConfig(configOf(map[string]any{
		"controllerPort": float64(11280),
		"routerPort":     float64(13022),
		"targetNetworks": []any{"vpc-net", "other-net"},
	}))
	if ic.ControllerPort != 11280 {
		t.Errorf("ControllerPort = %d, want 11280", ic.ControllerPort)
	}
	if ic.RouterPort != 13022 {
		t.Errorf("RouterPort = %d, want 13022", ic.RouterPort)
	}
	if len(ic.TargetNetworks) != 2 || ic.TargetNetworks[0] != "vpc-net" || ic.TargetNetworks[1] != "other-net" {
		t.Errorf("TargetNetworks = %v, want [vpc-net other-net]", ic.TargetNetworks)
	}
}

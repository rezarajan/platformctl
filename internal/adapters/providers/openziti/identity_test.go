package openziti

import "testing"

func TestIdentityRoleAttributeSanitizesSPIFFEURI(t *testing.T) {
	got := identityRoleAttribute("spiffe://datascape/payments/source/orders-db")
	want := "spiffe-datascape-payments-source-orders-db"
	if got != want {
		t.Errorf("identityRoleAttribute = %q, want %q", got, want)
	}
}

func TestIdentityRoleAttributeIsDeterministic(t *testing.T) {
	uri := "spiffe://datascape/analytics/binding/cdc-orders"
	first := identityRoleAttribute(uri)
	for i := 0; i < 5; i++ {
		if got := identityRoleAttribute(uri); got != first {
			t.Fatalf("not deterministic: call %d = %q, first = %q", i, got, first)
		}
	}
}

func TestIdentityRoleAttributeDistinctForDistinctURIs(t *testing.T) {
	a := identityRoleAttribute("spiffe://datascape/payments/source/orders-db")
	b := identityRoleAttribute("spiffe://datascape/analytics/source/orders-db")
	if a == b {
		t.Fatalf("collision: both URIs mapped to %q", a)
	}
}

func TestParseInstanceConfigDefaults(t *testing.T) {
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

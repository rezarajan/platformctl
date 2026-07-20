package endpoint

import (
	"encoding/json"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	in := List{
		{Name: "kafka", Scheme: "kafka", Host: "127.0.0.1:19092", Internal: "rp:29092", RuntimeName: "rp", ContainerPort: 29092, Audience: "internal"},
		{Name: "admin", Scheme: "http", Internal: "rp:9644"}, // no host
	}
	// ToState yields JSON-friendly maps; simulate the []any round-trip that
	// state persistence performs.
	stateVal := any([]any{})
	tmp := in.ToState()
	roundtripped := make([]any, len(tmp))
	for i, m := range tmp {
		roundtripped[i] = any(m)
	}
	stateVal = roundtripped

	out := FromState(stateVal)
	if len(out) != 2 {
		t.Fatalf("got %d endpoints, want 2", len(out))
	}
	if out[0] != in[0] {
		t.Errorf("endpoint 0 = %+v, want %+v", out[0], in[0])
	}
	if out[1].Host != "" {
		t.Errorf("endpoint 1 host = %q, want empty", out[1].Host)
	}
	if FromState("not-a-list") != nil {
		t.Error("FromState on garbage should be nil")
	}
}

// TestRuntimeFactsSurviveRealJSONRoundTrip proves ContainerPort survives the
// actual json.Marshal/Unmarshal state persistence performs — not just the
// direct map[string]any construction TestRoundTrip uses — since a real
// decode yields float64 for a JSON number, not int (docs/planning/08 F4).
func TestRuntimeFactsSurviveRealJSONRoundTrip(t *testing.T) {
	in := List{{Name: "forward", Scheme: "tcp", Host: "127.0.0.1:15432", RuntimeName: "orders-db", ContainerPort: 5432, Audience: "host"}}
	raw, err := json.Marshal(in.ToState())
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded []any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	out := FromState(decoded)
	if len(out) != 1 {
		t.Fatalf("got %d endpoints, want 1", len(out))
	}
	if out[0] != in[0] {
		t.Errorf("endpoint = %+v, want %+v", out[0], in[0])
	}
}

package endpoint

import "testing"

func TestRoundTrip(t *testing.T) {
	in := List{
		{Name: "kafka", Scheme: "kafka", Host: "127.0.0.1:19092", Internal: "rp:29092"},
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

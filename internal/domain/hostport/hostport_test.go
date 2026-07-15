package hostport

import "testing"

func TestDeterministicAndUnique(t *testing.T) {
	a := For("lake-redpanda")
	if a != For("lake-redpanda") {
		t.Error("not deterministic")
	}
	if a < rangeStart || a >= rangeStart+rangeLen {
		t.Errorf("port %d out of range", a)
	}
	if For("lake-redpanda") == For("lake-postgres") {
		t.Error("distinct names collided (unlikely but check)")
	}
}

func TestResolvePinWins(t *testing.T) {
	if Resolve(15432, "anything") != 15432 {
		t.Error("explicit pin not honoured")
	}
	if Resolve(0, "n") != For("n") {
		t.Error("zero should auto-allocate")
	}
}

package hostport

import (
	"fmt"
	"strings"
	"testing"
)

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

// TestConflictDetection pins doc 11's collision-diagnosability fix: two
// names hashing to one port are reported with both names and the pin
// remedy; pinned ports and single claimants report nothing.
func TestConflictDetection(t *testing.T) {
	// Find two distinct names that genuinely collide.
	seen := map[int]string{}
	var a, b string
	for i := 0; ; i++ {
		n := fmt.Sprintf("collide-probe-%d", i)
		p := For(n)
		if prev, ok := seen[p]; ok {
			a, b = prev, n
			break
		}
		seen[p] = n
	}
	Resolve(0, a)
	if err := ConflictError(); err != nil {
		t.Fatalf("single claimant reported a conflict: %v", err)
	}
	Resolve(0, b)
	err := ConflictError()
	if err == nil {
		t.Fatal("collision not reported")
	}
	for _, want := range []string{a, b, "pin an explicit port"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("conflict error missing %q: %v", want, err)
		}
	}
}

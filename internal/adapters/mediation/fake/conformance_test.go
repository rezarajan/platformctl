package fake_test

import (
	"context"
	"testing"

	fakemediation "github.com/rezarajan/platformctl/internal/adapters/mediation/fake"
	fakeruntime "github.com/rezarajan/platformctl/internal/adapters/runtime/fake"
	"github.com/rezarajan/platformctl/internal/domain/resource"
	"github.com/rezarajan/platformctl/internal/ports/mediation"
	"github.com/rezarajan/platformctl/internal/ports/mediation/conformance"
)

// TestFakeMediationConformance is the fast-tier proof that the fake upholds
// the full mediation contract (docs/planning/08 L2a) — the same suite the
// OpenZiti adapter runs live in the deep tier. It exercises the fake's
// MediationProvider, FabricProvisioner, and AddressResolver capabilities
// (it implements all three).
func TestFakeMediationConformance(t *testing.T) {
	t.Parallel()
	f := fakemediation.New()
	conformance.Run(t, conformance.Subject{
		Provider: f,
		Fabric:   f,
		Resolver: f,
		Runtime:  fakeruntime.New(),
	})
}

// brokenMint wraps the fake but breaks MintIdentity idempotency (mutates on
// every call). It exists to prove the conformance suite HAS TEETH — a
// non-idempotent implementation must fail — the negative self-proof
// docs/planning/08 L2a's accept bar requires.
type brokenMint struct {
	*fakemediation.Fake
	extra int
}

func (b *brokenMint) MintIdentity(ctx context.Context, node resource.Envelope) (mediation.WorkloadIdentity, error) {
	id, err := b.Fake.MintIdentity(ctx, node)
	b.extra++ // gratuitous mutation on every call — breaks the idempotency contract
	return id, err
}

func (b *brokenMint) Mutations() int { return b.Fake.Mutations() + b.extra }

// TestConformanceRejectsNonIdempotentMint runs the suite against the broken
// wrapper under a sub-test recorder and asserts it FAILS — if this passed,
// the suite would be a no-op that certifies nothing.
func TestConformanceRejectsNonIdempotentMint(t *testing.T) {
	t.Parallel()
	broken := &brokenMint{Fake: fakemediation.New()}
	rec := &testing.T{}
	conformance.Run(rec, conformance.Subject{Provider: broken})
	if !rec.Failed() {
		t.Fatal("conformance suite passed a non-idempotent MintIdentity — the suite has no teeth")
	}
}

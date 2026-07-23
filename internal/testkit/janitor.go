// Package testkit holds shared integration-test support. It is imported
// only from _test.go files (any layer may use it — test support sits
// outside the production dependency rules the archtests enforce on
// non-test code).
package testkit

import (
	"context"
	"os/exec"
	"testing"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// Janitor is docs/adr/029's shared cleanup component: integration tests
// declare WHAT they create; the janitor owns HOW and IN WHAT ORDER it
// dies. The rules it encodes were each recovered by autopsy of a live
// stray (doc 11, 2026-07-23 residue audit):
//
//   - Workloads are removed before volumes, and volumes before
//     networks/namespaces — RemoveNetwork refuses while a namespace still
//     holds workloads (the ca9d719 safety), so any other order strands.
//   - Raw fixtures (docker run/unlabeled objects) are removed with
//     `docker rm -f -v` — NEVER the port's Remove, which refuses
//     unmanaged objects by design; and always -v, because anonymous
//     volumes outlive the container otherwise.
//   - CleanSilent (pre-test) ignores errors: absent objects are the
//     expected state. The registered t.Cleanup is LOUD: a cleanup that
//     cannot clean is a test failure — a swallowed refusal is exactly how
//     two of the audit's strays recurred invisibly.
type Janitor struct {
	// RT is the runtime the managed objects were created through.
	RT runtime.ContainerRuntime
	// Workloads are managed container/Deployment/StatefulSet names,
	// removed first via the port.
	Workloads []string
	// Volumes are managed named volumes, removed after workloads.
	Volumes []string
	// Networks are managed networks/namespaces, removed last via the
	// port (which refuses while occupied — by then nothing should be).
	Networks []string
	// RawContainers are unlabeled `docker run` fixtures — the single
	// declared place for objects gc and the port's Remove cannot touch.
	RawContainers []string
	// RawNetworks are unlabeled `docker network create` fixtures.
	RawNetworks []string
	// RawVolumes are unlabeled `docker volume create` fixtures — removed
	// between raw containers and managed volumes.
	RawVolumes []string
	// RawImages are test-built image tags (`docker build`/`docker tag`) —
	// removed last; failures always ignored (an image may be shared with
	// a concurrent suite or already pruned).
	RawImages []string
	// SweepAllManaged removes EVERY managed object the runtime lists
	// (containers, then volumes, then networks) before the declared lists
	// run — for suites whose fixtures create objects with generated names
	// the test cannot enumerate statically (e.g. compose scenarios).
	SweepAllManaged bool
}

// CleanSilent removes everything best-effort, ignoring errors — the
// pre-test invocation, where absent objects are the normal case and a
// leftover from a crashed prior run should be swept without ceremony.
func (j Janitor) CleanSilent(ctx context.Context) {
	j.sweepManaged(ctx)
	for _, w := range j.Workloads {
		_ = j.RT.Remove(ctx, w)
	}
	for _, c := range j.RawContainers {
		_ = exec.Command("docker", "rm", "-f", "-v", c).Run()
	}
	for _, v := range j.RawVolumes {
		_ = exec.Command("docker", "volume", "rm", v).Run()
	}
	for _, v := range j.Volumes {
		_ = j.RT.RemoveVolume(ctx, v)
	}
	for _, n := range j.Networks {
		_ = j.RT.RemoveNetwork(ctx, n)
	}
	for _, n := range j.RawNetworks {
		_ = exec.Command("docker", "network", "rm", n).Run()
	}
	for _, img := range j.RawImages {
		_ = exec.Command("docker", "rmi", "-f", img).Run()
	}
}

// sweepManaged realizes SweepAllManaged: list-and-remove every managed
// object, silently — enumeration is best-effort by construction; the
// declared lists that follow are the loud, deterministic part.
func (j Janitor) sweepManaged(ctx context.Context) {
	if !j.SweepAllManaged {
		return
	}
	if cs, err := j.RT.ListManaged(ctx); err == nil {
		for _, c := range cs {
			_ = j.RT.Remove(ctx, c.Name)
		}
	}
	if vs, err := j.RT.ListManagedVolumes(ctx); err == nil {
		for _, v := range vs {
			_ = j.RT.RemoveVolume(ctx, v.Name)
		}
	}
	if ns, err := j.RT.ListManagedNetworks(ctx); err == nil {
		for _, n := range ns {
			_ = j.RT.RemoveNetwork(ctx, n.Name)
		}
	}
}

// Register installs the loud post-test cleanup: same order as
// CleanSilent, but every port-level failure is a t.Errorf — removal of
// an absent object is nil on every adapter (the port contract), so the
// only errors that surface here are real refusals, i.e. real residue.
// Raw-fixture removal stays best-effort (docker rm -f of an absent name
// is an expected error, not residue).
func (j Janitor) Register(ctx context.Context, t *testing.T) {
	t.Cleanup(func() {
		j.sweepManaged(ctx)
		for _, w := range j.Workloads {
			if err := j.RT.Remove(ctx, w); err != nil {
				t.Errorf("cleanup: remove workload %s: %v", w, err)
			}
		}
		for _, c := range j.RawContainers {
			_ = exec.Command("docker", "rm", "-f", "-v", c).Run()
		}
		for _, v := range j.RawVolumes {
			_ = exec.Command("docker", "volume", "rm", v).Run()
		}
		for _, v := range j.Volumes {
			if err := j.RT.RemoveVolume(ctx, v); err != nil {
				t.Errorf("cleanup: remove volume %s: %v", v, err)
			}
		}
		for _, n := range j.Networks {
			if err := j.RT.RemoveNetwork(ctx, n); err != nil {
				t.Errorf("cleanup: remove network %s: %v", n, err)
			}
		}
		for _, n := range j.RawNetworks {
			_ = exec.Command("docker", "network", "rm", n).Run()
		}
		for _, img := range j.RawImages {
			_ = exec.Command("docker", "rmi", "-f", img).Run()
		}
	})
}

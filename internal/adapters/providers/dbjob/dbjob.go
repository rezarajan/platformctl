// Package dbjob is the shared streaming-job-container mechanism postgres
// and mysql's BackupCapableProvider implementations use: two short-lived
// containers on the runtime, joined by a POSIX FIFO on a shared ephemeral
// volume, so a dump/restore payload streams straight from the database
// tool's stdout to the object-store tool's stdin (or back) without ever
// landing as a whole file — the "streamed to the destination via a
// short-lived job container" mechanism docs/planning/08 C6 asks for. s3's
// own Backup/Restore needs none of this: it already speaks the S3 API
// in-process (internal/adapters/providers/s3/bucket.go) and copies directly.
//
// Why two containers, not one: nothing in ContainerRuntime lets a caller
// attach to a running container's stdout/stdin or learn its exit code
// (runtime.ContainerState has no ExitCode — Inspect only reports
// Running/Healthy), and ReadFile is capped at 1MB (meant for small bootstrap
// files, e.g. a mounted password), unsuitable for a database dump of any
// real size. So neither the whole dump nor its final destination call can
// round-trip through this process's own memory. Splitting the job into a
// producer (the database's own dump/restore tool, whose image already has
// the exact right client version) and a consumer (mc, which already speaks
// every S3-compatible endpoint this platform targets) piped through a FIFO
// keeps the byte stream inside the runtime's own network the whole time,
// with mc's multipart upload naturally back-pressuring pg_dump/mysqldump
// through the pipe's kernel buffer — no unbounded buffering anywhere.
//
// Both sides report success by writing their shell exit code to a sentinel
// file on the same shared volume (workDir + "/producer-exit" /
// "/consumer-exit"), read back via ReadFile once the container has stopped
// — the same 1MB-capped call, but a one-line file comfortably fits it.
package dbjob

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/backup"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	// WorkDir is the mount path both containers share; PipePath is the FIFO
	// they synchronize on.
	WorkDir  = "/work"
	PipePath = WorkDir + "/pipe"

	// DefaultTimeout bounds how long RunPipeline waits for both containers
	// to finish before giving up — generous, since a large dump over a slow
	// destination can legitimately run long; callers may override per call.
	DefaultTimeout = 30 * time.Minute
	pollInterval   = 500 * time.Millisecond

	// MCImage is the pinned mc (MinIO Client) image used as the S3-side of
	// every postgres/mysql backup/restore job — the same "streams to/from
	// any S3-compatible endpoint" tool the object-store ecosystem already
	// standardizes on, so no bespoke S3 client needs building into a shell
	// script. Digest resolved from registry.hub.docker.com for
	// minio/mc:RELEASE.2025-04-16T18-13-26Z (2025-04-22 push).
	MCImage = "minio/mc:RELEASE.2025-04-16T18-13-26Z@sha256:aead63c77f9db9107f1696fb08ecb0faeda23729cde94b0f663edf4fe09728e3"

	// MCConfigDir/MCConfigPath is where a generated mc alias config (see
	// MCConfig) is mounted — MC_CONFIG_DIR points mc at it, so an alias's
	// credentials ride a file mount, never the container's environment or
	// its command line (docs/planning/07 Gate 1 checkbox 4's convention,
	// applied to a third-party CLI instead of a provider's own image).
	MCConfigDir  = "/mcconfig"
	MCConfigPath = MCConfigDir + "/config.json"

	// MCAlias is the single alias name every generated MCConfig registers;
	// producer/consumer shell commands address it as "job/<bucket>/<key>".
	MCAlias = "job"
)

// MCConfig renders the mc CLI's config.json selecting loc as the sole
// "job" alias, for mounting via a runtime.FileMount.
func MCConfig(loc backup.Location) ([]byte, error) {
	type alias struct {
		URL       string `json:"url"`
		AccessKey string `json:"accessKey"`
		SecretKey string `json:"secretKey"`
		API       string `json:"api"`
		Path      string `json:"path"`
	}
	cfg := struct {
		Version string           `json:"version"`
		Aliases map[string]alias `json:"aliases"`
	}{
		Version: "10",
		Aliases: map[string]alias{
			MCAlias: {URL: loc.Endpoint, AccessKey: loc.AccessKey, SecretKey: loc.SecretKey, API: "s3v4", Path: "auto"},
		},
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("render mc config: %w", err)
	}
	return b, nil
}

// Side is one half (producer or consumer) of a streaming pipeline.
type Side struct {
	Image    string
	Networks []string
	Env      map[string]string
	Files    []runtime.FileMount
	// ShellCmd is a POSIX sh command with no pipe redirection of its own:
	// the producer's stdout is redirected to the shared FIFO, the
	// consumer's stdin is redirected from it. Its shell exit code is what
	// RunPipeline treats as this side's success/failure.
	ShellCmd string
}

// PipelineSpec describes one streaming job. Producer writes to the FIFO,
// Consumer reads from it: a backup wires the database's own dump tool as
// Producer and mc as Consumer; a restore wires mc as Producer and the
// database's own restore tool as Consumer.
type PipelineSpec struct {
	// JobName must be unique per invocation (callers append a timestamp or
	// similar) — it names the ephemeral volume and both containers.
	JobName  string
	Labels   map[string]string
	Producer Side
	Consumer Side
	// Timeout bounds the whole run; 0 = DefaultTimeout.
	Timeout time.Duration
}

// RunPipeline creates an ephemeral shared volume, starts Producer and
// Consumer, waits for both to finish, verifies both exited zero, and always
// removes the containers and volume before returning — on success or
// failure alike, so a failed backup never leaks runtime objects for `gc` to
// find later.
func RunPipeline(ctx context.Context, rt runtime.ContainerRuntime, spec PipelineSpec) error {
	timeout := spec.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	volName := spec.JobName + "-work"
	producerName := spec.JobName + "-producer"
	consumerName := spec.JobName + "-consumer"

	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: volName, Labels: spec.Labels}); err != nil {
		return fmt.Errorf("job %q: ensure work volume: %w", spec.JobName, err)
	}
	defer func() { _ = rt.RemoveVolume(context.WithoutCancel(ctx), volName) }()
	defer func() { _ = rt.Remove(context.WithoutCancel(ctx), producerName) }()
	defer func() { _ = rt.Remove(context.WithoutCancel(ctx), consumerName) }()

	if _, err := rt.EnsureContainer(ctx, sideSpec(producerName, volName, spec.Labels, spec.Producer, redirectTo)); err != nil {
		return fmt.Errorf("job %q: start producer: %w", spec.JobName, err)
	}
	if _, err := rt.EnsureContainer(ctx, sideSpec(consumerName, volName, spec.Labels, spec.Consumer, redirectFrom)); err != nil {
		return fmt.Errorf("job %q: start consumer: %w", spec.JobName, err)
	}

	return waitPipeline(ctx, rt, spec.JobName, producerName, consumerName, timeout)
}

const (
	redirectTo   = true  // producer: shellCmd's stdout -> pipe
	redirectFrom = false // consumer: shellCmd's stdin <- pipe
)

// sideOutcome is what waitPipeline learns about one side once its container
// has stopped running: its recorded shell exit code, plus any error reading
// it back.
type sideOutcome struct {
	done bool
	code string
	err  error
}

// waitPipeline polls both sides and returns the moment either one is known
// to have failed — rather than always waiting for both, up to timeout, the
// way a pair of independent goroutines blocked on WaitGroup.Wait would
// (docs/planning/08 C6 review finding 4): an instantly-exiting side (e.g.
// the K1 entrypoint bug) otherwise leaves its peer blocked on the FIFO,
// which nothing but the read/write end closing (or the container being
// removed) can unstick, for the rest of DefaultTimeout. The moment a failure
// is known, this best-effort-removes the still-running peer so its blocked
// read/write unblocks (as an error) instead of idling out the clock, and
// returns promptly with a log tail from both sides.
func waitPipeline(ctx context.Context, rt runtime.ContainerRuntime, jobName, producerName, consumerName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var producer, consumer sideOutcome
	for {
		if !producer.done {
			producer = pollSide(ctx, rt, producerName, "producer-exit")
		}
		if !consumer.done {
			consumer = pollSide(ctx, rt, consumerName, "consumer-exit")
		}
		if producer.err != nil {
			return fmt.Errorf("job %q: producer: %w", jobName, producer.err)
		}
		if consumer.err != nil {
			return fmt.Errorf("job %q: consumer: %w", jobName, consumer.err)
		}
		if producer.done && producer.code != "0" {
			_ = rt.Remove(context.WithoutCancel(ctx), consumerName) // unstick the peer's blocked FIFO end
			return fmt.Errorf("job %q: producer failed (exit=%q)%s", jobName, producer.code, diagnostics(ctx, rt, producerName, consumerName))
		}
		if consumer.done && consumer.code != "0" {
			_ = rt.Remove(context.WithoutCancel(ctx), producerName)
			return fmt.Errorf("job %q: consumer failed (exit=%q)%s", jobName, consumer.code, diagnostics(ctx, rt, producerName, consumerName))
		}
		if producer.done && consumer.done {
			return nil // both exited zero
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("job %q: did not finish within %s%s", jobName, timeout, diagnostics(ctx, rt, producerName, consumerName))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// pollSide inspects one side once: not-yet-done (still running), done with
// an exit code (possibly non-zero — the caller decides what that means), or
// an error (the container vanished, or inspect itself failed).
func pollSide(ctx context.Context, rt runtime.ContainerRuntime, name, exitFile string) sideOutcome {
	st, found, err := rt.Inspect(ctx, name)
	switch {
	case err != nil:
		return sideOutcome{err: fmt.Errorf("inspect %q: %w", name, err)}
	case !found:
		return sideOutcome{err: fmt.Errorf("container %q disappeared before completing", name)}
	case st.Running:
		return sideOutcome{}
	}
	code, err := readExitCode(ctx, rt, name, exitFile)
	if err != nil {
		return sideOutcome{err: fmt.Errorf("read exit code for %q: %w", name, err)}
	}
	return sideOutcome{done: true, code: code}
}

func sideSpec(name, volName string, labels map[string]string, side Side, toPipe bool) runtime.ContainerSpec {
	redirect := "> " + PipePath
	if !toPipe {
		redirect = "< " + PipePath
	}
	exitFile := WorkDir + "/consumer-exit"
	if toPipe {
		exitFile = WorkDir + "/producer-exit"
	}
	script := fmt.Sprintf("mkfifo %s 2>/dev/null; (%s) %s; echo $? > %s", PipePath, side.ShellCmd, redirect, exitFile)
	return runtime.ContainerSpec{
		Name:  name,
		Image: side.Image,
		// Entrypoint replaces the image's own ENTRYPOINT so the script runs
		// under a shell regardless of it — Cmd alone (appended after
		// whatever the image declares) is not enough: minio/mc's image
		// ENTRYPOINT is ["mc"], so a bare Cmd here once ran as
		// "mc sh -c ...", which mc rejects as an unknown subcommand and
		// exits immediately (docs/planning/08 C6 review finding 1;
		// docs/adr/007-backup-restore.md). Postgres/mysql's official images
		// happen to tolerate this via their entrypoint scripts' "exec an
		// unrecognized command as-is" fallback, but relying on that per
		// image is exactly the kind of coincidence this makes unnecessary.
		Entrypoint: []string{"sh", "-c"},
		Cmd:        []string{script},
		Env:        side.Env,
		Files:      side.Files,
		Networks:   side.Networks,
		Volumes:    []runtime.VolumeMount{{VolumeName: volName, MountPath: WorkDir}},
		Labels:     labels,
	}
}

func readExitCode(ctx context.Context, rt runtime.ContainerRuntime, name, file string) (string, error) {
	data, err := rt.ReadFile(ctx, name, WorkDir+"/"+file)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

// diagnostics pulls a bounded log tail from both containers for the failure
// error message — the only place a job's stderr (pg_dump/mc's own error
// text) is surfaced, since neither container's exit path returns one.
func diagnostics(ctx context.Context, rt runtime.ContainerRuntime, producerName, consumerName string) string {
	pLogs, _ := rt.Logs(ctx, producerName, 40)
	cLogs, _ := rt.Logs(ctx, consumerName, 40)
	return fmt.Sprintf("\nproducer logs:\n%s\nconsumer logs:\n%s", pLogs, cLogs)
}

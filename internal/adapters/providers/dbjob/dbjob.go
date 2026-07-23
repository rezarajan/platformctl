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
// docs/adr/007-backup-restore.md's I12 addendum records why this shape was
// hardened rather than replaced with a single supervised container.
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
	"strconv"
	"strings"
	"time"

	"github.com/rezarajan/platformctl/internal/domain/backup"
	"github.com/rezarajan/platformctl/internal/domain/provider"
	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	// WorkDir is the mount path both containers share; PipePath is the FIFO
	// they synchronize on.
	WorkDir  = "/work"
	PipePath = WorkDir + "/pipe"

	// hashFifo/sizeFifo are two extra FIFOs on the same shared volume the
	// producer side's script tees its stdout through (alongside the main
	// PipePath) so a checksum and byte count of the exact bytes crossing
	// the pipe can be computed without landing the payload as a file
	// anywhere and without shell process substitution (docs/adr/
	// 007-backup-restore.md's I12 addendum) — plain POSIX `tee` fan-out,
	// portable to any `sh` that has it.
	hashFifo = PipePath + ".hash"
	sizeFifo = PipePath + ".size"

	// DefaultTimeout bounds how long RunPipeline waits for both containers
	// to finish before giving up — generous, since a large dump over a slow
	// destination can legitimately run long; callers may override per call,
	// per side (PipelineSpec.ProducerTimeout/ConsumerTimeout).
	DefaultTimeout = 30 * time.Minute
	pollInterval   = 500 * time.Millisecond

	// cleanupTimeout/manifestTimeout bound the small, self-contained
	// RunOneShot housekeeping steps around a pipeline run (partial-object
	// cleanup, manifest sidecar read/write) — these move at most a few KB
	// and never need DefaultTimeout's budget.
	cleanupTimeout  = 5 * time.Minute
	manifestTimeout = 5 * time.Minute

	// manifestSuffix names the JSON sidecar object PersistManifest writes
	// next to a backup's dump object, and ReadManifest reads back — the
	// durable, out-of-band integrity record docs/adr/007-backup-restore.md's
	// I12 addendum documents in full.
	manifestSuffix    = ".manifest.json"
	manifestMountPath = "/mcmanifest/manifest.json"
	manifestReadPath  = "/tmp/manifest.json"

	// oneShotExitFile is RunOneShot's exit-code sentinel — a one-shot job
	// has no shared volume of its own (it isn't a two-sided pipe), so this
	// rides the container's own writable filesystem instead of WorkDir.
	oneShotExitFile = "/tmp/dbjob-oneshot-exit"

	// MCImage is the pinned mc (MinIO Client) image used as the S3-side of
	// every postgres/mysql backup/restore job — the same "streams to/from
	// any S3-compatible endpoint" tool the object-store ecosystem already
	// standardizes on, so no bespoke S3 client needs building into a shell
	// script. Digest resolved from registry.hub.docker.com for
	// minio/mc:RELEASE.2025-04-16T18-13-26Z (2025-04-22 push). Confirmed to
	// ship GNU coreutils (sha256sum/wc/tee/mkfifo), which the producer-side
	// checksum script and RunOneShot's helpers both rely on.
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

// networksFor returns loc's extra network-join list for a job container —
// nil when loc is externally routable (a raw URL Location) and needs no
// extra join.
func networksFor(loc backup.Location) []string {
	if loc.Network == "" {
		return nil
	}
	return []string{loc.Network}
}

// Side is one half (producer or consumer) of a streaming pipeline.
type Side struct {
	Image    string
	Networks []string
	Env      map[string]string
	Files    []runtime.FileMount
	// Volumes, when set, mounts additional existing named volumes into
	// this side's container beyond the pipeline's own shared work volume
	// (RunPipeline) — or, for RunOneShot, the container's only volume
	// mounts, since a one-shot container has no shared work volume of its
	// own. Used by the I13 disk-headroom precheck to bind-mount an
	// instance's own data volume read-only-in-intent from a throwaway
	// container that just runs `df` against it (docs/adr/
	// 007-backup-restore.md addendum 2) — nil for every pre-I13 caller,
	// byte-for-byte unchanged.
	Volumes []runtime.VolumeMount
	// Namespace is the Kubernetes namespace RunOneShot schedules this
	// side's Job in when the runtime implements
	// runtime.JobCapableRuntime — ignored on Docker/fake, which derive
	// their own placement from Networks. Unused (may be left empty) on
	// every RunPipeline side; RunPipeline.Namespace covers the whole
	// pipeline's Job instead (docs/adr/007-backup-restore.md addendum 3).
	Namespace string
	// NodeName, when set, pins this side's one-shot Job to an exact
	// Kubernetes node (runtime.JobSpec.NodeName) — I13's disk-headroom
	// precheck uses this to co-locate with the running instance pod so
	// both can mount the same ReadWriteOnce PersistentVolumeClaim.
	// Ignored on Docker/fake and by RunPipeline sides.
	NodeName string
	// ShellCmd is a POSIX sh command with no pipe redirection of its own:
	// the producer's stdout is redirected to the shared FIFO, the
	// consumer's stdin is redirected from it. Its shell exit code is what
	// RunPipeline treats as this side's success/failure.
	ShellCmd string
}

// PipelineSpec describes one streaming job. Producer writes to the FIFO,
// Consumer reads from it: a backup wires the database's own dump tool as
// Producer and mc as Consumer; a restore wires mc as Producer and the
// database's own restore tool as Consumer — so the checksum RunPipeline
// always computes producer-side lands on whichever role is pushing bytes
// into the pipe (the freshly dumped content for Backup, the just-downloaded
// object for Restore), matching what docs/adr/007-backup-restore.md's I12
// addendum calls "computed producer-side" for both directions.
type PipelineSpec struct {
	// JobName must be unique per invocation (callers append a timestamp or
	// similar) — it names the ephemeral volume and both containers.
	JobName string
	// Namespace is the Kubernetes namespace RunPipeline schedules the
	// whole pipeline's Job in when the runtime implements
	// runtime.JobCapableRuntime — "the provider's domain namespace"
	// (docs/planning/08 I15): the database side's own namespace,
	// regardless of direction, since a Job's pod lives in exactly one
	// namespace and Producer/Consumer always run as siblings in it.
	// Ignored on Docker/fake, which derive per-side placement from each
	// Side's own Networks instead.
	Namespace string
	// RuntimeType is the realizing Provider's provider.Provider.RuntimeType,
	// passed through by callers. Path dispatch (Kubernetes Job vs Docker
	// container pipeline) reads THIS domain-layer fact, never a type
	// assertion on rt — every runtime wrapper statically implements
	// runtime.JobCapableRuntime (the wrapper-completeness archtest requires
	// it) and only errors at call time, so an assertion is always true
	// through a wrapped runtime and says nothing about the adapter
	// underneath (the same rule ingress's isKubernetes documents).
	RuntimeType string
	Labels      map[string]string
	Producer    Side
	Consumer    Side
	// Timeout bounds each side by default; 0 = DefaultTimeout.
	Timeout time.Duration
	// ProducerTimeout/ConsumerTimeout override Timeout for one side only
	// when nonzero — e.g. distinguishing "the whole transfer may
	// legitimately run long" from "this side must at least be moving
	// within its own budget," and naming which side blew its deadline
	// instead of one generic "job did not finish" (docs/adr/
	// 007-backup-restore.md's I12 addendum).
	ProducerTimeout time.Duration
	ConsumerTimeout time.Duration
	// Cleanup, if set, runs as a best-effort one-shot container
	// (RunOneShot) after any pipeline failure — never on success — once
	// both the producer and consumer containers have been forcibly
	// removed. A backup wires this to "mc rm --force <key>" so a producer
	// killed mid-stream (whose consumer may have already completed an
	// upload of the truncated bytes it received before the FIFO's write
	// end closed) never leaves a partial object behind. Cleanup's own
	// failure is folded into the returned error as additional context,
	// never promoted to hide the pipeline's own root-cause error.
	Cleanup *Side
}

// Result is what a successful RunPipeline call learns about the bytes that
// crossed the pipe: their sha256 digest (bare hex, no "sha256:" prefix —
// callers building a backup.Manifest.Checksum add that) and count, computed
// producer-side without ever landing the payload as a whole file in this
// process or on disk.
type Result struct {
	SHA256 string
	Bytes  int64
}

// RunPipeline creates an ephemeral shared volume, starts Producer and
// Consumer, waits for both to finish, verifies both exited zero, and always
// removes the containers and volume before returning — on success or
// failure alike, so a failed backup never leaks runtime objects for `gc` to
// find later. On failure, both containers are forcibly removed before
// spec.Cleanup (if set) runs, and the returned error names which side
// failed and why.
//
// When spec.RuntimeType is "kubernetes" (docs/adr/007-backup-restore.md
// addendum 3, docs/planning/08 I15), the whole pipeline instead runs as one
// Kubernetes Job whose pod runs Producer and Consumer as sibling containers
// sharing an emptyDir — the FIFO protocol below (sideScript) is identical
// either way; only how the two sides are scheduled and inspected differs.
// Docker/fake callers pass their own RuntimeType and are unaffected.
func RunPipeline(ctx context.Context, rt runtime.ContainerRuntime, spec PipelineSpec) (Result, error) {
	if spec.RuntimeType == provider.RuntimeTypeKubernetes {
		jrt, err := jobRuntime(rt)
		if err != nil {
			return Result{}, err
		}
		return runJobPipeline(ctx, jrt, spec)
	}
	producerTimeout := spec.ProducerTimeout
	if producerTimeout <= 0 {
		producerTimeout = spec.Timeout
	}
	if producerTimeout <= 0 {
		producerTimeout = DefaultTimeout
	}
	consumerTimeout := spec.ConsumerTimeout
	if consumerTimeout <= 0 {
		consumerTimeout = spec.Timeout
	}
	if consumerTimeout <= 0 {
		consumerTimeout = DefaultTimeout
	}
	volName := spec.JobName + "-work"
	producerName := spec.JobName + "-producer"
	consumerName := spec.JobName + "-consumer"

	if err := rt.EnsureVolume(ctx, runtime.VolumeSpec{Name: volName, Labels: spec.Labels}); err != nil {
		return Result{}, fmt.Errorf("job %q: ensure work volume: %w", spec.JobName, err)
	}
	defer func() { _ = rt.RemoveVolume(context.WithoutCancel(ctx), volName) }()
	defer func() { _ = rt.Remove(context.WithoutCancel(ctx), producerName) }()
	defer func() { _ = rt.Remove(context.WithoutCancel(ctx), consumerName) }()

	if _, err := rt.EnsureContainer(ctx, sideSpec(producerName, volName, spec.Labels, spec.Producer, redirectTo)); err != nil {
		return Result{}, fmt.Errorf("job %q: start producer: %w", spec.JobName, err)
	}
	if _, err := rt.EnsureContainer(ctx, sideSpec(consumerName, volName, spec.Labels, spec.Consumer, redirectFrom)); err != nil {
		return Result{}, fmt.Errorf("job %q: start consumer: %w", spec.JobName, err)
	}

	result, err := waitPipeline(ctx, rt, spec.JobName, producerName, consumerName, producerTimeout, consumerTimeout)
	if err == nil {
		return result, nil
	}

	// Force-remove both sides now — before any Cleanup step — closing the
	// race where a not-yet-killed consumer could still complete an
	// in-flight multipart upload after Cleanup already ran and found
	// nothing to delete (docs/adr/007-backup-restore.md's I12 addendum).
	_ = rt.Remove(context.WithoutCancel(ctx), producerName)
	_ = rt.Remove(context.WithoutCancel(ctx), consumerName)
	if spec.Cleanup != nil {
		if _, cerr := RunOneShot(ctx, rt, spec.RuntimeType, spec.JobName+"-cleanup", spec.Labels, *spec.Cleanup, cleanupTimeout, ""); cerr != nil {
			err = fmt.Errorf("%w (partial-object cleanup after failure also failed: %s)", err, cerr)
		}
	}
	return Result{}, err
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
// to have failed — rather than always waiting for both, up to a shared
// timeout, the way a pair of independent goroutines blocked on
// WaitGroup.Wait would (docs/planning/08 C6 review finding 4): an
// instantly-exiting side (e.g. the K1 entrypoint bug) otherwise leaves its
// peer blocked on the FIFO, which nothing but the read/write end closing
// (or the container being removed) can unstick, for the rest of the
// timeout. producerDeadline/consumerDeadline are tracked independently
// (docs/adr/007-backup-restore.md's I12 addendum) so a side that never
// even gets moving is named specifically, rather than reported as one
// generic "job did not finish" once the slower side's own legitimate
// budget also happens to run out. The moment a failure is known,
// RunPipeline force-removes both sides so a blocked peer unblocks (as an
// error) instead of idling out its own clock.
func waitPipeline(ctx context.Context, rt runtime.ContainerRuntime, jobName, producerName, consumerName string, producerTimeout, consumerTimeout time.Duration) (Result, error) {
	producerDeadline := time.Now().Add(producerTimeout)
	consumerDeadline := time.Now().Add(consumerTimeout)
	var producer, consumer sideOutcome
	for {
		if !producer.done {
			producer = pollSide(ctx, rt, producerName, "producer-exit")
		}
		if !consumer.done {
			consumer = pollSide(ctx, rt, consumerName, "consumer-exit")
		}
		if producer.err != nil {
			return Result{}, fmt.Errorf("job %q: producer: %w", jobName, producer.err)
		}
		if consumer.err != nil {
			return Result{}, fmt.Errorf("job %q: consumer: %w", jobName, consumer.err)
		}
		if producer.done && producer.code != "0" {
			return Result{}, fmt.Errorf("job %q: producer failed (exit=%q)%s", jobName, producer.code, diagnostics(ctx, rt, producerName, consumerName))
		}
		if consumer.done && consumer.code != "0" {
			return Result{}, fmt.Errorf("job %q: consumer failed (exit=%q)%s", jobName, consumer.code, diagnostics(ctx, rt, producerName, consumerName))
		}
		if producer.done && consumer.done {
			result, err := readResult(ctx, rt, jobName, producerName)
			if err != nil {
				return Result{}, err
			}
			return result, nil
		}
		now := time.Now()
		if !producer.done && now.After(producerDeadline) {
			return Result{}, fmt.Errorf("job %q: producer did not finish within %s%s", jobName, producerTimeout, diagnostics(ctx, rt, producerName, consumerName))
		}
		if !consumer.done && now.After(consumerDeadline) {
			return Result{}, fmt.Errorf("job %q: consumer did not finish within %s%s", jobName, consumerTimeout, diagnostics(ctx, rt, producerName, consumerName))
		}
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// readResult reads back the producer-side checksum/byte-count sentinel
// files sideSpec's hashing script writes, once both sides have exited zero.
func readResult(ctx context.Context, rt runtime.ContainerRuntime, jobName, producerName string) (Result, error) {
	sumData, err := rt.ReadFile(ctx, producerName, WorkDir+"/producer-checksum")
	if err != nil {
		return Result{}, fmt.Errorf("job %q: read producer checksum: %w", jobName, err)
	}
	bytesData, err := rt.ReadFile(ctx, producerName, WorkDir+"/producer-bytes")
	if err != nil {
		return Result{}, fmt.Errorf("job %q: read producer byte count: %w", jobName, err)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(bytesData)), 10, 64)
	if err != nil {
		return Result{}, fmt.Errorf("job %q: parse producer byte count %q: %w", jobName, bytesData, err)
	}
	sum := strings.TrimSpace(string(sumData))
	if sum == "" {
		return Result{}, fmt.Errorf("job %q: producer checksum file is empty", jobName)
	}
	return Result{SHA256: sum, Bytes: n}, nil
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

// sideScript renders one side's shell script — the FIFO/tee/checksum
// protocol shared byte-for-byte by every realization (Docker's per-side
// container, docs/adr/007-backup-restore.md addendum 3's Kubernetes Job
// container): the producer side (toPipe) tees its stdout through two extra
// FIFOs (hashFifo/sizeFifo) so a checksum and byte count of the exact bytes
// crossing PipePath are computed by backgrounded GNU coreutils processes —
// no process substitution, no landing the payload as a file (docs/adr/
// 007-backup-restore.md's I12 addendum). cmd's own exit status is captured
// inside a subshell BEFORE tee (the pipeline's last stage) can obscure it:
// `( cmd; echo $? > cmdrc ) | tee ...` — the subshell always finishes
// (including writing cmdrc) before the whole pipeline construct returns,
// regardless of tee's own exit status.
func sideScript(toPipe bool, side Side) string {
	if toPipe {
		cmdrc := WorkDir + "/producer-cmdrc"
		return fmt.Sprintf(
			"mkfifo %s 2>/dev/null; mkfifo %s %s 2>/dev/null; "+
				"sha256sum < %s | cut -d' ' -f1 > %s/producer-checksum & "+
				"wc -c < %s | tr -d ' ' > %s/producer-bytes & "+
				"( %s; echo $? > %s ) | tee %s %s > %s; "+
				"wait; cp %s %s/producer-exit",
			PipePath, hashFifo, sizeFifo,
			hashFifo, WorkDir,
			sizeFifo, WorkDir,
			side.ShellCmd, cmdrc, hashFifo, sizeFifo, PipePath,
			cmdrc, WorkDir,
		)
	}
	return fmt.Sprintf("mkfifo %s 2>/dev/null; (%s) < %s; echo $? > %s/consumer-exit", PipePath, side.ShellCmd, PipePath, WorkDir)
}

// sideSpec builds one side's Docker ContainerSpec around sideScript.
func sideSpec(name, volName string, labels map[string]string, side Side, toPipe bool) runtime.ContainerSpec {
	script := sideScript(toPipe, side)
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
		Volumes:    append(append([]runtime.VolumeMount{}, side.Volumes...), runtime.VolumeMount{VolumeName: volName, MountPath: WorkDir}),
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

// RunOneShot runs a single short-lived container to completion — no FIFO,
// no shared volume, just Side's own Files/Env/Networks — and returns once
// it has stopped, translating a nonzero exit code, a container that
// vanished, or a timeout into a named error. Used for the small,
// self-contained housekeeping steps around a RunPipeline call:
// PersistManifest/ReadManifest (the integrity-manifest sidecar), and
// RunPipeline's own Cleanup wiring (best-effort partial-object removal
// after a failure). If resultPath is non-empty, its content is read back
// (before the container is removed) and returned on success.
//
// When runtimeType is "kubernetes", this runs as a single-container
// Kubernetes Job instead (side.Namespace selects where; side.NodeName
// optionally pins it — docs/adr/007-backup-restore.md addendum 3).
// Dispatch reads runtimeType, never a type assertion on rt (see
// PipelineSpec.RuntimeType). Docker/fake are unaffected.
func RunOneShot(ctx context.Context, rt runtime.ContainerRuntime, runtimeType, name string, labels map[string]string, side Side, timeout time.Duration, resultPath string) ([]byte, error) {
	if runtimeType == provider.RuntimeTypeKubernetes {
		jrt, err := jobRuntime(rt)
		if err != nil {
			return nil, err
		}
		return runOneShotJob(ctx, jrt, name, labels, side, timeout, resultPath)
	}
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	script := fmt.Sprintf("(%s); echo $? > %s", side.ShellCmd, oneShotExitFile)
	spec := runtime.ContainerSpec{
		Name:       name,
		Image:      side.Image,
		Entrypoint: []string{"sh", "-c"},
		Cmd:        []string{script},
		Env:        side.Env,
		Files:      side.Files,
		Networks:   side.Networks,
		Volumes:    side.Volumes,
		Labels:     labels,
	}
	defer func() { _ = rt.Remove(context.WithoutCancel(ctx), name) }()
	if _, err := rt.EnsureContainer(ctx, spec); err != nil {
		return nil, fmt.Errorf("job %q: start: %w", name, err)
	}
	deadline := time.Now().Add(timeout)
	for {
		st, found, err := rt.Inspect(ctx, name)
		switch {
		case err != nil:
			return nil, fmt.Errorf("job %q: inspect: %w", name, err)
		case !found:
			return nil, fmt.Errorf("job %q: container disappeared before completing", name)
		case !st.Running:
			data, err := rt.ReadFile(ctx, name, oneShotExitFile)
			if err != nil {
				return nil, fmt.Errorf("job %q: read exit code: %w", name, err)
			}
			code := strings.TrimSpace(string(data))
			if code != "0" {
				return nil, fmt.Errorf("job %q: failed (exit=%q)%s", name, code, diagnostics(ctx, rt, name, name))
			}
			if resultPath == "" {
				return nil, nil
			}
			out, err := rt.ReadFile(ctx, name, resultPath)
			if err != nil {
				return nil, fmt.Errorf("job %q: read result %q: %w", name, resultPath, err)
			}
			return out, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("job %q: did not finish within %s", name, timeout)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// diskHeadroomTimeout/diskHeadroomResultPath bound and locate
// CheckDiskHeadroom's precheck job.
const (
	diskHeadroomTimeout    = 2 * time.Minute
	diskHeadroomResultPath = "/tmp/datascape-restore-freekb"
)

// CheckDiskHeadroom refuses (a named, honest error) unless volumeName (an
// existing named volume, already mounted by the running instance at
// mountPath) has at least 2x wantBytes free — I13's precheck
// (docs/adr/007-backup-restore.md addendum 2), run via a throwaway
// RunOneShot container that mounts the same volume and runs POSIX `df`
// (confirmed present in every pinned database image), before a restore's
// scratch database is even created — postgres and mysql both call this
// unchanged, the same "dbjob implements it once" shape every other backup/
// restore mechanic in this package already has.
// instanceName is the running database instance's own runtime object name
// (naming.RuntimeObjectName(req.Provider)) — used only to resolve a
// co-location node via runtime.JobCapableRuntime.NodeNameOf when rt
// implements it: a Kubernetes ReadWriteOnce PersistentVolumeClaim's access
// mode guarantees a single NODE may mount it, not a single pod, so this
// precheck's own throwaway pod must land on the SAME node as the already-
// running instance pod to mount volumeName alongside it (docs/adr/
// 007-backup-restore.md addendum 3). namespace is the Kubernetes namespace
// to schedule into; ignored, like instanceName, on Docker/fake.
func CheckDiskHeadroom(ctx context.Context, rt runtime.ContainerRuntime, runtimeType string, labels map[string]string, jobName, namespace, instanceName, image, volumeName, mountPath string, wantBytes int64) error {
	side := Side{
		Image:     image,
		Namespace: namespace,
		Volumes:   []runtime.VolumeMount{{VolumeName: volumeName, MountPath: mountPath}},
		ShellCmd:  fmt.Sprintf("df -Pk %s | tail -1 | awk '{print $4}' > %s", mountPath, diskHeadroomResultPath),
	}
	if runtimeType == provider.RuntimeTypeKubernetes {
		jrt, err := jobRuntime(rt)
		if err != nil {
			return fmt.Errorf("disk headroom precheck: %w", err)
		}
		node, err := jrt.NodeNameOf(ctx, instanceName)
		if err != nil {
			return fmt.Errorf("disk headroom precheck: resolve instance node: %w", err)
		}
		side.NodeName = node
	}
	out, err := RunOneShot(ctx, rt, runtimeType, jobName+"-headroom", labels, side, diskHeadroomTimeout, diskHeadroomResultPath)
	if err != nil {
		return fmt.Errorf("disk headroom precheck: %w", err)
	}
	freeKB, perr := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if perr != nil {
		return fmt.Errorf("disk headroom precheck: parse free space %q: %w", out, perr)
	}
	freeBytes := freeKB * 1024
	needBytes := 2 * wantBytes
	if freeBytes < needBytes {
		return fmt.Errorf("disk headroom precheck: %d bytes free on the instance volume, need at least %d (2x the %d-byte backup) — refusing restore", freeBytes, needBytes, wantBytes)
	}
	return nil
}

// PersistManifest uploads manifest as the JSON sidecar object
// "<key><manifestSuffix>" next to the backup object it describes — the
// durable, out-of-band integrity record a later, separate `restore`
// invocation reads back via ReadManifest before trusting anything it
// downloads (docs/adr/007-backup-restore.md's I12 addendum). loc must
// already carry resolved credentials, exactly like every other Location
// use; key is the exact object key the backup landed at. namespace is the
// Kubernetes namespace this one-shot job schedules into when the runtime
// implements runtime.JobCapableRuntime (docs/adr/007-backup-restore.md
// addendum 3) — ignored on Docker/fake; callers pass the same "provider's
// domain namespace" value they pass to PipelineSpec.Namespace.
func PersistManifest(ctx context.Context, rt runtime.ContainerRuntime, runtimeType, jobName, namespace string, labels map[string]string, loc backup.Location, key string, manifest backup.Manifest) error {
	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("job %q: encode manifest sidecar: %w", jobName, err)
	}
	mcConfig, err := MCConfig(loc)
	if err != nil {
		return err
	}
	side := Side{
		Image:     MCImage,
		Networks:  networksFor(loc),
		Namespace: namespace,
		Env:       map[string]string{"MC_CONFIG_DIR": MCConfigDir},
		Files: []runtime.FileMount{
			{Path: MCConfigPath, Content: mcConfig, Mode: 0o600},
			{Path: manifestMountPath, Content: data, Mode: 0o600},
		},
		ShellCmd: fmt.Sprintf("mc pipe %s/%s/%s < %s", MCAlias, loc.Bucket, key+manifestSuffix, manifestMountPath),
	}
	if _, err := RunOneShot(ctx, rt, runtimeType, jobName+"-manifest-write", labels, side, manifestTimeout, ""); err != nil {
		return fmt.Errorf("persist backup integrity manifest for %q: %w", key, err)
	}
	return nil
}

// ReadManifest downloads and parses key's manifest sidecar
// ("<key><manifestSuffix>"), for Restore to verify a freshly downloaded
// object against before trusting it. A missing or unreadable sidecar is a
// named error, not a silently skipped check — a backup made before this
// hardening landed cannot be integrity-verified, and Restore refuses it
// outright (docs/adr/007-backup-restore.md's I12 addendum, Known
// limitation (d)). namespace is PersistManifest's own parameter, same
// meaning.
func ReadManifest(ctx context.Context, rt runtime.ContainerRuntime, runtimeType, jobName, namespace string, labels map[string]string, loc backup.Location, key string) (backup.Manifest, error) {
	mcConfig, err := MCConfig(loc)
	if err != nil {
		return backup.Manifest{}, err
	}
	side := Side{
		Image:     MCImage,
		Networks:  networksFor(loc),
		Namespace: namespace,
		Env:       map[string]string{"MC_CONFIG_DIR": MCConfigDir},
		Files:     []runtime.FileMount{{Path: MCConfigPath, Content: mcConfig, Mode: 0o600}},
		ShellCmd:  fmt.Sprintf("mc cat %s/%s/%s > %s", MCAlias, loc.Bucket, key+manifestSuffix, manifestReadPath),
	}
	out, err := RunOneShot(ctx, rt, runtimeType, jobName+"-manifest-read", labels, side, manifestTimeout, manifestReadPath)
	if err != nil {
		return backup.Manifest{}, fmt.Errorf("read backup integrity manifest for %q: %w (this object may predate I12's integrity hardening, or the backup did not complete successfully)", key, err)
	}
	var manifest backup.Manifest
	if err := json.Unmarshal(out, &manifest); err != nil {
		return backup.Manifest{}, fmt.Errorf("parse backup integrity manifest for %q: %w", key, err)
	}
	return manifest, nil
}

// VerifyIntegrity compares a Restore pipeline's actual Result (the
// downloaded bytes' checksum/count, computed producer-side by the same
// RunPipeline call that streamed them into the database) against the
// backup's recorded Manifest, and returns a clean, named error on any
// mismatch — postgres/mysql's Restore call this immediately after
// RunPipeline succeeds.
//
// As docs/adr/007-backup-restore.md's I12 addendum records (Known
// limitation (d)), this check is necessarily post-hoc: the restore tool
// consumes the same FIFO bytes concurrently with the checksum being
// computed, so a mismatch is only known after the corrupted/truncated data
// has already been applied. The error names this: a caller must treat the
// target's data as untrustworthy and re-restore from a known-good backup,
// not assume the refusal rolled anything back.
func VerifyIntegrity(resourceName, bucket, key string, want backup.Manifest, got Result) error {
	wantSum := strings.TrimPrefix(want.Checksum, "sha256:")
	if wantSum == "" {
		return fmt.Errorf("%q: backup manifest for %s/%s has no recorded checksum — cannot verify integrity", resourceName, bucket, key)
	}
	if got.SHA256 != wantSum || got.Bytes != want.Bytes {
		return fmt.Errorf("%q: restore integrity check failed: downloaded sha256:%s (%d bytes) does not match backup manifest sha256:%s (%d bytes) for %s/%s — the object may be corrupted or truncated; treat the target's data as untrustworthy and re-restore from a known-good backup, not as rolled back", resourceName, got.SHA256, got.Bytes, wantSum, want.Bytes, bucket, key)
	}
	return nil
}

// --- Kubernetes Job realization (docs/adr/007-backup-restore.md addendum
// 3, docs/planning/08 I15) ---
//
// The functions below are RunPipeline/RunOneShot's Job-path counterparts,
// used only when the runtime implements runtime.JobCapableRuntime. They
// reuse sideScript unchanged (the FIFO/tee/checksum protocol is identical
// to the Docker path) and never reference any Kubernetes-specific type —
// everything Kubernetes-shaped lives behind the runtime.JobCapableRuntime
// port, realized by internal/adapters/runtime/kubernetes/job.go.

const (
	jobProducerContainerName = "producer"
	jobConsumerContainerName = "consumer"
	jobOneShotContainerName  = "oneshot"
)

// jobContainerSpec builds one side's runtime.JobContainerSpec around
// sideScript — the Job-path mirror of sideSpec.
func jobContainerSpec(name string, side Side, toPipe bool) runtime.JobContainerSpec {
	return runtime.JobContainerSpec{
		Name:       name,
		Image:      side.Image,
		Entrypoint: []string{"sh", "-c"},
		Cmd:        []string{sideScript(toPipe, side)},
		Env:        side.Env,
		Files:      side.Files,
		Volumes:    side.Volumes,
	}
}

// jobDiagnostics pulls a bounded log tail from each named container — the
// Job-path mirror of diagnostics.
func jobDiagnostics(ctx context.Context, jrt runtime.JobCapableRuntime, namespace, jobName string, containerNames ...string) string {
	var sb strings.Builder
	for _, name := range containerNames {
		logs, _ := jrt.JobLogs(ctx, namespace, jobName, name, 40)
		fmt.Fprintf(&sb, "\n%s logs:\n%s", name, logs)
	}
	return sb.String()
}

// runJobPipeline is RunPipeline's Kubernetes Job-path realization: one Job,
// one pod, Producer and Consumer as sibling containers sharing an emptyDir
// at WorkDir. Mirrors RunPipeline's own shape (start both sides, wait,
// force-remove on failure before Cleanup runs) as closely as the
// port allows.
func runJobPipeline(ctx context.Context, jrt runtime.JobCapableRuntime, spec PipelineSpec) (Result, error) {
	producerTimeout := spec.ProducerTimeout
	if producerTimeout <= 0 {
		producerTimeout = spec.Timeout
	}
	if producerTimeout <= 0 {
		producerTimeout = DefaultTimeout
	}
	consumerTimeout := spec.ConsumerTimeout
	if consumerTimeout <= 0 {
		consumerTimeout = spec.Timeout
	}
	if consumerTimeout <= 0 {
		consumerTimeout = DefaultTimeout
	}

	jobSpec := runtime.JobSpec{
		Name:      spec.JobName,
		Namespace: spec.Namespace,
		Labels:    spec.Labels,
		Containers: []runtime.JobContainerSpec{
			jobContainerSpec(jobProducerContainerName, spec.Producer, redirectTo),
			jobContainerSpec(jobConsumerContainerName, spec.Consumer, redirectFrom),
		},
		SharedVolumeMountPath: WorkDir,
	}
	if _, err := jrt.EnsureJob(ctx, jobSpec); err != nil {
		return Result{}, fmt.Errorf("job %q: ensure job: %w", spec.JobName, err)
	}

	result, err := waitJobPipeline(ctx, jrt, spec.JobName, spec.Namespace, producerTimeout, consumerTimeout)
	// Whether the pipeline succeeded or failed, the Job (and its
	// keep-alive reader container) is no longer needed once
	// waitJobPipeline has already read back the result/diagnostics it
	// needs — remove it before Cleanup runs, the same "force-remove both
	// sides first" ordering RunPipeline's Docker path uses to close the
	// race where a not-yet-killed consumer could still complete an
	// in-flight multipart upload after Cleanup already ran and found
	// nothing to delete.
	_ = jrt.RemoveJob(context.WithoutCancel(ctx), spec.Namespace, spec.JobName)
	if err == nil {
		return result, nil
	}
	if spec.Cleanup != nil {
		cleanup := *spec.Cleanup
		if cleanup.Namespace == "" {
			cleanup.Namespace = spec.Namespace
		}
		if _, cerr := runOneShotJob(ctx, jrt, spec.JobName+"-cleanup", spec.Labels, cleanup, cleanupTimeout, ""); cerr != nil {
			err = fmt.Errorf("%w (partial-object cleanup after failure also failed: %s)", err, cerr)
		}
	}
	return Result{}, err
}

// waitJobPipeline polls both sides via InspectJob and returns the moment
// either one is known to have failed — the Job-path mirror of waitPipeline,
// reading each container's state (running/terminated/exit code) straight
// from Kubernetes' own per-container status rather than dbjob's
// exit-file-sentinel convention (a strictly better signal that works
// without needing to exec into a container that may have already
// terminated — docs/adr/007-backup-restore.md addendum 3).
func waitJobPipeline(ctx context.Context, jrt runtime.JobCapableRuntime, jobName, namespace string, producerTimeout, consumerTimeout time.Duration) (Result, error) {
	producerDeadline := time.Now().Add(producerTimeout)
	consumerDeadline := time.Now().Add(consumerTimeout)
	for {
		state, found, err := jrt.InspectJob(ctx, namespace, jobName)
		if err != nil {
			return Result{}, fmt.Errorf("job %q: %w", jobName, err)
		}
		if !found {
			return Result{}, fmt.Errorf("job %q: disappeared before completing", jobName)
		}
		producer := state.Containers[jobProducerContainerName]
		consumer := state.Containers[jobConsumerContainerName]

		if producer.Terminated && producer.ExitCode != 0 {
			return Result{}, fmt.Errorf("job %q: producer failed (exit=%d)%s", jobName, producer.ExitCode, jobDiagnostics(ctx, jrt, namespace, jobName, jobProducerContainerName, jobConsumerContainerName))
		}
		if consumer.Terminated && consumer.ExitCode != 0 {
			return Result{}, fmt.Errorf("job %q: consumer failed (exit=%d)%s", jobName, consumer.ExitCode, jobDiagnostics(ctx, jrt, namespace, jobName, jobProducerContainerName, jobConsumerContainerName))
		}
		if producer.Terminated && consumer.Terminated {
			if err := checkJobSideExits(ctx, jrt, namespace, jobName); err != nil {
				return Result{}, err
			}
			return readJobResult(ctx, jrt, namespace, jobName)
		}
		now := time.Now()
		if !producer.Terminated && now.After(producerDeadline) {
			return Result{}, fmt.Errorf("job %q: producer did not finish within %s%s", jobName, producerTimeout, jobDiagnostics(ctx, jrt, namespace, jobName, jobProducerContainerName, jobConsumerContainerName))
		}
		if !consumer.Terminated && now.After(consumerDeadline) {
			return Result{}, fmt.Errorf("job %q: consumer did not finish within %s%s", jobName, consumerTimeout, jobDiagnostics(ctx, jrt, namespace, jobName, jobProducerContainerName, jobConsumerContainerName))
		}
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// checkJobSideExits reads both sides' sentinel exit files — the SAME
// verdict source the Docker path uses (pollSide/readExitCode). It exists
// because sideScript deliberately routes the real command's exit code into
// WorkDir/{producer,consumer}-exit and then ends with cp/echo, so the
// CONTAINER always terminates 0: Kubernetes' ContainerStatuses ExitCode is
// truthful about the script, not the command, and trusting it alone let a
// failed pg_dump masquerade as a successful 0-byte backup (found by the
// I15 live round-trip). The ExitCode checks above still stand — they catch
// script-level infrastructure failures (mkfifo, missing shell) that never
// reach the sentinel files.
func checkJobSideExits(ctx context.Context, jrt runtime.JobCapableRuntime, namespace, jobName string) error {
	for side, file := range map[string]string{
		"producer": WorkDir + "/producer-exit",
		"consumer": WorkDir + "/consumer-exit",
	} {
		data, err := jrt.ReadJobFile(ctx, namespace, jobName, file)
		if err != nil {
			return fmt.Errorf("job %q: read %s exit sentinel: %w", jobName, side, err)
		}
		code, perr := strconv.Atoi(strings.TrimSpace(string(data)))
		if perr != nil {
			return fmt.Errorf("job %q: parse %s exit sentinel %q: %w", jobName, side, data, perr)
		}
		if code != 0 {
			return fmt.Errorf("job %q: %s failed (exit=%d)%s", jobName, side, code, jobDiagnostics(ctx, jrt, namespace, jobName, jobProducerContainerName, jobConsumerContainerName))
		}
	}
	return nil
}

// readJobResult reads back the producer-side checksum/byte-count files via
// ReadJobFile — the Job-path mirror of readResult.
func readJobResult(ctx context.Context, jrt runtime.JobCapableRuntime, namespace, jobName string) (Result, error) {
	sumData, err := jrt.ReadJobFile(ctx, namespace, jobName, WorkDir+"/producer-checksum")
	if err != nil {
		return Result{}, fmt.Errorf("job %q: read producer checksum: %w", jobName, err)
	}
	bytesData, err := jrt.ReadJobFile(ctx, namespace, jobName, WorkDir+"/producer-bytes")
	if err != nil {
		return Result{}, fmt.Errorf("job %q: read producer byte count: %w", jobName, err)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(bytesData)), 10, 64)
	if err != nil {
		return Result{}, fmt.Errorf("job %q: parse producer byte count %q: %w", jobName, bytesData, err)
	}
	sum := strings.TrimSpace(string(sumData))
	if sum == "" {
		return Result{}, fmt.Errorf("job %q: producer checksum file is empty", jobName)
	}
	return Result{SHA256: sum, Bytes: n}, nil
}

// runOneShotJob is RunOneShot's Kubernetes Job-path realization: a
// single-container Job. Exit codes come natively from
// pod.Status.ContainerStatuses (no Docker-style exit-sentinel file), and a
// non-empty resultPath is copied into the pod's shared emptyDir (WorkDir)
// on success so ReadJobFile — which reads through the keep-alive reader
// container, the only filesystem that outlives the one-shot container —
// can return it: the one-shot's own /tmp is invisible to the reader.
func runOneShotJob(ctx context.Context, jrt runtime.JobCapableRuntime, name string, labels map[string]string, side Side, timeout time.Duration, resultPath string) ([]byte, error) {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	script := side.ShellCmd
	sharedMount := ""
	if resultPath != "" {
		sharedMount = WorkDir
		script = fmt.Sprintf("(%s) && cp %s %s/oneshot-result", side.ShellCmd, resultPath, WorkDir)
	}
	jobSpec := runtime.JobSpec{
		Name:      name,
		Namespace: side.Namespace,
		Labels:    labels,
		Containers: []runtime.JobContainerSpec{{
			Name:       jobOneShotContainerName,
			Image:      side.Image,
			Entrypoint: []string{"sh", "-c"},
			Cmd:        []string{script},
			Env:        side.Env,
			Files:      side.Files,
			Volumes:    side.Volumes,
		}},
		SharedVolumeMountPath: sharedMount,
		NodeName:              side.NodeName,
	}
	defer func() { _ = jrt.RemoveJob(context.WithoutCancel(ctx), side.Namespace, name) }()
	if _, err := jrt.EnsureJob(ctx, jobSpec); err != nil {
		return nil, fmt.Errorf("job %q: start: %w", name, err)
	}
	deadline := time.Now().Add(timeout)
	for {
		state, found, err := jrt.InspectJob(ctx, side.Namespace, name)
		if err != nil {
			return nil, fmt.Errorf("job %q: inspect: %w", name, err)
		}
		if !found {
			return nil, fmt.Errorf("job %q: disappeared before completing", name)
		}
		c := state.Containers[jobOneShotContainerName]
		if c.Terminated {
			if c.ExitCode != 0 {
				return nil, fmt.Errorf("job %q: failed (exit=%d)%s", name, c.ExitCode, jobDiagnostics(ctx, jrt, side.Namespace, name, jobOneShotContainerName))
			}
			if resultPath == "" {
				return nil, nil
			}
			out, err := jrt.ReadJobFile(ctx, side.Namespace, name, WorkDir+"/oneshot-result")
			if err != nil {
				return nil, fmt.Errorf("job %q: read result %q: %w", name, resultPath, err)
			}
			return out, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("job %q: did not finish within %s", name, timeout)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// jobRuntime obtains the JobCapableRuntime capability once the Kubernetes
// path has been chosen by RuntimeType — the assertion OBTAINS the
// capability, it never DECIDES the path (mirroring ingress's
// ingressRuntime).
func jobRuntime(rt runtime.ContainerRuntime) (runtime.JobCapableRuntime, error) {
	jrt, ok := rt.(runtime.JobCapableRuntime)
	if !ok {
		return nil, fmt.Errorf("dbjob: runtime does not implement JobCapableRuntime (expected on a Kubernetes-runtime Provider)")
	}
	return jrt, nil
}

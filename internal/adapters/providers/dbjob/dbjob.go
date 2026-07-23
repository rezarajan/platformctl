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
	JobName  string
	Labels   map[string]string
	Producer Side
	Consumer Side
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
func RunPipeline(ctx context.Context, rt runtime.ContainerRuntime, spec PipelineSpec) (Result, error) {
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
		if _, cerr := RunOneShot(ctx, rt, spec.JobName+"-cleanup", spec.Labels, *spec.Cleanup, cleanupTimeout, ""); cerr != nil {
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

// sideSpec builds one side's ContainerSpec. The producer side (toPipe)
// tees its stdout through two extra FIFOs (hashFifo/sizeFifo) so a
// checksum and byte count of the exact bytes crossing PipePath are
// computed by backgrounded GNU coreutils processes — no process
// substitution, no landing the payload as a file (docs/adr/
// 007-backup-restore.md's I12 addendum). cmd's own exit status is captured
// inside a subshell BEFORE tee (the pipeline's last stage) can obscure it:
// `( cmd; echo $? > cmdrc ) | tee ...` — the subshell always finishes
// (including writing cmdrc) before the whole pipeline construct returns,
// regardless of tee's own exit status.
func sideSpec(name, volName string, labels map[string]string, side Side, toPipe bool) runtime.ContainerSpec {
	var script string
	if toPipe {
		cmdrc := WorkDir + "/producer-cmdrc"
		script = fmt.Sprintf(
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
	} else {
		script = fmt.Sprintf("mkfifo %s 2>/dev/null; (%s) < %s; echo $? > %s/consumer-exit", PipePath, side.ShellCmd, PipePath, WorkDir)
	}
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

// RunOneShot runs a single short-lived container to completion — no FIFO,
// no shared volume, just Side's own Files/Env/Networks — and returns once
// it has stopped, translating a nonzero exit code, a container that
// vanished, or a timeout into a named error. Used for the small,
// self-contained housekeeping steps around a RunPipeline call:
// PersistManifest/ReadManifest (the integrity-manifest sidecar), and
// RunPipeline's own Cleanup wiring (best-effort partial-object removal
// after a failure). If resultPath is non-empty, its content is read back
// (before the container is removed) and returned on success.
func RunOneShot(ctx context.Context, rt runtime.ContainerRuntime, name string, labels map[string]string, side Side, timeout time.Duration, resultPath string) ([]byte, error) {
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

// PersistManifest uploads manifest as the JSON sidecar object
// "<key><manifestSuffix>" next to the backup object it describes — the
// durable, out-of-band integrity record a later, separate `restore`
// invocation reads back via ReadManifest before trusting anything it
// downloads (docs/adr/007-backup-restore.md's I12 addendum). loc must
// already carry resolved credentials, exactly like every other Location
// use; key is the exact object key the backup landed at.
func PersistManifest(ctx context.Context, rt runtime.ContainerRuntime, jobName string, labels map[string]string, loc backup.Location, key string, manifest backup.Manifest) error {
	data, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("job %q: encode manifest sidecar: %w", jobName, err)
	}
	mcConfig, err := MCConfig(loc)
	if err != nil {
		return err
	}
	side := Side{
		Image:    MCImage,
		Networks: networksFor(loc),
		Env:      map[string]string{"MC_CONFIG_DIR": MCConfigDir},
		Files: []runtime.FileMount{
			{Path: MCConfigPath, Content: mcConfig, Mode: 0o600},
			{Path: manifestMountPath, Content: data, Mode: 0o600},
		},
		ShellCmd: fmt.Sprintf("mc pipe %s/%s/%s < %s", MCAlias, loc.Bucket, key+manifestSuffix, manifestMountPath),
	}
	if _, err := RunOneShot(ctx, rt, jobName+"-manifest-write", labels, side, manifestTimeout, ""); err != nil {
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
// limitation (d)).
func ReadManifest(ctx context.Context, rt runtime.ContainerRuntime, jobName string, labels map[string]string, loc backup.Location, key string) (backup.Manifest, error) {
	mcConfig, err := MCConfig(loc)
	if err != nil {
		return backup.Manifest{}, err
	}
	side := Side{
		Image:    MCImage,
		Networks: networksFor(loc),
		Env:      map[string]string{"MC_CONFIG_DIR": MCConfigDir},
		Files:    []runtime.FileMount{{Path: MCConfigPath, Content: mcConfig, Mode: 0o600}},
		ShellCmd: fmt.Sprintf("mc cat %s/%s/%s > %s", MCAlias, loc.Bucket, key+manifestSuffix, manifestReadPath),
	}
	out, err := RunOneShot(ctx, rt, jobName+"-manifest-read", labels, side, manifestTimeout, manifestReadPath)
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

package runtime

import "context"

// JobContainerSpec is one container in a JobCapableRuntime Job's pod — a
// slimmer sibling of ContainerSpec, carrying only what a run-to-completion
// job container needs (docs/adr/007-backup-restore.md addendum 3).
type JobContainerSpec struct {
	Name string
	// Image, Entrypoint, Cmd, Env, Files mirror ContainerSpec's own fields
	// exactly (same semantics: Entrypoint replaces the image's own
	// ENTRYPOINT, Cmd is the shell command run under it).
	Image      string
	Entrypoint []string
	Cmd        []string
	Env        map[string]string
	Files      []FileMount
	// Volumes names EXISTING named volumes (created by a prior
	// EnsureVolume call) to mount into THIS container only — distinct
	// from JobSpec.SharedVolumeMountPath, which every container in the
	// Job's pod mounts automatically. Used by the I13 disk-headroom
	// precheck to mount a running instance's own data volume from a
	// throwaway container.
	Volumes []VolumeMount
}

// JobSpec describes one Kubernetes Job realization: a single pod running
// every container in Containers as siblings, sharing one ephemeral
// directory. Mirrors dbjob's producer/consumer/cleanup shapes without
// dbjob (or any provider) knowing anything about Kubernetes — the
// dbjob<->JobCapableRuntime boundary is the only place this type is built
// or consumed (docs/adr/007-backup-restore.md addendum 3).
type JobSpec struct {
	// Name identifies the Job (and its pod) — must be unique per
	// invocation, exactly like PipelineSpec.JobName.
	Name string
	// Namespace is the Kubernetes namespace to schedule the Job's pod in
	// — explicit and unambiguous, unlike Docker's per-container Networks
	// list (which can name several networks to join at once; a
	// Kubernetes pod belongs to exactly one namespace).
	Namespace string
	Labels    map[string]string
	// Containers runs as sibling containers in one pod — order is
	// insignificant; each Name must be unique within the Job.
	Containers []JobContainerSpec
	// SharedVolumeMountPath, when non-empty, mounts one ephemeral
	// (pod-lifetime, not a named EnsureVolume-created volume) empty
	// directory at this exact path in every container — the FIFO work
	// directory dbjob's protocol depends on. Realized as a Kubernetes
	// emptyDir.
	SharedVolumeMountPath string
	// NodeName, when non-empty, pins the Job's pod to this exact node — a
	// hard scheduling constraint (spec.nodeName), not a soft affinity
	// preference. Used by I13's disk-headroom precheck to co-locate with
	// an already-running instance pod so both can mount the same
	// ReadWriteOnce PersistentVolumeClaim (a PVC's RWO access mode
	// guarantees a single NODE, not a single pod — two pods on the same
	// node may both mount it).
	NodeName string
}

// JobContainerState reports one named container's observed state within a
// Job's pod.
type JobContainerState struct {
	// Found is false when no container by this name exists in the Job's
	// pod (a caller error, or the pod itself is gone).
	Found bool
	// Running is true while the container's process has not yet exited.
	Running bool
	// Terminated is true once the container's process has exited —
	// mutually exclusive with Running. Neither is true before the
	// container has been scheduled/started (e.g. still pulling its
	// image).
	Terminated bool
	// ExitCode is meaningful only when Terminated is true — read from
	// Kubernetes' own per-container status
	// (pod.Status.ContainerStatuses[i].State.Terminated.ExitCode), never
	// from a sentinel file: a strictly better signal than dbjob's
	// exit-file convention (which the shared shell script still writes,
	// for Docker parity — the Kubernetes realization simply never reads
	// it back).
	ExitCode int
}

// JobState reports every named container's observed state within a Job.
type JobState struct {
	// PodName is the Kubernetes-assigned pod name backing this Job —
	// useful for diagnostics (kubectl logs/describe), never required for
	// any JobCapableRuntime method (every method takes the Job's own
	// stable Name, not PodName).
	PodName    string
	Containers map[string]JobContainerState
}

// JobCapableRuntime is an optional ContainerRuntime capability (the same
// type-assert-an-optional-capability pattern as IngressCapableRuntime):
// implemented only by the Kubernetes adapter, since realizing a
// run-to-completion multi-container pipeline as a Kubernetes Job is a
// genuinely different shape from EnsureContainer's Deployment/StatefulSet
// realization — not a mechanical port of it (docs/adr/007-backup-restore.md
// addendum 3, closing known limitation (c)). internal/adapters/providers/
// dbjob type-asserts req.Runtime against this interface and takes the Job
// path only when present; Docker/fake's existing per-side EnsureContainer
// path is completely unchanged when it is absent.
type JobCapableRuntime interface {
	// EnsureJob creates spec's Job. Name (and every name derived from
	// it, e.g. per-side file Secrets) must be a lowercase RFC 1123
	// subdomain — callers embedding timestamps use lowercase formats
	// ("20060102t150405z"), since Kubernetes rejects uppercase object
	// names outright and adapters must NOT silently sanitize (a
	// lowercasing rewrite could collide two distinct caller names).
	//
	// EnsureJob creates spec's Job (a single pod running every Containers
	// entry as a sibling container sharing one emptyDir at
	// SharedVolumeMountPath) if it does not already exist. Idempotent
	// like every other Ensure* method on the ContainerRuntime port: a
	// second call with the same Name is a no-op even if spec's content
	// differs (a Job's pod template is immutable once created — unlike
	// EnsureContainer, there is no in-place update path; a caller that
	// needs a different spec removes the Job first via RemoveJob).
	EnsureJob(ctx context.Context, spec JobSpec) (JobState, error)
	// InspectJob reports the current state of every container in the
	// named Job's pod. found is false when no such Job exists.
	InspectJob(ctx context.Context, namespace, name string) (state JobState, found bool, err error)
	// ReadJobFile reads path from the Job's shared volume — works
	// regardless of whether any particular container has already
	// terminated (realized via an internal keep-alive reader container
	// the Kubernetes adapter always adds to the pod; dbjob never
	// references it directly).
	ReadJobFile(ctx context.Context, namespace, name, path string) ([]byte, error)
	// JobLogs returns the last `tail` lines of one container's combined
	// stdout/stderr — works after termination (Kubernetes' pods/log
	// subresource remains available as long as the pod object exists).
	JobLogs(ctx context.Context, namespace, name, containerName string, tail int) (string, error)
	// RemoveJob deletes the Job and its pod — the CLI, not the cluster,
	// owns this object's lifetime (the keep-alive reader container means
	// the pod never reaches Kubernetes' own Succeeded phase on its own).
	// A no-op, not an error, if already gone.
	RemoveJob(ctx context.Context, namespace, name string) error
	// NodeNameOf resolves the Kubernetes node currently hosting the named
	// (non-Job) container/workload — used to co-locate a headroom-check
	// Job with a running database instance (JobSpec.NodeName) so both can
	// mount the same ReadWriteOnce PersistentVolumeClaim.
	NodeNameOf(ctx context.Context, name string) (string, error)
}

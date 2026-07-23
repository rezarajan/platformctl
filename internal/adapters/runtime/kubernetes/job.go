// This file realizes runtime.JobCapableRuntime (docs/adr/
// 007-backup-restore.md addendum 3, docs/planning/08 I15): a single
// Kubernetes Job whose pod runs every JobSpec.Containers entry as a sibling
// container sharing one emptyDir — dbjob's producer/consumer/cleanup
// pipeline realized without forking its protocol. A run-to-completion
// pipeline is a genuinely different shape from EnsureContainer's
// Deployment/StatefulSet realization (see kubernetes.go's package doc), so
// this is deliberately its own object kind rather than a special case of
// buildDeployment.
//
// The keep-alive reader container (jobReaderContainerName) is the one
// non-obvious piece: Kubernetes' pods/exec subresource cannot reach a
// container that has already terminated (confirmed against this adapter's
// own ReadFile/exec.go before writing this file — unlike Docker's `docker
// cp` on a stopped-but-not-removed container), so a bare producer+consumer
// pod would make readResult's post-completion checksum/byte-count file
// reads impossible the moment either side finishes. Every EnsureJob call
// therefore adds one extra, always-running container (image taken from
// Containers[0] — already pinned/trusted, no new digest to track) that
// ReadJobFile always execs into regardless of which "real" container
// wrote the file — invisible to dbjob, which only ever calls
// ReadJobFile(ctx, ns, jobName, path).
package kubernetes

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

const (
	// jobSharedVolumeName is the emptyDir every container in a Job's pod
	// mounts at JobSpec.SharedVolumeMountPath — the Kubernetes-native
	// replacement for dbjob's Docker named work volume; the FIFO path and
	// protocol inside it are unchanged.
	jobSharedVolumeName = "datascape-work"
	// jobReaderContainerName is the always-on keep-alive container — see
	// this file's package doc comment for why it exists.
	jobReaderContainerName = "datascape-reader"
	// jobReaderSleepSeconds/jobActiveDeadlineSeconds bound the reader's own
	// keep-alive command and the Job's overall pod lifetime as a safety
	// net — the normal path always ends sooner, via an explicit RemoveJob
	// call once dbjob has read back everything it needs (docs/adr/
	// 007-backup-restore.md addendum 3, known limitation (f): the CLI, not
	// the cluster, owns this object's lifetime).
	jobReaderSleepSeconds    = 3600
	jobActiveDeadlineSeconds = 3600
	// jobLabelKey groups a Job's own housekeeping objects (per-container
	// Files Secrets) for RemoveJob's cleanup — distinct from Kubernetes'
	// own automatic "job-name" pod label, which only appears on Pods the
	// Job controller creates, never on Secrets.
	jobLabelKey = "io.datascape.job-name"
)

func jobFilesSecretName(jobName, containerName string) string {
	return jobName + "-" + containerName + "-files"
}

func jobFilesVolumeName(containerName string) string { return "files-" + containerName }

// EnsureJob creates spec's Job if it does not already exist — idempotent by
// name, like every other Ensure* method on this port; a Job's pod template
// is immutable once created, so a second call is a no-op regardless of
// spec's content (every caller uses a freshly time-stamped, unique Name per
// invocation, exactly like dbjob.PipelineSpec.JobName already requires).
func (r *Runtime) EnsureJob(ctx context.Context, spec runtimeport.JobSpec) (runtimeport.JobState, error) {
	if spec.Namespace == "" {
		return runtimeport.JobState{}, fmt.Errorf("job %q: no namespace specified", spec.Name)
	}
	if len(spec.Containers) == 0 {
		return runtimeport.JobState{}, fmt.Errorf("job %q: no containers specified", spec.Name)
	}
	existing, err := r.clientset.BatchV1().Jobs(spec.Namespace).Get(ctx, spec.Name, metav1.GetOptions{})
	if err == nil {
		if existing.Labels[runtimeport.LabelManagedBy] != runtimeport.ManagedByValue {
			return runtimeport.JobState{}, fmt.Errorf("job %q exists but is not managed by platformctl; refusing to replace it", spec.Name)
		}
		state, _, ierr := r.InspectJob(ctx, spec.Namespace, spec.Name)
		return state, ierr
	} else if !apierrors.IsNotFound(err) {
		return runtimeport.JobState{}, fmt.Errorf("get job %q: %w", spec.Name, err)
	}

	for _, c := range spec.Containers {
		if len(c.Files) == 0 {
			continue
		}
		if err := r.ensureJobContainerFilesSecret(ctx, spec, c); err != nil {
			return runtimeport.JobState{}, err
		}
	}

	job, err := buildJob(spec)
	if err != nil {
		return runtimeport.JobState{}, err
	}
	if _, err := r.clientset.BatchV1().Jobs(spec.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return runtimeport.JobState{}, fmt.Errorf("create job %q: %w", spec.Name, err)
	}
	state, _, err := r.InspectJob(ctx, spec.Namespace, spec.Name)
	return state, err
}

// ensureJobContainerFilesSecret renders one container's Files into its own
// Secret — per-container (not per-Job), since sibling containers in the
// same Job's pod may declare different files at the same path.
func (r *Runtime) ensureJobContainerFilesSecret(ctx context.Context, spec runtimeport.JobSpec, c runtimeport.JobContainerSpec) error {
	data := make(map[string][]byte, len(c.Files))
	for i, f := range c.Files {
		data[fileKey(i)] = f.Content
	}
	labels := withOwnership(spec.Labels)
	labels[jobLabelKey] = spec.Name
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobFilesSecretName(spec.Name, c.Name),
			Namespace: spec.Namespace,
			Labels:    labels,
		},
		Data: data,
	}
	// No path->key annotation is needed here (unlike buildFilesSecret's
	// Deployment path, which ReadFile's fast path reads directly): a Job
	// container's Files are only ever consumed by the container's own
	// mounted subPath, never read back through ReadJobFile — that always
	// goes through the reader container's live filesystem instead.
	_, err := r.clientset.CoreV1().Secrets(spec.Namespace).Create(ctx, secret, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create job files secret %q: %w", secret.Name, err)
	}
	return nil
}

// buildJob translates spec into a Kubernetes Job: every declared container
// as a sibling in one pod, sharing spec.SharedVolumeMountPath as an
// emptyDir, plus the always-on reader container (see package doc).
// RestartPolicy is Never and BackoffLimit is 0 — a producer/consumer
// container that exits (any code) is never restarted, matching dbjob's own
// run-once contract; the reader container's deliberate non-termination
// means the pod never reaches a terminal phase on its own, which is why
// InspectJob reads per-container status directly rather than trusting
// Job.status.succeeded (known limitation (f)).
func buildJob(spec runtimeport.JobSpec) (*batchv1.Job, error) {
	labels := withOwnership(spec.Labels)
	labels["app"] = spec.Name

	var containers []corev1.Container
	var volumes []corev1.Volume
	seenVolumes := map[string]bool{}
	addPVCVolume := func(name string) {
		if seenVolumes[name] {
			return
		}
		seenVolumes[name] = true
		volumes = append(volumes, corev1.Volume{
			Name:         name,
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: name}},
		})
	}

	for _, c := range spec.Containers {
		if c.Name == "" || c.Name == jobReaderContainerName {
			return nil, fmt.Errorf("job %q: container name %q is invalid or reserved", spec.Name, c.Name)
		}
		container := corev1.Container{
			Name:            c.Name,
			Image:           c.Image,
			ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         c.Entrypoint,
			Args:            c.Cmd,
			Env:             envVars(c.Env),
		}
		if spec.SharedVolumeMountPath != "" {
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: jobSharedVolumeName, MountPath: spec.SharedVolumeMountPath})
		}
		for _, vm := range c.Volumes {
			addPVCVolume(vm.VolumeName)
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: vm.VolumeName, MountPath: vm.MountPath})
		}
		if len(c.Files) > 0 {
			for i, f := range c.Files {
				container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
					Name:      jobFilesVolumeName(c.Name),
					MountPath: f.Path,
					SubPath:   fileKey(i),
					ReadOnly:  true,
				})
			}
			mode := int32(0o444)
			volumes = append(volumes, corev1.Volume{
				Name: jobFilesVolumeName(c.Name),
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: jobFilesSecretName(spec.Name, c.Name), DefaultMode: &mode, Items: fileItems(c.Files)},
				},
			})
		}
		containers = append(containers, container)
	}

	if spec.SharedVolumeMountPath != "" {
		volumes = append(volumes, corev1.Volume{
			Name:         jobSharedVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		})
	}

	reader := corev1.Container{
		Name:    jobReaderContainerName,
		Image:   spec.Containers[0].Image,
		Command: []string{"sh", "-c"},
		Args:    []string{fmt.Sprintf("sleep %d", jobReaderSleepSeconds)},
	}
	if spec.SharedVolumeMountPath != "" {
		reader.VolumeMounts = append(reader.VolumeMounts, corev1.VolumeMount{Name: jobSharedVolumeName, MountPath: spec.SharedVolumeMountPath})
	}
	containers = append(containers, reader)

	backoffLimit := int32(0)
	activeDeadline := int64(jobActiveDeadlineSeconds)
	podSpec := corev1.PodSpec{
		RestartPolicy:                 corev1.RestartPolicyNever,
		TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
		Containers:                    containers,
		Volumes:                       volumes,
	}
	if spec.NodeName != "" {
		// A hard placement, not an affinity preference — I13's
		// disk-headroom precheck needs co-location with one specific
		// already-running pod so both can mount the same ReadWriteOnce
		// PersistentVolumeClaim (docs/adr/007-backup-restore.md addendum
		// 3).
		podSpec.NodeName = spec.NodeName
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: spec.Name, Namespace: spec.Namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &backoffLimit,
			ActiveDeadlineSeconds: &activeDeadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       podSpec,
			},
		},
	}, nil
}

// findJobPod returns the Job's own pod (Kubernetes labels every pod it
// creates for a Job with "job-name=<name>"), or nil if none exists yet. A
// BackoffLimit:0 Job creates exactly one pod under normal operation; if
// more than one is somehow found (a stale object from a prior failed
// create), the newest is used.
func (r *Runtime) findJobPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	pods, err := r.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + name})
	if err != nil {
		return nil, fmt.Errorf("list pods for job %q: %w", name, err)
	}
	if len(pods.Items) == 0 {
		return nil, nil
	}
	best := &pods.Items[0]
	for i := range pods.Items {
		if pods.Items[i].CreationTimestamp.After(best.CreationTimestamp.Time) {
			best = &pods.Items[i]
		}
	}
	return best, nil
}

// InspectJob reports every container's state straight from
// pod.Status.ContainerStatuses — never from Job.status, which a
// deliberately-never-exiting reader container would leave perpetually
// incomplete (this file's package doc comment).
func (r *Runtime) InspectJob(ctx context.Context, namespace, name string) (runtimeport.JobState, bool, error) {
	if _, err := r.clientset.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{}); apierrors.IsNotFound(err) {
		return runtimeport.JobState{}, false, nil
	} else if err != nil {
		return runtimeport.JobState{}, false, fmt.Errorf("get job %q: %w", name, err)
	}
	state := runtimeport.JobState{Containers: map[string]runtimeport.JobContainerState{}}
	pod, err := r.findJobPod(ctx, namespace, name)
	if err != nil {
		return runtimeport.JobState{}, false, err
	}
	if pod == nil {
		return state, true, nil // Job exists, its pod hasn't been created/scheduled yet
	}
	state.PodName = pod.Name
	for _, cs := range pod.Status.ContainerStatuses {
		st := runtimeport.JobContainerState{Found: true}
		switch {
		case cs.State.Running != nil:
			st.Running = true
		case cs.State.Terminated != nil:
			st.Terminated = true
			st.ExitCode = int(cs.State.Terminated.ExitCode)
		}
		state.Containers[cs.Name] = st
	}
	return state, true, nil
}

// ReadJobFile execs `cat <path>` in the Job's always-on reader container —
// works regardless of whether any "real" container has already terminated
// (this file's package doc comment).
func (r *Runtime) ReadJobFile(ctx context.Context, namespace, name, path string) ([]byte, error) {
	pod, err := r.findJobPod(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	if pod == nil {
		return nil, fmt.Errorf("job %q: no pod found to read %q from", name, path)
	}
	stdout, stderr, err := r.execInPod(ctx, namespace, pod.Name, jobReaderContainerName, []string{"cat", path})
	if err != nil {
		return nil, fmt.Errorf("job %q: read %q: %w (stderr: %s)", name, path, err, strings.TrimSpace(stderr))
	}
	return []byte(stdout), nil
}

// JobLogs returns one container's log tail — the pods/log subresource
// remains available after a container terminates (unlike pods/exec), so
// this needs no reader-container indirection.
func (r *Runtime) JobLogs(ctx context.Context, namespace, name, containerName string, tail int) (string, error) {
	pod, err := r.findJobPod(ctx, namespace, name)
	if err != nil {
		return "", err
	}
	if pod == nil {
		return "", nil
	}
	if tail <= 0 {
		tail = 200
	}
	tailInt64 := int64(tail)
	rc, err := r.clientset.CoreV1().Pods(namespace).GetLogs(pod.Name, &corev1.PodLogOptions{Container: containerName, TailLines: &tailInt64}).Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("job %q: read logs for container %q: %w", name, containerName, err)
	}
	defer rc.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return "", fmt.Errorf("job %q: read logs for container %q: %w", name, containerName, err)
	}
	return strings.TrimSpace(buf.String()), nil
}

// RemoveJob deletes the Job (foreground propagation cascades to its Pod,
// matching removeDeployment's synchronous teardown contract) plus every
// per-container Files Secret this Job created — those carry no
// ownerReference to the Job (only Pods do, automatically), so they need
// explicit label-selected cleanup. A no-op, not an error, if already gone.
func (r *Runtime) RemoveJob(ctx context.Context, namespace, name string) error {
	propagation := metav1.DeletePropagationForeground
	if err := r.clientset.BatchV1().Jobs(namespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &propagation}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete job %q: %w", name, err)
	}
	secrets, err := r.clientset.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{LabelSelector: jobLabelKey + "=" + name})
	if err != nil {
		return fmt.Errorf("list files secrets for job %q: %w", name, err)
	}
	for _, s := range secrets.Items {
		if err := r.clientset.CoreV1().Secrets(namespace).Delete(ctx, s.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete files secret %q: %w", s.Name, err)
		}
	}
	return r.waitObjectGone(ctx, func() error {
		_, err := r.clientset.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		return err
	}, "job", name)
}

// NodeNameOf resolves the Kubernetes node currently hosting name's running
// pod — a Deployment's newest ready pod, or a StatefulSet ordinal's own
// pod, the same two lookup paths ReadFile/Logs already use. Used by I13's
// disk-headroom precheck (JobSpec.NodeName) to co-locate a headroom-check
// Job with the running database instance so both can mount the same
// ReadWriteOnce PersistentVolumeClaim.
func (r *Runtime) NodeNameOf(ctx context.Context, name string) (string, error) {
	ns, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return "", err
	}
	if d != nil {
		pods, err := r.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=" + name})
		if err != nil {
			return "", fmt.Errorf("list pods for %q: %w", name, err)
		}
		pod := newestReadyPod(pods.Items)
		if pod == nil {
			return "", fmt.Errorf("no ready pod for %q to resolve a node from", name)
		}
		return pod.Spec.NodeName, nil
	}
	_, pod, _, err := r.findOrdinalPod(ctx, name)
	if err != nil {
		return "", err
	}
	if pod == nil {
		return "", fmt.Errorf("no deployment or replica pod named %q found to resolve a node from", name)
	}
	return pod.Spec.NodeName, nil
}

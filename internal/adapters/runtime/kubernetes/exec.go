// This file holds the exec/logs seam: ReadFile's FileMount-and-live-exec
// paths, Logs' pod-log tail, and the pods/exec subresource plumbing they
// share (docs/planning/08 §7.6 G3).
package kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

// ReadFile maps the path back to its Secret key via the annotation
// buildFilesSecret wrote, then returns that key's data.
// ReadFile returns a FileMount's content when the path is one this adapter
// placed itself (the fast, no-exec path every provider's bootstrap-secret
// recovery actually uses); for any other path it falls back to a live
// `cat` inside the pod (docs/planning/08 B3) — content the container's own
// process wrote at runtime, e.g. into a mounted PersistentVolumeClaim. This
// mirrors the Docker adapter's ReadFile, which reads any live path via
// CopyFromContainer without a FileMount-vs-not distinction.
func (r *Runtime) ReadFile(ctx context.Context, name, path string) ([]byte, error) {
	_, secret, err := findAcrossNamespaces(ctx, r, func(ns string) (*corev1.Secret, error) {
		return r.clientset.CoreV1().Secrets(ns).Get(ctx, filesSecretName(name), metav1.GetOptions{})
	})
	if err != nil {
		return nil, err
	}
	if secret != nil {
		var paths map[string]string
		if err := json.Unmarshal([]byte(secret.Annotations[filePathsAnnotation]), &paths); err == nil {
			if key, ok := paths[path]; ok {
				return secret.Data[key], nil
			}
		}
	}
	return r.readFileViaExec(ctx, name, path)
}

// readFileViaExec execs `cat <path>` in the deployment's current running
// pod and returns stdout — the live-filesystem fallback ReadFile uses for
// paths that aren't a FileMount, e.g. content a container's own process
// wrote into a mounted volume. When name is not a Deployment, it may be the
// literal name of a StatefulSet ordinal's own Pod (docs/adr/004-replicas-
// and-identity.md) — the aggregate base name of a StableIdentity set is
// deliberately not supported here; callers must address a specific ordinal.
func (r *Runtime) readFileViaExec(ctx context.Context, name, path string) ([]byte, error) {
	ns, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return nil, err
	}
	if d != nil {
		// buildDeployment always names the (single) container after the
		// Deployment itself — see its ObjectMeta.Name/container.Name — so
		// name doubles as both the pod-selector value and the container to
		// exec into.
		return r.readFileFromSelector(ctx, ns, name, name, path)
	}
	podNS, pod, stsName, err := r.findOrdinalPod(ctx, name)
	if err != nil {
		return nil, err
	}
	if pod == nil {
		return nil, fmt.Errorf("no deployment or replica pod named %q found to read %q from", name, path)
	}
	stdout, stderr, err := r.execInPod(ctx, podNS, pod.Name, stsName, []string{"cat", path})
	if err != nil {
		return nil, fmt.Errorf("read %q from pod %q: %w (stderr: %s)", path, pod.Name, err, strings.TrimSpace(stderr))
	}
	return []byte(stdout), nil
}

// readFileFromSelector execs `cat <path>` in the newest ready pod matching
// app=selectorName — the Deployment-path lookup shared by ReadFile.
func (r *Runtime) readFileFromSelector(ctx context.Context, ns, selectorName, containerName, path string) ([]byte, error) {
	pods, err := r.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=" + selectorName})
	if err != nil {
		return nil, fmt.Errorf("list pods for %q: %w", selectorName, err)
	}
	pod := newestReadyPod(pods.Items)
	if pod == nil {
		return nil, fmt.Errorf("no ready pod for %q to read %q from", selectorName, path)
	}
	stdout, stderr, err := r.execInPod(ctx, ns, pod.Name, containerName, []string{"cat", path})
	if err != nil {
		return nil, fmt.Errorf("read %q from pod %q: %w (stderr: %s)", path, pod.Name, err, strings.TrimSpace(stderr))
	}
	return []byte(stdout), nil
}

// newestReadyPod picks the most recently created Running pod with every
// container ready, or nil if none qualify. A rolling Deployment update can
// transiently leave an old (terminating) pod matching the same selector
// alongside the new one — a bare "first match" is not reliably the current
// generation's pod, which is what broke the first version of exec-based
// ReadFile against a real rollout (found live against minikube, not just in
// a synthetic test).
func newestReadyPod(pods []corev1.Pod) *corev1.Pod {
	var best *corev1.Pod
	for i := range pods {
		p := &pods[i]
		if p.Status.Phase != corev1.PodRunning || p.DeletionTimestamp != nil {
			continue
		}
		ready := len(p.Status.ContainerStatuses) > 0
		for _, cs := range p.Status.ContainerStatuses {
			if !cs.Ready {
				ready = false
				break
			}
		}
		if !ready {
			continue
		}
		if best == nil || p.CreationTimestamp.After(best.CreationTimestamp.Time) {
			best = p
		}
	}
	return best
}

// execInPod runs command in the named container of a pod via the
// pods/exec subresource, returning captured stdout/stderr.
func (r *Runtime) execInPod(ctx context.Context, ns, podName, containerName string, command []string) (stdout, stderr string, err error) {
	req := r.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(ns).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(r.restConfig, "POST", req.URL())
	if err != nil {
		return "", "", fmt.Errorf("build exec request: %w", err)
	}
	var outBuf, errBuf bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &outBuf, Stderr: &errBuf}); err != nil {
		return outBuf.String(), errBuf.String(), err
	}
	return outBuf.String(), errBuf.String(), nil
}

// RunsContainerCommands marks this adapter as one whose containers actually
// execute their declared Cmd — see the Docker adapter's identical method
// for why the conformance suite checks for it.
func (r *Runtime) RunsContainerCommands() bool { return true }

// Logs returns the target's log tail. When name is a Deployment, this is the
// newest ready pod matching it; when name is instead the literal name of a
// StatefulSet ordinal's own Pod (docs/adr/004-replicas-and-identity.md),
// its logs directly — the aggregate base name of a StableIdentity set is
// deliberately not supported here, matching ReadFile/EnsureReachable.
func (r *Runtime) Logs(ctx context.Context, name string, tail int) (string, error) {
	ns, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return "", err
	}
	if d != nil {
		return r.podLogsFromSelector(ctx, ns, name, tail)
	}
	podNS, pod, _, err := r.findOrdinalPod(ctx, name)
	if err != nil {
		return "", err
	}
	if pod == nil {
		return "", fmt.Errorf("no deployment or replica pod named %q found", name)
	}
	return r.singlePodLogs(ctx, podNS, pod.Name, tail)
}

// tailLogs mirrors the Docker adapter's failure-message helper: best-effort,
// swallows errors, formatted for inclusion in a "did not become healthy"
// error.
func (r *Runtime) tailLogs(ctx context.Context, ns, name string) string {
	out, err := r.podLogsFromSelector(ctx, ns, name, 10)
	if err != nil || out == "" {
		return ""
	}
	if len(out) > 2000 {
		out = out[len(out)-2000:]
	}
	return "; last log lines:\n" + out
}

// podLogsFromSelector returns the newest matching pod's logs — the
// Deployment-path lookup shared by Logs and tailLogs.
func (r *Runtime) podLogsFromSelector(ctx context.Context, ns, selectorName string, tail int) (string, error) {
	pods, err := r.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: "app=" + selectorName,
	})
	if err != nil {
		return "", fmt.Errorf("list pods for %q: %w", selectorName, err)
	}
	if len(pods.Items) == 0 {
		return "", nil
	}
	return r.singlePodLogs(ctx, ns, pods.Items[0].Name, tail)
}

// singlePodLogs fetches one exact pod's log tail.
func (r *Runtime) singlePodLogs(ctx context.Context, ns, podName string, tail int) (string, error) {
	if tail <= 0 {
		tail = 200
	}
	tailInt64 := int64(tail)
	rc, err := r.clientset.CoreV1().Pods(ns).GetLogs(podName, &corev1.PodLogOptions{TailLines: &tailInt64}).Stream(ctx)
	if err != nil {
		return "", fmt.Errorf("read logs for pod %q: %w", podName, err)
	}
	defer rc.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		return "", fmt.Errorf("read logs for pod %q: %w", podName, err)
	}
	return strings.TrimSpace(buf.String()), nil
}

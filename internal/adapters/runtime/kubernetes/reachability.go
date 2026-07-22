// This file holds the reachability seam: ProbeReachable's in-network/
// ephemeral-pod dial strategies, EnsureReachable's node-port/load-balancer/
// port-forward strategies, and the observed-address helpers they share
// (docs/planning/08 §7.6 G3).
package kubernetes

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	"github.com/rezarajan/platformctl/internal/adapters/runtime/probe"
	runtimeport "github.com/rezarajan/platformctl/internal/ports/runtime"
)

// ProbeReachable answers "can a pod in namespace network reach target right
// now" from an in-network vantage point — never from this process
// (docs/planning/08 C10, ADR 015's connectivity plane). network names the
// Namespace (EnsureNetwork's own mapping — see the package doc). Two tiers,
// mirroring the Docker adapter: first, exec a TCP dial inside an existing
// managed, ready pod already in the namespace; if none exists, or the exec
// attempt is inconclusive (that pod's own image happens to lack both nc and
// a /dev/tcp-capable shell), an ephemeral probe pod — the pinned, known-good
// busybox image — gives an authoritative answer instead.
func (r *Runtime) ProbeReachable(ctx context.Context, network, target string) error {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return fmt.Errorf("ProbeReachable: invalid target %q: %w", target, err)
	}
	ns := network

	if pod, ok := r.execProbeCandidate(ctx, ns); ok {
		if derr := r.execTCPDial(ctx, ns, pod, host, port); derr == nil {
			return nil // confirmed reachable by an existing in-network pod
		}
		// Inconclusive or genuinely unreachable — the ephemeral probe pod
		// (below) gives the authoritative answer either way.
	}
	return r.ephemeralProbe(ctx, ns, host, port)
}

// execProbeCandidate finds the newest ready platformctl-managed pod in ns —
// an existing in-network vantage point cheaper than scheduling a dedicated
// probe pod.
func (r *Runtime) execProbeCandidate(ctx context.Context, ns string) (*corev1.Pod, bool) {
	pods, err := r.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: runtimeport.LabelManagedBy + "=" + runtimeport.ManagedByValue,
	})
	if err != nil {
		return nil, false
	}
	pod := newestReadyPod(pods.Items)
	if pod == nil {
		return nil, false
	}
	return pod, true
}

// execTCPDial execs probe.TCPDialScript inside pod's first container and reports
// whether it exited zero (dial succeeded). Any exec-layer failure (the
// pod's image has neither nc nor a /dev/tcp-capable shell, the exec API call
// itself failed, ...) and a genuine connection failure both surface as a
// non-nil error here — ProbeReachable's caller treats both the same way:
// fall back to the ephemeral probe pod for an authoritative answer.
func (r *Runtime) execTCPDial(ctx context.Context, ns string, pod *corev1.Pod, host, port string) error {
	if len(pod.Spec.Containers) == 0 {
		return fmt.Errorf("pod %q declares no containers to exec into", pod.Name)
	}
	containerName := pod.Spec.Containers[0].Name
	_, stderr, err := r.execInPod(ctx, ns, pod.Name, containerName, probe.ExecArgs(host, port))
	if err != nil {
		return fmt.Errorf("exec dial %s in pod %q: %w (stderr: %s)", net.JoinHostPort(host, port), pod.Name, err, strings.TrimSpace(stderr))
	}
	return nil
}

// ephemeralProbe schedules the pinned probe image as a one-shot Pod in ns,
// dials host:port from inside it, and always deletes the Pod before
// returning — its terminal phase is the answer.
func (r *Runtime) ephemeralProbe(ctx context.Context, ns, host, port string) error {
	name := fmt.Sprintf("datascape-probe-%d", time.Now().UnixNano())
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    withOwnership(map[string]string{runtimeport.LabelGeneration: "probe"}),
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:    "probe",
				Image:   probe.Image,
				Command: probe.Command(host, port),
			}},
		},
	}
	created, err := r.clientset.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("ProbeReachable: create probe pod: %w", err)
	}
	defer func() {
		_ = r.clientset.CoreV1().Pods(ns).Delete(context.Background(), created.Name, metav1.DeleteOptions{})
	}()

	const pollTimeout = 30 * time.Second
	deadline := time.Now().Add(pollTimeout)
	for {
		p, err := r.clientset.CoreV1().Pods(ns).Get(ctx, created.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("ProbeReachable: get probe pod: %w", err)
		}
		switch p.Status.Phase {
		case corev1.PodSucceeded:
			return nil
		case corev1.PodFailed:
			return fmt.Errorf("dial %s from in-network probe pod in %q: unreachable", net.JoinHostPort(host, port), ns)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("ProbeReachable: probe pod %q in %q did not complete within %s", created.Name, ns, pollTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// observedHostAddr resolves the real, currently-observed host-reachable
// address for a Service port, per the Service's type — nothing for
// ClusterIP (the port-forward/in-cluster access modes have no standing host
// binding at all; EnsureReachable opens one on demand instead).
func (r *Runtime) observedHostAddr(ctx context.Context, svc *corev1.Service, containerPort int32) (string, int, bool) {
	switch svc.Spec.Type {
	case corev1.ServiceTypeNodePort:
		for _, p := range svc.Spec.Ports {
			if p.Port != containerPort {
				continue
			}
			if p.NodePort == 0 {
				return "", 0, false
			}
			nodeIP, err := r.firstNodeAddr(ctx)
			if err != nil || nodeIP == "" {
				return "", 0, false
			}
			return nodeIP, int(p.NodePort), true
		}
	case corev1.ServiceTypeLoadBalancer:
		if len(svc.Status.LoadBalancer.Ingress) == 0 {
			return "", 0, false
		}
		ing := svc.Status.LoadBalancer.Ingress[0]
		addr := ing.IP
		if addr == "" {
			addr = ing.Hostname
		}
		if addr == "" {
			return "", 0, false
		}
		for _, p := range svc.Spec.Ports {
			if p.Port == containerPort {
				return addr, int(p.Port), true
			}
		}
	}
	return "", 0, false
}

// firstNodeAddr picks an address for reaching a NodePort Service: an
// ExternalIP when the cluster has one (real/cloud clusters), falling back to
// InternalIP (local clusters like minikube/kind, where platformctl itself
// typically runs on the same host/network as the node).
func (r *Runtime) firstNodeAddr(ctx context.Context) (string, error) {
	nodes, err := r.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list nodes: %w", err)
	}
	var internal string
	for _, node := range nodes.Items {
		for _, addr := range node.Status.Addresses {
			if addr.Type == corev1.NodeExternalIP && addr.Address != "" {
				return addr.Address, nil
			}
			if addr.Type == corev1.NodeInternalIP && internal == "" {
				internal = addr.Address
			}
		}
	}
	return internal, nil
}

// EnsureReachable makes a container's port reachable from this process
// (running outside the cluster), per the AccessMode its Deployment was last
// created/updated with (docs/planning/08 B1):
//
//   - in-cluster: refuses — there is no host-reachable address by design.
//   - node-port/load-balancer: resolves the Service's observed address;
//     close is a no-op (the Service itself is the standing tunnel).
//   - port-forward (default): opens an ephemeral client-go port-forward
//     tunnel to the container's current pod; close tears it down.
//
// When name is not a Deployment, it may be the literal name of a
// StatefulSet ordinal's own Pod (docs/adr/004-replicas-and-identity.md):
// only the port-forward access mode is supported for one specific ordinal in
// this iteration — node-port/load-balancer addressing of a single replica
// is a documented "Known limitations" follow-up, not implemented here.
func (r *Runtime) EnsureReachable(ctx context.Context, name string, containerPort int) (string, func() error, error) {
	ns, d, err := findAcrossNamespaces(ctx, r, func(ns string) (*appsv1.Deployment, error) {
		return r.clientset.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	})
	if err != nil {
		return "", nil, err
	}
	if d != nil {
		accessMode := d.Annotations[accessModeAnnotation]
		switch accessMode {
		case runtimeport.AccessInCluster:
			return "", nil, fmt.Errorf("container %q uses access mode %q; no CLI-side (outside-the-cluster) admin connection is possible — run admin operations from a pod inside the cluster instead", name, runtimeport.AccessInCluster)
		case runtimeport.AccessNodePort, runtimeport.AccessLoadBalancer:
			return r.serviceReachableAddr(ctx, ns, name, containerPort, accessMode)
		default:
			return r.portForwardReachableAddrBySelector(ctx, ns, name, containerPort)
		}
	}
	podNS, pod, _, err := r.findOrdinalPod(ctx, name)
	if err != nil {
		return "", nil, err
	}
	if pod == nil {
		return "", nil, fmt.Errorf("deployment %q not found", name)
	}
	return r.portForwardToPod(ctx, podNS, pod, containerPort)
}

// serviceReachableAddr resolves the node-port/load-balancer address and
// polls until it is actually dialable, not merely assigned. Two distinct
// delays separate "the Service object has an address" from "traffic sent to
// it succeeds": a freshly (re)created LoadBalancer's ingress address can
// take a while to provision, and — found live against minikube, not a
// synthetic test — a NodePort number is allocated by the API server
// synchronously at Service-creation time, before kube-proxy has programmed
// the node's iptables/ipvs rule that actually accepts traffic on it, so a
// dial immediately after the number appears can still see connection
// refused for a brief window. EnsureReachable's contract is "a host:port
// this process can dial right now" — resolving the address without proving
// it's live would silently violate that for callers who dial only once.
func (r *Runtime) serviceReachableAddr(ctx context.Context, ns, name string, containerPort int, accessMode string) (string, func() error, error) {
	const pollTimeout = 60 * time.Second
	deadline := time.Now().Add(pollTimeout)
	for {
		svc, err := r.clientset.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return "", nil, fmt.Errorf("get service %q: %w", name, err)
		}
		if ip, port, ok := r.observedHostAddr(ctx, svc, int32(containerPort)); ok {
			addr := net.JoinHostPort(ip, strconv.Itoa(port))
			if probe.Dialable(ctx, addr) {
				return addr, func() error { return nil }, nil
			}
		}
		if time.Now().After(deadline) {
			return "", nil, fmt.Errorf("service %q (access mode %q) did not become dialable for port %d within %s", name, accessMode, containerPort, pollTimeout)
		}
		select {
		case <-ctx.Done():
			return "", nil, ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
}

// portForwardReachableAddrBySelector resolves the newest ready pod matching
// app=selectorName, then opens a tunnel to it — the Deployment-path lookup
// EnsureReachable's default (port-forward) access mode uses.
func (r *Runtime) portForwardReachableAddrBySelector(ctx context.Context, ns, selectorName string, containerPort int) (string, func() error, error) {
	pods, err := r.clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=" + selectorName})
	if err != nil {
		return "", nil, fmt.Errorf("list pods for %q: %w", selectorName, err)
	}
	pod := newestReadyPod(pods.Items)
	if pod == nil {
		return "", nil, fmt.Errorf("no ready pod for %q to port-forward to", selectorName)
	}
	return r.portForwardToPod(ctx, ns, pod, containerPort)
}

// portForwardToPod opens an ephemeral client-go port-forward tunnel to pod
// on an OS-assigned local port, mirroring `kubectl port-forward :containerPort`.
func (r *Runtime) portForwardToPod(ctx context.Context, ns string, pod *corev1.Pod, containerPort int) (string, func() error, error) {
	transport, upgrader, err := spdy.RoundTripperFor(r.restConfig)
	if err != nil {
		return "", nil, fmt.Errorf("build port-forward transport: %w", err)
	}
	req := r.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(ns).
		Name(pod.Name).
		SubResource("portforward")
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", req.URL())

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	var outBuf, errBuf bytes.Buffer
	fw, err := portforward.New(dialer, []string{fmt.Sprintf("0:%d", containerPort)}, stopCh, readyCh, &outBuf, &errBuf)
	if err != nil {
		return "", nil, fmt.Errorf("create port-forwarder to pod %q: %w", pod.Name, err)
	}
	fwErrCh := make(chan error, 1)
	go func() { fwErrCh <- fw.ForwardPorts() }()

	select {
	case <-readyCh:
	case err := <-fwErrCh:
		return "", nil, fmt.Errorf("port-forward to pod %q failed: %w (stderr: %s)", pod.Name, err, strings.TrimSpace(errBuf.String()))
	case <-ctx.Done():
		close(stopCh)
		return "", nil, ctx.Err()
	case <-time.After(15 * time.Second):
		close(stopCh)
		return "", nil, fmt.Errorf("port-forward to pod %q: timed out waiting to become ready", pod.Name)
	}
	ports, err := fw.GetPorts()
	if err != nil || len(ports) == 0 {
		close(stopCh)
		return "", nil, fmt.Errorf("port-forward to pod %q: no local port allocated: %w", pod.Name, err)
	}
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(ports[0].Local)))
	var closeOnce sync.Once
	closeFn := func() error {
		closeOnce.Do(func() { close(stopCh) })
		return nil
	}
	// readyCh only proves the tunnel itself is up (the SPDY stream to the
	// kubelet is established) — not that the container's own process is
	// listening on containerPort yet (the K11 class: a tunnel opened before
	// listen() can look ready forever while carrying no traffic). Per the
	// port contract (docs/planning/08 F3), EnsureReachable must not return
	// an address that isn't currently dialable, so prove it with one direct
	// dial through the tunnel before handing the address back; a caller
	// using runtime.WithReachable (F1) will retry with a fresh tunnel on
	// this error rather than being handed a dead one to discover later.
	if !probe.Dialable(ctx, addr) {
		closeFn()
		return "", nil, fmt.Errorf("port-forward to pod %q: tunnel is up but port %d is not currently accepting connections", pod.Name, containerPort)
	}
	return addr, closeFn, nil
}

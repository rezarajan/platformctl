//go:build integration

package docker

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rezarajan/platformctl/internal/ports/runtime"
)

// TestImagePullAuthPullsFromPrivateRegistry covers docs/planning/08 A1:
// ContainerSpec.ImagePullAuth must let EnsureContainer pull an image from a
// registry that requires authentication — previously ImagePull sent no
// RegistryAuth header at all (docs/planning/07 §1.1 deferral), so only
// daemon-ambient credentials worked.
//
// The registry (registry:2 + htpasswd, per the task's own accept criterion)
// runs on 127.0.0.1, which the Docker daemon trusts as plain HTTP by
// default (docker info's "Insecure Registries" always includes
// 127.0.0.0/8) — no daemon config changes needed. The setup phase (seeding
// the private image) authenticates via `docker login`/`push`, but with
// DOCKER_CONFIG pointed at a throwaway temp directory so it never touches
// the real user's ~/.docker/config.json.
func TestImagePullAuthPullsFromPrivateRegistry(t *testing.T) {
	rt, err := New(nil)
	if err != nil {
		t.Fatalf("connect to Docker: %v", err)
	}
	ctx := context.Background()

	const (
		registryName = "datascape-test-private-registry"
		registryPort = "15051"
		username     = "datascape-test-user"
		password     = "datascape-test-password-123"
		pullName     = "datascape-test-private-pull"
	)
	registryAddr := "127.0.0.1:" + registryPort
	imageTag := registryAddr + "/private/alpine:conformance"

	dockerConfigDir := t.TempDir() // isolates docker login/push from the real credential store
	dockerEnv := append(os.Environ(), "DOCKER_CONFIG="+dockerConfigDir)

	cleanup := func() {
		_ = rt.Remove(ctx, pullName)
		_ = exec.Command("docker", "rm", "-f", registryName).Run()
		_ = exec.Command("docker", "rmi", "-f", imageTag).Run()
	}
	cleanup()
	t.Cleanup(cleanup)

	// 1. Generate a bcrypt htpasswd entry (what registry:2's htpasswd auth
	// backend requires) via httpd's own htpasswd binary — no local install
	// dependency.
	htpasswdDir := t.TempDir()
	out, err := exec.Command("docker", "run", "--rm", "httpd:2.4", "htpasswd", "-Bbn", username, password).Output()
	if err != nil {
		t.Fatalf("generate htpasswd: %v", err)
	}
	if err := os.WriteFile(filepath.Join(htpasswdDir, "htpasswd"), out, 0o644); err != nil {
		t.Fatal(err)
	}

	// 2. Start the private registry.
	if out, err := exec.Command("docker", "run", "-d", "--name", registryName,
		"-p", registryPort+":5000",
		"-v", htpasswdDir+":/auth",
		"-e", "REGISTRY_AUTH=htpasswd",
		"-e", "REGISTRY_AUTH_HTPASSWD_REALM=datascape-test",
		"-e", "REGISTRY_AUTH_HTPASSWD_PATH=/auth/htpasswd",
		"registry:2.8.3").CombinedOutput(); err != nil {
		t.Fatalf("start private registry: %v\n%s", err, out)
	}
	waitForRegistry(t, "http://"+registryAddr+"/v2/", 30*time.Second)

	// 3. Seed the private image: tag a small already-pullable image and
	// push it, authenticating through the isolated DOCKER_CONFIG.
	if out, err := exec.Command("docker", "pull", "alpine:3.20").CombinedOutput(); err != nil {
		t.Fatalf("pull base image: %v\n%s", err, out)
	}
	if out, err := exec.Command("docker", "tag", "alpine:3.20", imageTag).CombinedOutput(); err != nil {
		t.Fatalf("tag image: %v\n%s", err, out)
	}
	login := exec.Command("docker", "login", registryAddr, "-u", username, "--password-stdin")
	login.Env = dockerEnv
	login.Stdin = strings.NewReader(password)
	if out, err := login.CombinedOutput(); err != nil {
		t.Fatalf("docker login (isolated config): %v\n%s", err, out)
	}
	push := exec.Command("docker", "push", imageTag)
	push.Env = dockerEnv
	if out, err := push.CombinedOutput(); err != nil {
		t.Fatalf("push image: %v\n%s", err, out)
	}
	// The daemon may have cached the push's auth; remove the local image
	// copy and any ambient login so only ImagePullAuth can make the pull
	// below succeed.
	if out, err := exec.Command("docker", "rmi", "-f", imageTag).CombinedOutput(); err != nil {
		t.Fatalf("remove local image copy: %v\n%s", err, out)
	}
	logout := exec.Command("docker", "logout", registryAddr)
	logout.Env = dockerEnv
	_ = logout.Run()

	// 4. Without ImagePullAuth, the runtime cannot pull the private image —
	// establishes that what follows is actually exercising the auth path,
	// not coasting on some other ambient credential.
	if _, err := rt.EnsureContainer(ctx, runtime.ContainerSpec{
		Name: pullName, Image: imageTag, PullPolicy: runtime.PullAlways, Cmd: []string{"sleep", "30"},
		Labels: runtime.ManagedLabels("default", "Provider", pullName, pullName),
	}); err == nil {
		t.Fatal("pull without ImagePullAuth unexpectedly succeeded against an authenticated registry")
	}

	// 5. With ImagePullAuth: the pull must succeed.
	spec := runtime.ContainerSpec{
		Name:       pullName,
		Image:      imageTag,
		PullPolicy: runtime.PullAlways,
		Cmd:        []string{"sleep", "30"},
		ImagePullAuth: &runtime.ImagePullAuth{
			Username: username,
			Password: password,
			Registry: registryAddr,
		},
		Labels: runtime.ManagedLabels("default", "Provider", pullName, pullName),
	}
	if _, err := rt.EnsureContainer(ctx, spec); err != nil {
		t.Fatalf("EnsureContainer with ImagePullAuth failed to pull from the private registry: %v", err)
	}

	// 6. Credentials never leak into inspectable container state.
	st, found, err := rt.Inspect(ctx, pullName)
	if err != nil || !found {
		t.Fatalf("Inspect after authenticated pull: found=%v err=%v", found, err)
	}
	for k, v := range st.Env {
		if strings.Contains(v, password) {
			t.Errorf("registry password leaked into container env %s=%s", k, v)
		}
	}
	inspect, err := rt.cli.ContainerInspect(ctx, pullName)
	if err != nil {
		t.Fatalf("raw ContainerInspect: %v", err)
	}
	for _, e := range inspect.Config.Env {
		if strings.Contains(e, password) {
			t.Errorf("registry password leaked into raw docker inspect env: %s", e)
		}
	}
}

func waitForRegistry(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		resp, err := http.Get(url) //nolint:noctx
		if err == nil {
			resp.Body.Close()
			// 401 is expected (auth required) — it proves the registry is
			// actually serving, not just that the port accepted a TCP dial.
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusUnauthorized {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("private registry at %s did not become ready within %s", url, timeout)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

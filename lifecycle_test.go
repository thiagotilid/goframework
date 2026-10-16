package goframework

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

// newTestFramework builds a framework with no config file, listening on a free port.
func newTestFramework(t *testing.T) (*GoFramework, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	t.Chdir(t.TempDir())
	t.Setenv("env", "test")
	port := freePort(t)
	t.Setenv("port", port)
	return NewGoFramework(), "http://127.0.0.1:" + port
}

// startAsync runs Start() in a goroutine and waits until the server answers.
func startAsync(t *testing.T, gf *GoFramework, base string) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- gf.Start() }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(base + "/health/live"); err == nil {
			resp.Body.Close()
			return done
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("server did not come up")
	return nil
}

func sigterm(t *testing.T) {
	t.Helper()
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
}

func waitStart(t *testing.T, done <-chan error, within time.Duration) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(within):
		t.Fatalf("Start() did not return within %s", within)
		return nil
	}
}

func TestStartReturnsNilOnSIGTERM(t *testing.T) {
	gf, base := newTestFramework(t)
	done := startAsync(t, gf, base)

	sigterm(t)

	if err := waitStart(t, done, 5*time.Second); err != nil {
		t.Fatalf("Start() = %v, want nil on a clean shutdown", err)
	}
}

func TestStartReturnsListenError(t *testing.T) {
	gf, _ := newTestFramework(t)
	busy, err := net.Listen("tcp", ":"+os.Getenv("port"))
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()

	if err := gf.Start(); err == nil {
		t.Fatal("Start() = nil, want the listen error")
	}
}

// An in-flight request must finish with 200 after the signal, and the handler must still
// see the tenant of that request: the tenant is a property of the request, not of the
// lifecycle.
func TestInFlightRequestCompletesWithTenantAfterSIGTERM(t *testing.T) {
	gf, base := newTestFramework(t)

	started := make(chan struct{})
	var tenantAfterSignal atomic.Value
	gf.server.GET("/slow", func(c *gin.Context) {
		close(started)
		time.Sleep(600 * time.Millisecond)
		tenantAfterSignal.Store(GetContextHeader(c, XTENANTID, TTENANTID))
		c.String(http.StatusOK, "done")
	})
	done := startAsync(t, gf, base)

	type result struct {
		code int
		body string
		err  error
	}
	res := make(chan result, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodGet, base+"/slow", nil)
		req.Header.Set("Authorization", "Bearer "+tokenWithTenant(t, testTenant))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			res <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		res <- result{code: resp.StatusCode, body: string(b)}
	}()

	<-started
	sigterm(t)

	r := <-res
	if r.err != nil {
		t.Fatalf("in-flight request failed: %v", r.err)
	}
	if r.code != http.StatusOK || r.body != "done" {
		t.Fatalf("response = %d %q, want 200 done", r.code, r.body)
	}
	if got := tenantAfterSignal.Load(); got != testTenant {
		t.Fatalf("tenant seen after the signal = %v, want %q", got, testTenant)
	}
	if err := waitStart(t, done, 5*time.Second); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
}

func TestShutdownTimeoutDoesNotHang(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT_SECONDS", "1")
	gf, base := newTestFramework(t)

	started := make(chan struct{})
	handlerExited := make(chan struct{})
	gf.server.GET("/stuck", func(c *gin.Context) {
		defer close(handlerExited)
		close(started)
		select {
		case <-time.After(10 * time.Second):
		case <-c.Request.Context().Done():
		}
	})
	done := startAsync(t, gf, base)

	clientExited := make(chan struct{})
	go func() {
		defer close(clientExited)
		if resp, err := http.Get(base + "/stuck"); err == nil {
			resp.Body.Close()
		}
	}()
	<-started
	sigterm(t)

	start := time.Now()
	if err := waitStart(t, done, 5*time.Second); err != nil {
		t.Fatalf("Start() = %v, want nil even when the drain times out", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("shutdown took %s, want about the 1s timeout", elapsed)
	}

	// Let the abandoned request unwind before the next test builds another framework.
	<-handlerExited
	<-clientExited
}

func TestReadyTurnsUnhealthyAtSignalWhileLiveAndHealthStay(t *testing.T) {
	t.Setenv("SHUTDOWN_DELAY_SECONDS", "2")
	gf, base := newTestFramework(t)
	done := startAsync(t, gf, base)

	status := func(path string) int {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if got := status("/health/ready"); got != http.StatusOK {
		t.Fatalf("ready before the signal = %d, want 200", got)
	}

	sigterm(t)
	// The delay keeps the listener open, so the probes can still be observed.
	deadline := time.Now().Add(1500 * time.Millisecond)
	for status("/health/ready") != http.StatusServiceUnavailable {
		if time.Now().After(deadline) {
			t.Fatal("ready never turned 503 after the signal")
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got := status("/health/live"); got != http.StatusOK {
		t.Fatalf("live during shutdown = %d, want 200", got)
	}
	if got := status("/health"); got != http.StatusOK {
		t.Fatalf("/health during shutdown = %d, want 200 (legacy behavior)", got)
	}

	if err := waitStart(t, done, 6*time.Second); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
}

func TestLegacyHealthReflectsHealthChecks(t *testing.T) {
	gf, base := newTestFramework(t)
	healthy := atomic.Bool{}
	healthy.Store(true)
	gf.healthCheck = append(gf.healthCheck, func() (string, bool) { return "DEP", healthy.Load() })
	done := startAsync(t, gf, base)
	defer func() {
		sigterm(t)
		waitStart(t, done, 5*time.Second)
	}()

	get := func(path string) int {
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if get("/health") != http.StatusOK || get("/health/ready") != http.StatusOK {
		t.Fatal("healthy dependency must give 200 on /health and /health/ready")
	}

	healthy.Store(false)
	if get("/health") != http.StatusServiceUnavailable || get("/health/ready") != http.StatusServiceUnavailable {
		t.Fatal("unhealthy dependency must give 503 on /health and /health/ready")
	}
	if get("/health/live") != http.StatusOK {
		t.Fatal("/health/live must not depend on external services")
	}
}

func TestLifecycleTrackRefusedAfterBegin(t *testing.T) {
	lc := newLifecycle()
	if !lc.track() {
		t.Fatal("track() = false before begin")
	}
	lc.done()

	lc.begin()

	if lc.track() {
		t.Fatal("track() = true after begin; late workers must not start")
	}
	if !lc.isShuttingDown() {
		t.Fatal("isShuttingDown() = false after begin")
	}
	select {
	case <-lc.Context().Done():
	default:
		t.Fatal("context not cancelled after begin")
	}
}

func TestLifecycleWaitHonorsDeadline(t *testing.T) {
	lc := newLifecycle()
	lc.track()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if lc.wait(ctx) {
		t.Fatal("wait() = true with a worker still running")
	}

	lc.done()
	if !lc.wait(context.Background()) {
		t.Fatal("wait() = false after the worker finished")
	}
}

func TestNilLifecycleKeepsLegacyBehavior(t *testing.T) {
	var lc *lifecycle

	if !lc.track() {
		t.Fatal("nil lifecycle must accept workers")
	}
	lc.done()
	lc.begin()
	if lc.isShuttingDown() {
		t.Fatal("nil lifecycle must never report shutting down")
	}
	select {
	case <-lc.Context().Done():
		t.Fatal("nil lifecycle context must never be cancelled")
	default:
	}
	if !lc.wait(context.Background()) {
		t.Fatal("nil lifecycle wait must return immediately")
	}
}

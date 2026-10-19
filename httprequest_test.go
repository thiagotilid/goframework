package goframework

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
)

// captureTransport replaces http.DefaultTransport: no sockets, every request is recorded.
type captureTransport struct {
	mu       sync.Mutex
	requests []*http.Request
	respond  func(*http.Request) *http.Response
}

func (c *captureTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.mu.Lock()
	c.requests = append(c.requests, r.Clone(r.Context()))
	c.mu.Unlock()
	if c.respond != nil {
		if resp := c.respond(r); resp != nil {
			return resp, nil
		}
	}
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Header: http.Header{}, Request: r}, nil
}

func (c *captureTransport) last(t *testing.T) *http.Request {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.requests) == 0 {
		t.Fatal("no request was sent")
	}
	return c.requests[len(c.requests)-1]
}

func useCapture(t *testing.T) *captureTransport {
	t.Helper()
	ct := &captureTransport{}
	orig := http.DefaultTransport
	http.DefaultTransport = ct
	t.Cleanup(func() {
		http.DefaultTransport = orig
		restPolicyValue.Store(nil)
	})
	restPolicyValue.Store(nil)
	return ct
}

func ginContextWith(headers map[string]string) *gin.Context {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/origin", nil)
	for k, v := range headers {
		c.Request.Header.Set(k, v)
	}
	return c
}

var internalURL = "http://tilid-provider-service.tilid-provider-service/v1/x"

func TestRestRequestForwardsContextToInternalAPIs(t *testing.T) {
	tests := []struct {
		name string
		ctx  func() context.Context
	}{
		{"gin context", func() context.Context {
			return ginContextWith(map[string]string{XTENANTID: testTenant, XAUTHOR: "ana", XAUTHORID: "u1", XCORRELATIONID: "corr-1"})
		}},
		{"consumer context", func() context.Context {
			return consumerCtxWith(
				kafka.Header{Key: XTENANTID, Value: []byte(testTenant)},
				kafka.Header{Key: XAUTHOR, Value: []byte("ana")},
				kafka.Header{Key: XAUTHORID, Value: []byte("u1")},
				kafka.Header{Key: XCORRELATIONID, Value: []byte("corr-1")},
			)
		}},
		{"plain context", func() context.Context {
			ctx := context.WithValue(context.Background(), XTENANTID, testTenant)
			ctx = context.WithValue(ctx, XAUTHOR, "ana")
			ctx = context.WithValue(ctx, XAUTHORID, "u1")
			return context.WithValue(ctx, XCORRELATIONID, "corr-1")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct := useCapture(t)

			resp, err := RestRequest(tt.ctx(), http.MethodGet, internalURL, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()

			r := ct.last(t)
			for k, want := range map[string]string{XTENANTID: testTenant, XAUTHOR: "ana", XAUTHORID: "u1", XCORRELATIONID: "corr-1"} {
				if got := r.Header.Get(k); got != want {
					t.Fatalf("%s = %q, want %q", k, got, want)
				}
			}
		})
	}
}

func TestRestRequestNeverForwardsToExternalAPIs(t *testing.T) {
	for _, url := range []string{
		"https://maps.googleapis.com/maps/api/geocode",
		"https://exp.host/--/api/v2/push/send",
		"https://api.edenai.run/v2/x",
		"https://checklist.conectahub360.com.br/v1/x",
		"https://tilid-foo.com/x",
		"https://8.8.8.8/x",
	} {
		t.Run(url, func(t *testing.T) {
			ct := useCapture(t)
			ctx := ginContextWith(map[string]string{XTENANTID: testTenant, XAUTHOR: "ana", XCORRELATIONID: "corr-1"})

			resp, err := RestRequest(ctx, http.MethodGet, url, nil, map[string]string{"Authorization": "Bearer x"})
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()

			r := ct.last(t)
			for _, k := range forwardedHeaders {
				if got := r.Header.Get(k); got != "" {
					t.Fatalf("%s = %q was sent to an external API", k, got)
				}
			}
			if r.Header.Get("Authorization") != "Bearer x" {
				t.Fatal("headers passed by the caller must still be sent")
			}
		})
	}
}

func TestRestRequestCallerHeadersWin(t *testing.T) {
	ct := useCapture(t)
	ctx := ginContextWith(map[string]string{XTENANTID: testTenant, XAUTHOR: "ana"})

	resp, err := RestRequest(ctx, http.MethodGet, internalURL, nil, map[string]string{XTENANTID: "other-tenant"})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	r := ct.last(t)
	if got := r.Header.Get(XTENANTID); got != "other-tenant" {
		t.Fatalf("X-Tenant-Id = %q, want the caller's value", got)
	}
	if got := r.Header.Values(XTENANTID); len(got) != 1 {
		t.Fatalf("X-Tenant-Id sent %d times, want 1", len(got))
	}
	if got := r.Header.Get(XAUTHOR); got != "ana" {
		t.Fatalf("X-Author = %q, want the forwarded one", got)
	}
}

// Services build the header map with the value of the context, which can be empty.
func TestRestRequestFillsEmptyCallerHeader(t *testing.T) {
	ct := useCapture(t)
	ctx := ginContextWith(map[string]string{XTENANTID: testTenant})

	resp, err := RestRequest(ctx, http.MethodGet, internalURL, nil, map[string]string{XTENANTID: ""})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if got := ct.last(t).Header.Get(XTENANTID); got != testTenant {
		t.Fatalf("X-Tenant-Id = %q, want %q", got, testTenant)
	}
}

func TestRestRequestAbsentValuesAreNotSent(t *testing.T) {
	ct := useCapture(t)

	resp, err := RestRequest(context.Background(), http.MethodGet, internalURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	r := ct.last(t)
	for _, k := range forwardedHeaders {
		if got := r.Header.Get(k); got != "" {
			t.Fatalf("%s = %q, want it absent (never %q)", k, got, "<nil>")
		}
	}
}

// The tenant must never follow a redirect to a host that is not internal.
func TestRestRequestRedirectToExternalDropsForwardedHeaders(t *testing.T) {
	ct := useCapture(t)
	ct.respond = func(r *http.Request) *http.Response {
		if r.URL.Host == "tilid-provider-service.tilid-provider-service" {
			return &http.Response{
				StatusCode: http.StatusFound, Body: http.NoBody, Request: r,
				Header: http.Header{"Location": []string{"https://api.partner.example.com/landing"}},
			}
		}
		return nil
	}
	ctx := ginContextWith(map[string]string{XTENANTID: testTenant, XAUTHOR: "ana"})

	resp, err := RestRequest(ctx, http.MethodGet, internalURL, nil, map[string]string{"X-Explicit": "keep"})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	ct.mu.Lock()
	defer ct.mu.Unlock()
	if len(ct.requests) != 2 {
		t.Fatalf("%d requests, want 2 (original + redirect)", len(ct.requests))
	}
	first, second := ct.requests[0], ct.requests[1]
	if first.Header.Get(XTENANTID) != testTenant {
		t.Fatal("the internal hop must carry the tenant")
	}
	for _, k := range []string{XTENANTID, XAUTHOR} {
		if got := second.Header.Get(k); got != "" {
			t.Fatalf("%s = %q followed a redirect to an external host", k, got)
		}
	}
	if second.Header.Get("X-Explicit") != "keep" {
		t.Fatal("headers set by the caller are not touched")
	}
}

func TestRestRequestRedirectBetweenInternalHostsKeepsHeaders(t *testing.T) {
	ct := useCapture(t)
	ct.respond = func(r *http.Request) *http.Response {
		if r.URL.Host == "tilid-provider-service.tilid-provider-service" {
			return &http.Response{
				StatusCode: http.StatusFound, Body: http.NoBody, Request: r,
				Header: http.Header{"Location": []string{"http://tilid-provider-user.tilid-provider-user/v1/y"}},
			}
		}
		return nil
	}
	ctx := ginContextWith(map[string]string{XTENANTID: testTenant})

	resp, err := RestRequest(ctx, http.MethodGet, internalURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	ct.mu.Lock()
	defer ct.mu.Unlock()
	if len(ct.requests) != 2 || ct.requests[1].Header.Get(XTENANTID) != testTenant {
		t.Fatalf("an internal redirect must keep the tenant (%d requests)", len(ct.requests))
	}
}

func TestIsInternalHost(t *testing.T) {
	p := newRestPolicy(nil)
	internal := []string{
		"foo.apitilid.com.br", "service-dev.apitilid.com.br", "FOO.APITILID.COM.BR",
		"localhost", "127.0.0.1", "::1", "10.100.12.191", "192.168.1.10", "172.20.0.1",
		"x.ns.svc", "x.ns.svc.cluster.local", "x.cluster.local",
		"tilid-provider-service", "tilid-provider-service.tilid-provider-service",
		"tilid-provider-service.tilid-provider-service-v2", "TILID-PROVIDER-FLEET.TILID-PROVIDER-FLEET",
		"tilid-hub-integration.tilid-hub-integration",
	}
	external := []string{
		"", "maps.googleapis.com", "exp.host", "api.edenai.run", "api.maplink.global",
		"graph.facebook.com", "checklist.conectahub360.com.br", "portal.conectahub360.com.br",
		"tilid-foo.com", "tilid-foo.example.com", "evilapitilid.com.br", "apitilid.com.br.evil.com",
		"apitilid.com.br", "8.8.8.8", "172.32.0.1", "auth.auth", "valhalla.valhalla",
	}
	for _, h := range internal {
		if !p.isInternalHost(h) {
			t.Errorf("%q should be internal", h)
		}
	}
	for _, h := range external {
		if p.isInternalHost(h) {
			t.Errorf("%q should be external", h)
		}
	}
}

func TestInternalHostsFromConfiguration(t *testing.T) {
	t.Cleanup(func() { restPolicyValue.Store(nil) })
	v := viper.New()
	v.Set("internal_hosts", "*.conectahub360.com.br, auth.auth;partner-*")

	configureRest(v)
	p := currentRestPolicy()

	for _, h := range []string{"checklist.conectahub360.com.br", "auth.auth", "partner-api", "foo.apitilid.com.br"} {
		if !p.isInternalHost(h) {
			t.Errorf("%q should be internal after INTERNAL_HOSTS", h)
		}
	}
	for _, h := range []string{"conectahub360.com.br", "maps.googleapis.com", "exp.host"} {
		if p.isInternalHost(h) {
			t.Errorf("%q should stay external", h)
		}
	}
}

func TestRestForwardCanBeDisabled(t *testing.T) {
	ct := useCapture(t)
	v := viper.New()
	v.Set("rest_forward_context", false)
	configureRest(v)

	resp, err := RestRequest(ginContextWith(map[string]string{XTENANTID: testTenant}), http.MethodGet, internalURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if got := ct.last(t).Header.Get(XTENANTID); got != "" {
		t.Fatalf("X-Tenant-Id = %q, want none when forwarding is disabled", got)
	}
}

// What the receiving service does with the forwarded header: AddTenant must use it.
func TestForwardedTenantIsWhatTheReceiverSees(t *testing.T) {
	seen := serveWithHeader(t, XTENANTID, testTenant)
	if seen != testTenant {
		t.Fatalf("receiver saw %q, want %q", seen, testTenant)
	}
}

func serveWithHeader(t *testing.T, key, value string) string {
	t.Helper()
	gin.SetMode(gin.TestMode)
	var seen string
	e := gin.New()
	e.Use(AddTenant(viper.New()))
	e.GET("/x", func(c *gin.Context) { seen = GetContextHeader(c, XTENANTID, TTENANTID) })
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set(key, value)
	e.ServeHTTP(httptest.NewRecorder(), req)
	return seen
}

package goframework

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// forwardedHeaders are the headers of the request context that RestRequest copies to calls
// made to internal APIs. They are the same ones the Kafka producer copies to messages, so a
// request keeps its tenant, author and correlation across HTTP and Kafka hops.
var forwardedHeaders = []string{XTENANTID, XAUTHOR, XAUTHORID, XCORRELATIONID, XCREATEDAT}

// RestRequest sends an HTTP request carrying the trace context.
//
// When the target is an internal API (see isInternalHost) the tenant, author and correlation
// headers of ctx are forwarded too, unless the caller already set them in headers. External
// APIs never receive them. ctx can be a *gin.Context, a *ConsumerContext or any context that
// carries the values.
func RestRequest(ctx context.Context, method, url string, body io.Reader, headers ...map[string]string) (*http.Response, error) {
	// Read the context values before unwrapping: a *gin.Context carries them in the request
	// headers, a *ConsumerContext in the message headers.
	forward := contextHeaders(ctx)

	if gc, ok := ctx.(*gin.Context); ok {
		ctx = gc.Request.Context()
	}

	policy := currentRestPolicy()

	var autoAdded []string
	client := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport, otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
			return "HTTP " + r.Method + " " + r.URL.Host + r.URL.Path
		})),
		// Same limit as the default client, plus: a redirect to a host that is not internal
		// must not carry the headers added automatically. Go strips Authorization on
		// cross-domain redirects but not custom headers.
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			if !policy.isInternalHost(req.URL.Hostname()) {
				for _, k := range autoAdded {
					req.Header.Del(k)
				}
			}
			return nil
		},
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}

	for _, h := range headers {
		for k, v := range h {
			req.Header.Add(k, v)
		}
	}

	if policy.forward && policy.isInternalHost(req.URL.Hostname()) {
		for _, k := range forwardedHeaders {
			if v := forward[k]; v != "" && req.Header.Get(k) == "" {
				req.Header.Set(k, v)
				autoAdded = append(autoAdded, k)
			}
		}
	}

	return client.Do(req)
}

// contextHeaders reads the forwardable headers from ctx, ignoring the ones that are absent.
func contextHeaders(ctx context.Context) map[string]string {
	if ctx == nil {
		return nil
	}
	out := make(map[string]string, len(forwardedHeaders))
	for _, k := range forwardedHeaders {
		// GetContextHeader returns "<nil>" for a plain context without the value.
		if v := GetContextHeader(ctx, k); v != "" && v != "<nil>" {
			out[k] = v
		}
	}
	return out
}

// restPolicy decides which hosts are internal APIs.
type restPolicy struct {
	forward  bool
	suffixes []string // ".apitilid.com.br": any host ending with it
	prefixes []string // "tilid-": short Kubernetes names (see isInternalHost)
	exact    []string
}

var restPolicyValue atomic.Pointer[restPolicy]

func currentRestPolicy() *restPolicy {
	if p := restPolicyValue.Load(); p != nil {
		return p
	}
	return newRestPolicy(nil)
}

// newRestPolicy builds the default policy plus the extra patterns, if any.
func newRestPolicy(extra []string) *restPolicy {
	p := &restPolicy{
		forward: true,
		suffixes: []string{
			".apitilid.com.br",
			".svc", ".svc.cluster.local", ".cluster.local",
		},
		prefixes: []string{"tilid-"},
		exact:    []string{"localhost"},
	}
	for _, e := range extra {
		e = strings.ToLower(strings.TrimSpace(e))
		switch {
		case e == "":
		case strings.HasPrefix(e, "*."):
			p.suffixes = append(p.suffixes, e[1:])
		case strings.HasPrefix(e, "."):
			p.suffixes = append(p.suffixes, e)
		case strings.HasSuffix(e, "*"):
			p.prefixes = append(p.prefixes, strings.TrimSuffix(e, "*"))
		default:
			p.exact = append(p.exact, e)
		}
	}
	return p
}

// configureRest reads the policy from the configuration (environment or file):
//
//	INTERNAL_HOSTS        extra internal hosts, separated by comma or space. Each is an exact
//	                      host, "*.domain" / ".domain" (suffix) or "prefix-*" (prefix).
//	REST_FORWARD_CONTEXT  false turns the automatic forwarding off.
func configureRest(v *viper.Viper) {
	extra := strings.FieldsFunc(v.GetString("internal_hosts"), func(r rune) bool {
		return r == ',' || r == ' ' || r == ';'
	})
	p := newRestPolicy(extra)
	if v.IsSet("rest_forward_context") && !v.GetBool("rest_forward_context") {
		p.forward = false
	}
	restPolicyValue.Store(p)
}

// isInternalHost reports whether host (without port) is an internal API, where forwarding
// the tenant is safe. The rule is deliberately closed: anything not matched is external.
//
//   - loopback and private IP addresses (RFC 1918, fc00::/7);
//   - Kubernetes DNS names: "*.svc", "*.svc.cluster.local", "*.cluster.local";
//   - short Kubernetes names of the platform: "tilid-x" or "tilid-x.tilid-x" (every label
//     starts with the prefix, so "tilid-x.com" is not internal);
//   - "*.apitilid.com.br";
//   - whatever INTERNAL_HOSTS adds.
//
// There is no guess based on how many labels a name has: "exp.host" looks like a short
// cluster name and is an external API.
func (p *restPolicy) isInternalHost(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	if host == "" {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || ip.IsPrivate()
	}
	for _, e := range p.exact {
		if host == e {
			return true
		}
	}
	for _, s := range p.suffixes {
		if strings.HasSuffix(host, s) {
			return true
		}
	}
	labels := strings.Split(host, ".")
	for _, pre := range p.prefixes {
		if len(labels) > 2 {
			continue
		}
		all := true
		for _, l := range labels {
			if !strings.HasPrefix(l, pre) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

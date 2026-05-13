package goframework

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/trace"
)

func RestRequest(ctx context.Context, method, url string, body io.Reader, headers ...map[string]string) (*http.Response, error) {
	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()
	fmt.Printf("[RestRequest DEBUG] url=%s valid=%v traceID=%s spanID=%s remote=%v sampled=%v\n",
		url,
		sc.IsValid(),
		sc.TraceID().String(),
		sc.SpanID().String(),
		sc.IsRemote(),
		sc.IsSampled(),
	)

	client := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport, otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
			return "HTTP " + r.Method + " " + r.URL.Host + r.URL.Path
		})),
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

	return client.Do(req)

}

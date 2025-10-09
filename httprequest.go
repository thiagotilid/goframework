package goframework

import (
	"context"
	"io"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func RestRequest(ctx context.Context, method, url string, body io.Reader, headers ...map[string]string) (*http.Response, error) {
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

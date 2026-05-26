package goframework

import (
	"context"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func RestRequest(ctx context.Context, method, url string, body io.Reader, headers ...map[string]string) (*http.Response, error) {
	if gc, ok := ctx.(*gin.Context); ok {
		ctx = gc.Request.Context()
	}

	client := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport, otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
			return "HTTP " + r.Method
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

package goframework

import (
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const httpServerRequestTotalName = "http.server.request.total"

var (
	httpRequestsTotal   metric.Int64Counter
	httpRequestDuration metric.Float64Histogram
)

func initHTTPMetrics() {
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName, _ = os.Hostname()
	}
	meter := otel.Meter(serviceName)

	httpRequestsTotal, _ = meter.Int64Counter(
		httpServerRequestTotalName,
		metric.WithDescription("Total number of HTTP requests"),
	)

	httpRequestDuration, _ = meter.Float64Histogram(
		semconv.HTTPServerRequestDurationName,
		metric.WithDescription(semconv.HTTPServerRequestDurationDescription),
		metric.WithUnit(semconv.HTTPServerRequestDurationUnit),
	)
}

func metricsMiddleware() gin.HandlerFunc {
	initHTTPMetrics()

	return func(ctx *gin.Context) {
		start := time.Now()
		ctx.Next()

		attrs := []attribute.KeyValue{
			semconv.HTTPRequestMethodKey.String(ctx.Request.Method),
			semconv.HTTPRouteKey.String(ctx.FullPath()),
			semconv.HTTPResponseStatusCodeKey.Int(ctx.Writer.Status()),
		}

		attrSet := metric.WithAttributes(attrs...)
		httpRequestsTotal.Add(ctx.Request.Context(), 1, attrSet)
		httpRequestDuration.Record(ctx.Request.Context(), time.Since(start).Seconds(), attrSet)
	}
}

package goframework

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type GoTelemetry struct {
	ProjectName string
	Endpoint    string
	ApiKey      string
}

func NewTelemetry(projectName string, endpoint string, apiKey string) *GoTelemetry {
	return &GoTelemetry{Endpoint: endpoint, ApiKey: apiKey, ProjectName: projectName}
}

func (gt *GoTelemetry) run(gf *GoFramework) {}

func (gt *GoTelemetry) initTracer(ctx context.Context) (shutdown func(context.Context) error, err error) {

	var shutdownFuncs []func(context.Context) error

	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceName(gt.ProjectName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tracerProvider, err := gt.newTraceProvider(ctx, res)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	// meterProvider, err := newMeterProvider(ctx, res)
	// if err != nil {
	// 	handleErr(err)
	// 	return
	// }
	// shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	// otel.SetMeterProvider(meterProvider)

	// loggerProvider, err := newLoggerProvider(ctx)
	// if err != nil {
	// 	handleErr(err)
	// 	return
	// }
	// shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
	// global.SetLoggerProvider(loggerProvider)

	return
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func (gt *GoTelemetry) newTraceProvider(ctx context.Context, res *resource.Resource) (*trace.TracerProvider, error) {

	// conn, err := grpc.NewClient(gt.Endpoint)
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	// }
	// traceExporter, err := otlptracehttp.New(ctx)
	traceExporter, err := otlptracegrpc.New(ctx)
	// traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithHeaders(map[string]string{"api-key": gt.ApiKey}))
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter, trace.WithBatchTimeout(3*time.Second)),
		trace.WithResource(res),
	)
	return traceProvider, nil
}

func newMeterProvider(ctx context.Context, res *resource.Resource) (*metric.MeterProvider, error) {
	metricExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, err
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter, metric.WithInterval(3*time.Second))),
		metric.WithResource(res),
	)
	return meterProvider, nil
}

func newLoggerProvider(ctx context.Context, res *resource.Resource) (*log.LoggerProvider, error) {
	logExporter, err := otlploggrpc.New(ctx)
	if err != nil {
		return nil, err
	}

	loggerProvider := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
		log.WithResource(res),
	)
	return loggerProvider, nil
}

func Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		span := oteltrace.SpanFromContext(c.Request.Context())
		c.Next()

		statuscode := attribute.Key("http.response.status_code")
		span.SetAttributes(statuscode.String(string(c.Writer.Status())))
	}
}

package goframework

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
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

	tracerProvider, err := gt.newTraceProvider(ctx)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	return
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func (gt *GoTelemetry) newTraceProvider(ctx context.Context) (*trace.TracerProvider, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceName(gt.ProjectName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

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
		trace.WithBatcher(traceExporter, trace.WithBatchTimeout(5*time.Second)),
		trace.WithResource(res),
	)
	return traceProvider, nil
}

// func (gt *GoTelemetry) GinTelemetry() gin.HandlerFunc {
// 	return func(ctx *gin.Context) {
// 		shutdown, err := gt.initTracer()
// 		if err != nil {
// 			log.Fatal(err)
// 		}

// 		defer func() {
// 			if err := shutdown(ctx.Request.Context()); err != nil {
// 				log.Fatal("failed to shutdown TracerProvider: %w", err)
// 			}
// 		}()

// 		tracer := otel.Tracer("")
// 		var span trace.Span
// 		c, span := tracer.Start(ctx.Request.Context(), ctx.FullPath())

// 		ctx.Request = ctx.Request.WithContext(c)
// 		ctx.Next()

// 		span.End()
// 	}
// }

// func (ag *agentTelemetry) gin() gin.HandlerFunc {
// 	return otelgin.Middleware(ag.serviceName)
// }

// func (ag *agentTelemetry) mongoMonitor() *event.CommandMonitor {
// 	return otelmongo.NewMonitor()
// }

package goframework

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// setupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func setupOTelSDK(ctx context.Context) (shutdown func(context.Context) error, err error) {
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

	tracerProvider, err := newTraceProvider(ctx)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	meterProvider, err := newMeterProvider(ctx)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	runtime.Start(runtime.WithMeterProvider(meterProvider))

	loggerProvider, err := newLoggerProvider(ctx)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
	global.SetLoggerProvider(loggerProvider)

	return
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

// resolveProtocol returns the OTLP protocol for the given signal,
// honoring per-signal env vars then the global OTEL_EXPORTER_OTLP_PROTOCOL.
// Defaults to "http/protobuf" per the OTel spec.
func resolveProtocol(signalEnv string) string {
	if p := strings.TrimSpace(os.Getenv(signalEnv)); p != "" {
		return p
	}
	if p := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")); p != "" {
		return p
	}
	return "http/protobuf"
}

func newTraceProvider(ctx context.Context) (*trace.TracerProvider, error) {
	var traceExporter trace.SpanExporter
	var err error

	if os.Getenv("OTEL_TRACES_EXPORTER") == "stdout" {
		traceExporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
	} else {
		switch resolveProtocol("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL") {
		case "grpc":
			traceExporter, err = otlptrace.New(ctx, otlptracegrpc.NewClient())
		default:
			traceExporter, err = otlptracehttp.New(ctx)
		}
	}
	if err != nil {
		return nil, err
	}

	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName, _ = os.Hostname()
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, err
	}

	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter),
		trace.WithResource(res),
		trace.WithSampler(resolveSampler()),
	)
	return traceProvider, nil
}

// resolveSampler reads OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG and
// returns the matching sampler. Defaults to ParentBased(AlwaysSample).
// Supported values: always_on, always_off, traceidratio,
// parentbased_always_on, parentbased_always_off, parentbased_traceidratio.
func resolveSampler() trace.Sampler {
	name := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER")))
	arg := strings.TrimSpace(os.Getenv("OTEL_TRACES_SAMPLER_ARG"))

	parseRatio := func() float64 {
		r, err := strconv.ParseFloat(arg, 64)
		if err != nil || r < 0 || r > 1 {
			return 1.0
		}
		return r
	}

	switch name {
	case "always_on":
		return trace.AlwaysSample()
	case "always_off":
		return trace.NeverSample()
	case "traceidratio":
		return trace.TraceIDRatioBased(parseRatio())
	case "parentbased_always_off":
		return trace.ParentBased(trace.NeverSample())
	case "parentbased_traceidratio":
		return trace.ParentBased(trace.TraceIDRatioBased(parseRatio()))
	case "", "parentbased_always_on":
		return trace.ParentBased(trace.AlwaysSample())
	default:
		return trace.ParentBased(trace.AlwaysSample())
	}
}

func newMeterProvider(ctx context.Context) (*metric.MeterProvider, error) {
	var metricExporter metric.Exporter
	var err error

	if os.Getenv("OTEL_TRACES_EXPORTER") == "stdout" {
		metricExporter, err = stdoutmetric.New(stdoutmetric.WithPrettyPrint())
	} else {
		switch resolveProtocol("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL") {
		case "grpc":
			metricExporter, err = otlpmetricgrpc.New(ctx)
		default:
			metricExporter, err = otlpmetrichttp.New(ctx)
		}
	}
	if err != nil {
		return nil, err
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithReader(
			metric.NewPeriodicReader(
				metricExporter,
				metric.WithInterval(3*time.Second),
			),
		),
	)
	return meterProvider, nil
}

func newLoggerProvider(ctx context.Context) (*log.LoggerProvider, error) {
	var logExporter log.Exporter
	var err error

	switch resolveProtocol("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL") {
	case "grpc":
		logExporter, err = otlploggrpc.New(ctx)
	default:
		logExporter, err = otlploghttp.New(ctx)
	}
	if err != nil {
		return nil, err
	}

	loggerProvider := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
	)
	return loggerProvider, nil
}

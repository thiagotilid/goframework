package goframework

import (
	"context"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/event"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/mongo/otelmongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"go.opentelemetry.io/otel/trace"
)

type GoTelemetry struct {
	ProjectName string
	Endpoint    string
	ApiKey      string
}

type agentTelemetry struct {
	tracer      trace.Tracer
	serviceName string
}

type GfSpan struct {
	span trace.Span
}

type GfAgentTelemetry interface {
	gin() gin.HandlerFunc
	mongoMonitor() *event.CommandMonitor
	StartTransaction(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, *GfSpan)
}

func NewTelemetry(projectName string, endpoint string, apiKey string) *GoTelemetry {
	return &GoTelemetry{Endpoint: endpoint, ApiKey: apiKey, ProjectName: projectName}
}

func (gt *GoTelemetry) run(gf *GoFramework) {

	traceConnOpts := []otlptracehttp.Option{
		otlptracehttp.WithEndpoint(gt.Endpoint),
		otlptracehttp.WithHeaders(map[string]string{"api-key": gt.ApiKey}),
	}

	if gt.ApiKey != "" {
		exporter, err := otlptracehttp.New(context.Background(), traceConnOpts...)
		if err != nil {
			log.Fatal(err)
		}

		resources, err := resource.Merge(
			resource.Default(),
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceName(gt.ProjectName),
			),
		)

		otel.SetTracerProvider(
			sdktrace.NewTracerProvider(
				sdktrace.WithResource(resources),
				sdktrace.WithBatcher(exporter),
			),
		)

		if err != nil {
			log.Printf("Could not set resources: %s", err)
		}

		gf.agentTelemetry = &agentTelemetry{
			tracer: otel.GetTracerProvider().Tracer(
				gt.ProjectName,
				trace.WithInstrumentationVersion(os.Getenv("APPVERSION")),
				trace.WithSchemaURL(semconv.SchemaURL)), serviceName: gt.ProjectName}
	}
}

func (ag *agentTelemetry) gin() gin.HandlerFunc {
	return otelgin.Middleware(ag.serviceName)
}

func (ag *agentTelemetry) mongoMonitor() *event.CommandMonitor {
	return otelmongo.NewMonitor()
}

func (ag *agentTelemetry) StartTransaction(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, *GfSpan) {
	ctx, t := ag.tracer.Start(ctx, spanName, opts...)
	return ctx, &GfSpan{span: t}
}

func (g *GfSpan) End() {
	if g.span != nil {
		g.span.End()
	}
}

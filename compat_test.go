package goframework

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
)

// Compile-time guard for the public API the services rely on. If a signature here changes,
// services stop compiling when they bump the version, which the release must not do.
var (
	_ func(...GoFrameworkOptions) *GoFramework                                                       = NewGoFramework
	_ func(*GoFramework) error                                                                       = (*GoFramework).Start
	_ func(*GoFramework, string) string                                                              = (*GoFramework).GetConfig
	_ func(*GoFramework, interface{})                                                                = (*GoFramework).Invoke
	_ func(*GoFramework, interface{})                                                                = (*GoFramework).RegisterController
	_ func(*GoFramework, interface{})                                                                = (*GoFramework).RegisterApplication
	_ func(*GoFramework, interface{})                                                                = (*GoFramework).RegisterRepository
	_ func(*GoFramework, interface{})                                                                = (*GoFramework).RegisterKafkaConsumer
	_ func(*GoFramework, interface{})                                                                = (*GoFramework).RegisterKafkaProducer
	_ func(*GoFramework, interface{})                                                                = (*GoFramework).RegisterProducer
	_ func(*GoFramework, interface{})                                                                = (*GoFramework).RegisterCache
	_ func(*GoFramework)                                                                             = (*GoFramework).RegisterRedis
	_ func(*GoKafka, string, ConsumerFunc)                                                           = (*GoKafka).Consumer
	_ func(*GoKafka, string, ConsumerFunc, ConsumerSettings)                                         = (*GoKafka).ConsumerWithSettings
	_ func(*GoKafka, string, ConsumerFunc, ConsumerMultiRoutineSettings)                             = (*GoKafka).ConsumerMultiRoutine
	_ func(*GoKafka, string, ConsumerFunc, ConsumerMultiRoutineSettings)                             = (*GoKafka).ConsumerWithWorker
	_ func(string, string, string, string, string, string) *GoKafka                                  = NewKafkaConfigMap
	_ func(*GoKafka) Producer                                                                        = NewKafkaProducer
	_ func(*viper.Viper) gin.HandlerFunc                                                             = AddTenant
	_ func(context.Context) context.Context                                                          = ToContext
	_ func(context.Context, string, string)                                                          = AddToContext
	_ func(context.Context, ...string) string                                                        = GetContextHeader
	_ func(context.Context, string, string, io.Reader, ...map[string]string) (*http.Response, error) = RestRequest
)

// Wires a framework the way the services do (cmd/main.go): RegisterKafka, a consumer
// registered through RegisterKafkaConsumer, Start() at the end. A SIGTERM must stop the
// consumer cleanly, deliver the tenant to the handler, and make Start() return nil.
func TestServiceStyleWiringShutsDownCleanly(t *testing.T) {
	src, _, _ := useFakeKafka(t)
	gf, base := newTestFramework(t)

	gf.RegisterKafka("localhost:9092", "test-group", "", "", "", "")

	handled := make(chan string, 1)
	gf.RegisterKafkaConsumer(func(g *GoKafka) {
		g.Consumer("test-topic", func(ctx *ConsumerContext) {
			handled <- GetContextHeader(ctx, XTENANTID, TTENANTID)
		})
	})

	done := startAsync(t, gf, base)

	src.queue <- tenantMessage("m1")
	select {
	case tenant := <-handled:
		if tenant != testTenant {
			t.Fatalf("tenant = %q, want %q", tenant, testTenant)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("message not handled")
	}

	sigterm(t)

	if err := waitStart(t, done, 5*time.Second); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
	select {
	case <-src.closed:
	case <-time.After(time.Second):
		t.Fatal("the consumer did not leave the group before Start() returned")
	}
}

// Start() can be called from a goroutine while the main goroutine keeps working
// (notifiedjob does this), and consumers can be registered after Start() began
// (integrationgate does this).
func TestConsumerRegisteredWhileRunningStillShutsDown(t *testing.T) {
	src, _, _ := useFakeKafka(t)
	gf, base := newTestFramework(t)
	gf.RegisterKafka("localhost:9092", "test-group", "", "", "", "")
	done := startAsync(t, gf, base)

	gf.RegisterKafkaConsumer(func(g *GoKafka) {
		g.Consumer("test-topic", func(*ConsumerContext) {})
	})

	sigterm(t)
	if err := waitStart(t, done, 5*time.Second); err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
	select {
	case <-src.closed:
	case <-time.After(time.Second):
		t.Fatal("late-registered consumer was not closed")
	}
}

func TestFrameworkOnlyRoutesAreNotRegisteredAsServiceRoutes(t *testing.T) {
	for _, path := range []string{"/health/live", "/health/ready"} {
		if !isFrameworkOnlyRoute(path) {
			t.Fatalf("%s must be excluded from the routes collection", path)
		}
	}
	for _, path := range []string{"/health", "/ready", "/v1/service"} {
		if isFrameworkOnlyRoute(path) {
			t.Fatalf("%s must keep being registered", path)
		}
	}
}

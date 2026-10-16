package goframework

// Characterization tests for how the tenant travels through the framework.
//
// The tenant depends on the concrete type of the context (type switch in helper.go):
//   - *gin.Context      -> request header X-Tenant-Id (written by AddTenant from the JWT)
//   - *ConsumerContext  -> Kafka message headers
//   - any other context -> ctx.Value(key)
//
// These tests pin the behavior as it was in v4.7.7. They must keep passing after any change
// to the lifecycle, Start() or the Kafka consumers: if the tenant stops reaching the
// handlers, every service breaks.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/spf13/viper"
)

const (
	testTenant = "d7b8d4da-2da7-458b-8870-3b18fc13e368"
	nilTenant  = "00000000-0000-0000-0000-000000000000"
)

func tokenWithTenant(t *testing.T, tenant string) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		TTENANTID: tenant,
		"name":    "tester",
		"sub":     "user-1",
	})
	signed, err := token.SignedString([]byte("not-validated"))
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func consumerCtxWith(headers ...kafka.Header) *ConsumerContext {
	return &ConsumerContext{
		Context: context.Background(),
		Msg:     &kafka.Message{Headers: headers},
	}
}

type tenantSeen struct {
	direct    string
	viaToCtx  string
	author    string
	authorID  string
	headerRaw string
}

// serveWith runs one request through a gin engine wired like the framework does
// (AddTenant middleware) and reports what the handler sees.
func serveWith(t *testing.T, method string, authorization string) tenantSeen {
	t.Helper()
	gin.SetMode(gin.TestMode)

	var seen tenantSeen
	engine := gin.New()
	engine.Use(AddTenant(viper.New()))
	handler := func(c *gin.Context) {
		seen.direct = GetContextHeader(c, XTENANTID, TTENANTID)
		seen.viaToCtx = GetContextHeader(ToContext(c), XTENANTID)
		seen.author = GetContextHeader(c, XAUTHOR)
		seen.authorID = GetContextHeader(c, XAUTHORID)
		seen.headerRaw = c.Request.Header.Get(XTENANTID)
		c.Status(http.StatusOK)
	}
	engine.Handle(method, "/x", handler)

	req := httptest.NewRequest(method, "/x", nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	return seen
}

func TestTenantHTTPFromJWT(t *testing.T) {
	seen := serveWith(t, http.MethodGet, "Bearer "+tokenWithTenant(t, testTenant))

	if seen.direct != testTenant {
		t.Fatalf("GetContextHeader(*gin.Context) = %q, want %q", seen.direct, testTenant)
	}
	if seen.viaToCtx != testTenant {
		t.Fatalf("GetContextHeader(ToContext(*gin.Context)) = %q, want %q", seen.viaToCtx, testTenant)
	}
}

func TestTenantHTTPWithoutAuthorizationIsNilUUID(t *testing.T) {
	seen := serveWith(t, http.MethodGet, "")

	if seen.direct != nilTenant {
		t.Fatalf("tenant = %q, want nil UUID", seen.direct)
	}
	if seen.viaToCtx != nilTenant {
		t.Fatalf("tenant via ToContext = %q, want nil UUID", seen.viaToCtx)
	}
}

func TestTenantHTTPAuthorOnlyOnWrites(t *testing.T) {
	auth := "Bearer " + tokenWithTenant(t, testTenant)

	post := serveWith(t, http.MethodPost, auth)
	if post.author != "tester" || post.authorID != "user-1" {
		t.Fatalf("POST author = %q/%q, want tester/user-1", post.author, post.authorID)
	}

	get := serveWith(t, http.MethodGet, auth)
	if get.author != "" || get.authorID != "" {
		t.Fatalf("GET author = %q/%q, want empty", get.author, get.authorID)
	}
}

func TestTenantHTTPInvalidTokenIsRejected(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(AddTenant(viper.New()))
	engine.GET("/x", func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req.Header.Set("Authorization", "Bearer not-a-jwt")
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestTenantKafkaFromMessageHeaders(t *testing.T) {
	ctx := consumerCtxWith(kafka.Header{Key: XTENANTID, Value: []byte(testTenant)})

	if got := GetContextHeader(ctx, XTENANTID, TTENANTID); got != testTenant {
		t.Fatalf("GetContextHeader(*ConsumerContext) = %q, want %q", got, testTenant)
	}
	if got := GetContextHeader(ToContext(ctx), XTENANTID); got != testTenant {
		t.Fatalf("GetContextHeader(ToContext(*ConsumerContext)) = %q, want %q", got, testTenant)
	}
}

func TestTenantKafkaWithoutHeaderIsEmpty(t *testing.T) {
	ctx := consumerCtxWith()

	if got := GetContextHeader(ctx, XTENANTID, TTENANTID); got != "" {
		t.Fatalf("tenant = %q, want empty (never another tenant)", got)
	}
}

func TestTenantKafkaEmptyHeaderValueIsIgnored(t *testing.T) {
	ctx := consumerCtxWith(kafka.Header{Key: XTENANTID, Value: nil})

	if got := GetContextHeader(ctx, XTENANTID, TTENANTID); got != "" {
		t.Fatalf("tenant = %q, want empty", got)
	}
}

func TestTenantKafkaAcceptsTTENANTIDHeader(t *testing.T) {
	ctx := consumerCtxWith(kafka.Header{Key: TTENANTID, Value: []byte(testTenant)})

	if got := GetContextHeader(ctx, XTENANTID, TTENANTID); got != testTenant {
		t.Fatalf("tenant = %q, want %q", got, testTenant)
	}
}

func TestTenantAddToContextOnConsumerContext(t *testing.T) {
	ctx := consumerCtxWith()
	AddToContext(ctx, XTENANTID, testTenant)

	if got := GetContextHeader(ctx, XTENANTID); got != testTenant {
		t.Fatalf("tenant = %q, want %q", got, testTenant)
	}
}

func TestTenantGenericContextUsesValue(t *testing.T) {
	ctx := context.WithValue(context.Background(), XTENANTID, testTenant)

	if got := GetContextHeader(ctx, XTENANTID, TTENANTID); got != testTenant {
		t.Fatalf("tenant = %q, want %q", got, testTenant)
	}
}

// Pins a quirk: the default branch returns on the first key, even when the value is absent.
func TestTenantGenericContextWithoutValueReturnsNilString(t *testing.T) {
	if got := GetContextHeader(context.Background(), XTENANTID, TTENANTID); got != "<nil>" {
		t.Fatalf("tenant = %q, want %q (current behavior)", got, "<nil>")
	}
}

func TestTenantToContextCopiesAllKeysFromGin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/x", nil)
	c.Request.Header.Set(XTENANTID, testTenant)
	c.Request.Header.Set(XAUTHOR, "tester")

	out := ToContext(c)

	if got := out.Value(XTENANTID); got != testTenant {
		t.Fatalf("Value(X-Tenant-Id) = %v, want %q", got, testTenant)
	}
	if got := out.Value(XAUTHOR); got != "tester" {
		t.Fatalf("Value(X-Author) = %v, want tester", got)
	}
}

func TestTenantToContextCopiesAllKeysFromConsumer(t *testing.T) {
	ctx := consumerCtxWith(
		kafka.Header{Key: XTENANTID, Value: []byte(testTenant)},
		kafka.Header{Key: XAUTHORID, Value: []byte("user-1")},
	)

	out := ToContext(ctx)

	if got := out.Value(XTENANTID); got != testTenant {
		t.Fatalf("Value(X-Tenant-Id) = %v, want %q", got, testTenant)
	}
	if got := out.Value(XAUTHORID); got != "user-1" {
		t.Fatalf("Value(X-Author-Id) = %v, want user-1", got)
	}
}

func TestTenantProducerHeadersFromEachContextType(t *testing.T) {
	keys := []string{XTENANTID, XAUTHOR, XAUTHORID, XCORRELATIONID, XCREATEDAT}

	t.Run("gin", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/x", nil)
		c.Request.Header.Set(XTENANTID, testTenant)

		if got := helperContextKafka(c, keys).GetString(XTENANTID); got != testTenant {
			t.Fatalf("producer tenant = %q, want %q", got, testTenant)
		}
	})

	t.Run("consumer", func(t *testing.T) {
		ctx := consumerCtxWith(kafka.Header{Key: XTENANTID, Value: []byte(testTenant)})

		if got := helperContextKafka(ctx, keys).GetString(XTENANTID); got != testTenant {
			t.Fatalf("producer tenant = %q, want %q", got, testTenant)
		}
	})

	t.Run("generic", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), XTENANTID, testTenant)

		if got := helperContextKafka(ctx, keys).GetString(XTENANTID); got != testTenant {
			t.Fatalf("producer tenant = %q, want %q", got, testTenant)
		}
	})
}

func TestTenantRepositoryFilters(t *testing.T) {
	repo := &MongoDbRepository[struct{}]{}
	ginCtx := func() *gin.Context {
		gin.SetMode(gin.TestMode)
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodGet, "/x", nil)
		c.Request.Header.Set(XTENANTID, testTenant)
		return c
	}

	t.Run("filter adds tenant or nil tenant", func(t *testing.T) {
		filter := map[string]interface{}{}
		repo.appendTenantToFilter(ginCtx(), filter)

		if _, ok := filter["$or"]; !ok || filter["active"] != true {
			t.Fatalf("filter = %v, want $or and active", filter)
		}
	})

	t.Run("filter works with a consumer context", func(t *testing.T) {
		filter := map[string]interface{}{}
		repo.appendTenantToFilter(consumerCtxWith(kafka.Header{Key: XTENANTID, Value: []byte(testTenant)}), filter)

		if _, ok := filter["$or"]; !ok {
			t.Fatalf("filter = %v, want $or", filter)
		}
	})

	t.Run("filter without nil tenant", func(t *testing.T) {
		filter := map[string]interface{}{}
		repo.appendTenantToFilterWithoutNil(ginCtx(), filter)

		if _, ok := filter["$or"]; !ok {
			t.Fatalf("filter = %v, want $or", filter)
		}
	})

	t.Run("aggregator filter", func(t *testing.T) {
		agg := map[string][]interface{}{}
		repo.appendTenantToFilterAgg(ginCtx(), agg)

		if len(agg["$and"]) != 1 {
			t.Fatalf("agg = %v, want one $and entry", agg)
		}
	})

	t.Run("no tenant leaves the filter untouched", func(t *testing.T) {
		filter := map[string]interface{}{}
		repo.appendTenantToFilter(consumerCtxWith(), filter)

		if len(filter) != 0 {
			t.Fatalf("filter = %v, want empty", filter)
		}
	})
}

package goframework

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/dig"
)

type GoFramework struct {
	ioc           *dig.Container
	configuration *viper.Viper
	server        *gin.Engine
	routeRegister func() error
	healthCheck   []func() (string, bool)
	mainCtx       context.Context
	otelShutdown  func(context.Context) error
	lc            *lifecycle
}

type GoFrameworkOptions interface {
	run(gf *GoFramework)
}

func AddTenant(v *viper.Viper) gin.HandlerFunc {
	return func(ctx *gin.Context) {

		correlation := uuid.New()
		if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
			if id, err := uuid.Parse(ctxCorrelation); err == nil {
				correlation = id
			}
		}
		ctx.Request.Header.Add(XCORRELATIONID, correlation.String())
		trace.SpanFromContext(ctx.Request.Context()).SetAttributes(
			attribute.String("correlation.id", correlation.String()),
		)

		createdat := time.Now().Format(time.RFC3339)
		if ctxCreatedat := GetContextHeader(ctx, XCREATEDAT); ctxCreatedat != "" {
			createdat = ctxCreatedat
		}
		ctx.Request.Header.Add(XCREATEDAT, createdat)

		tokenString := ctx.GetHeader("Authorization")
		if tokenString == "" {
			ctx.Request.Header.Add(XTENANTID, "00000000-0000-0000-0000-000000000000")
			return
		}

		tokenString = strings.Replace(tokenString, "Bearer ", "", 1)
		token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
		if err != nil {
			ctx.AbortWithStatus(http.StatusUnauthorized)
			return
		}

		if claims, ok := token.Claims.(jwt.MapClaims); ok {
			if ctx.Request.Method == http.MethodPost || ctx.Request.Method == http.MethodPut || ctx.Request.Method == http.MethodDelete || ctx.Request.Method == http.MethodPatch {
				ctx.Request.Header.Add(XAUTHOR, fmt.Sprint(claims["name"]))
				ctx.Request.Header.Add(XAUTHORID, fmt.Sprint(claims["sub"]))
			}

			ctx.Request.Header.Add(XTENANTID, fmt.Sprint(claims[TTENANTID]))
		}

		sourcename := v.GetString("kafka.groupid")
		if sourcename == "" {
			sourcename, _ = os.Hostname()
		}

		ctx.Next()
	}
}

func NewGoFramework(opts ...GoFrameworkOptions) *GoFramework {
	location, err := time.LoadLocation("UTC")
	if err != nil {
		panic(err)
	}

	// Skip the write when already UTC: it avoids a needless data race on this global when
	// several frameworks are built in one process (tests).
	if time.Local != location {
		time.Local = location
	}

	gf := &GoFramework{
		ioc:           dig.New(),
		configuration: initializeViper(),
		server:        gin.Default(),
		healthCheck:   make([]func() (string, bool), 0),
		mainCtx:       context.Background(),
		lc:            newLifecycle(),
	}

	cconfig := cors.DefaultConfig()
	cconfig.AllowAllOrigins = true
	cconfig.AllowHeaders = []string{"*", "Authorization"}

	corsconfig := cors.New(cconfig)

	for _, opt := range opts {
		opt.run(gf)
	}

	gf.ioc.Provide(initializeViper)
	gf.ioc.Provide(newLog)
	gf.ioc.Provide(func() *log.Logger { return log.Default() })

	gf.ioc.Invoke(func(v *viper.Viper) {
		gf.server.Use(corsconfig, AddTenant(v))
	})

	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName, _ = os.Hostname()
	}

	if otelEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); len(otelEndpoint) > 0 {
		if shutdown, err := setupOTelSDK(gf.mainCtx); err == nil {
			gf.otelShutdown = shutdown
		}
	}

	gf.server.Use(otelgin.Middleware(serviceName))

	// /health keeps the behavior it always had, including while the pod is shutting down.
	gf.server.GET("/health", func(ctx *gin.Context) {
		list, httpCode := gf.runHealthChecks()
		ctx.JSON(httpCode, list)
	})

	// /health/live never depends on external services, so it can back a liveness probe.
	gf.server.GET(healthLivePath, func(ctx *gin.Context) {
		ctx.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})

	// /health/ready answers 503 as soon as the shutdown begins, taking the pod out of rotation.
	gf.server.GET(healthReadyPath, func(ctx *gin.Context) {
		if gf.lc.isShuttingDown() {
			ctx.JSON(http.StatusServiceUnavailable, map[string]string{"status": "shutting_down"})
			return
		}
		list, httpCode := gf.runHealthChecks()
		ctx.JSON(httpCode, list)
	})

	err = gf.ioc.Provide(func() *gin.RouterGroup { return gf.server.Group("/") })
	if err != nil {
		log.Panic(err)
	}

	return gf
}

// VIPER
func initializeViper() *viper.Viper {
	v := viper.New()
	v.AddConfigPath("./configs")
	v.SetConfigType("json")
	v.SetConfigName(os.Getenv("env"))

	// Environment variables take precedence over the config file.
	// Key "redis.hosts" is read from REDIS_HOSTS, "kafka.groupid" from KAFKA_GROUPID.
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		var notFound viper.ConfigFileNotFoundError
		if !errors.As(err, &notFound) {
			log.Panic(err)
		}
	}
	return v
}

func (gf *GoFramework) GetConfig(key string) string {
	return strings.Join(gf.configuration.GetStringSlice(key), ",")
}

// DIG
func (gf *GoFramework) RegisterRepository(constructor interface{}) {
	err := gf.ioc.Provide(constructor)
	if err != nil {
		log.Panic(err)
	}
}

func (gf *GoFramework) RegisterApplication(application interface{}) {
	err := gf.ioc.Provide(application)
	if err != nil {
		log.Panic(err)
	}
}

// GIN
func (gf *GoFramework) RegisterController(controller interface{}) {
	err := gf.ioc.Invoke(controller)
	if err != nil {
		log.Panic(err)
	}
}

const (
	healthLivePath  = "/health/live"
	healthReadyPath = "/health/ready"
)

// isFrameworkOnlyRoute reports routes the framework adds for the platform itself. They are
// not part of the service API, so they are not registered in the routes collection.
func isFrameworkOnlyRoute(path string) bool {
	return path == healthLivePath || path == healthReadyPath
}

func (gf *GoFramework) runHealthChecks() (map[string]bool, int) {
	list := make(map[string]bool)
	httpCode := http.StatusOK
	for _, item := range gf.healthCheck {
		name, status := item()
		list[name] = status
		if !status {
			httpCode = http.StatusServiceUnavailable
		}
	}
	return list, httpCode
}

// Start serves HTTP until the process receives SIGTERM or SIGINT, then shuts down
// gracefully: readiness turns unhealthy, the Kafka consumers stop reading, and in-flight
// requests and messages are given up to SHUTDOWN_TIMEOUT_SECONDS (default 20) to finish.
// SHUTDOWN_DELAY_SECONDS (default 0) optionally holds the listener open at the start of
// that window, for deployments without a preStop hook. The delay is part of the timeout.
//
// It returns nil on a clean shutdown, and the listen error if the server fails to run.
func (gf *GoFramework) Start() error {
	port := os.Getenv("port")
	if port == "" {
		port = "8081"
	}

	if gf.routeRegister != nil {
		if err := gf.routeRegister(); err != nil {
			panic(err)
		}
	}

	ctx, stop := signal.NotifyContext(gf.mainCtx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	srv := &http.Server{Addr: ":" + port, Handler: gf.server.Handler()}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()

	var err error
	select {
	case err = <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		gf.drain(nil)
	case <-ctx.Done():
		// A second signal now terminates the process immediately.
		stop()
		log.Println("goframework: shutdown signal received, draining")
		gf.drain(srv)
	}

	if gf.otelShutdown != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), otelFlushTimeout)
		defer cancel()
		gf.otelShutdown(shutdownCtx)
	}

	return err
}

// drain stops the consumers and, when srv is not nil, the HTTP server, within the shutdown
// timeout. It never fails: when time runs out it logs and returns.
func (gf *GoFramework) drain(srv *http.Server) {
	timeout := time.Duration(gf.configuration.GetInt("shutdown_timeout_seconds")) * time.Second
	if timeout <= 0 {
		timeout = defaultShutdownTimeout
	}
	delay := time.Duration(gf.configuration.GetInt("shutdown_delay_seconds")) * time.Second

	drainCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	gf.lc.begin()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if !gf.lc.wait(drainCtx) {
			log.Println("goframework: timed out waiting for Kafka consumers to finish")
		}
	}()

	if srv != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-drainCtx.Done():
				}
			}
			if err := srv.Shutdown(drainCtx); err != nil {
				log.Printf("goframework: timed out waiting for HTTP requests to finish: %v", err)
				srv.Close()
			}
		}()
	}

	wg.Wait()
}

func (gf *GoFramework) Invoke(function interface{}) {
	err := gf.ioc.Invoke(function)
	if err != nil {
		log.Panic(err)
	}
}

// mongo
func (gf *GoFramework) RegisterDbMongo(host string, user string, pass string, database string, normalize bool) {

	opts := options.Client().ApplyURI(host)

	if user != "" {
		opts.SetAuth(options.Credential{Username: user, Password: pass})
	}

	err := gf.ioc.Provide(func() *mongo.Database {
		cli, err := newMongoClient(gf.mainCtx, opts, normalize)
		if err != nil {
			return nil
		}
		return cli.Database(database)
	})

	gf.ioc.Provide(NewMongoTransaction)

	gf.routeRegister = func() error {
		if err := gf.ioc.Invoke(gf.RegisterRoutes); err != nil {
			return err
		}
		return nil
	}

	gf.healthCheck = append(gf.healthCheck, func() (string, bool) {
		serviceName := "MDB"
		if err := gf.ioc.Invoke(gf.PingMongoClient); err != nil {
			return serviceName, false
		}
		return serviceName, true
	})

	if err != nil {
		log.Panic(err)
	}
}

// mongo ping for health check
func (gf *GoFramework) PingMongoClient(db *mongo.Database) error {
	return db.Client().Ping(context.Background(), readpref.Nearest())
}

// Redis
func (gf *GoFramework) RegisterRedis() {
	settings := NewRedisSettings(gf.configuration)
	err := gf.ioc.Provide(func() ICache { return NewRedisClient(settings) })
	if err != nil {
		log.Panic(err)
	}
}

func (gf *GoFramework) RegisterRedisWithSettings(addrs []string, password string, db int, client string, cluster bool) {

	settings := &RedisSettings{
		Addr:     addrs,
		Password: password,
		DB:       db,
		Client:   client,
		Ttl:      10 * time.Second,
		Cluster:  cluster,
	}

	err := gf.ioc.Provide(func() ICache { return NewRedisClient(settings) })
	if err != nil {
		log.Panic(err)
	}
}

func (gf *GoFramework) RegisterCache(constructor interface{}) {
	err := gf.ioc.Provide(constructor)
	if err != nil {
		log.Panic(err)
	}
}

// Used to register default producer
func (gf *GoFramework) RegisterProducer(constructor interface{}) {
	err := gf.ioc.Provide(constructor)
	if err != nil {
		log.Panic(err)
	}
}

func (gf *GoFramework) RegisterKafka(server string,
	groupId string,
	securityprotocol string,
	saslmechanism string,
	saslusername string,
	saslpassword string) {
	err := gf.ioc.Provide(func() *GoKafka {
		kc := NewKafkaConfigMap(server, groupId, securityprotocol, saslmechanism, saslusername, saslpassword)
		kc.lc = gf.lc
		return kc
	})
	if err != nil {
		log.Panic(err)
	}
}

// Kafka
func (gf *GoFramework) RegisterKafkaProducer(producer interface{}) {
	err := gf.ioc.Provide(producer)
	if err != nil {
		log.Panic(err)
	}
}

func (gf *GoFramework) RegisterKafkaConsumer(consumer interface{}) {
	err := gf.ioc.Invoke(consumer)
	if err != nil {
		log.Panic(err)
	}
}

// Register Routes On DB
func (gf *GoFramework) RegisterRoutes(db *mongo.Database) {
	mod := os.Getenv("API_HOST")
	if mod != "" {
		coll := db.Client().Database("user").Collection("routes")
		opt := options.InsertOne()
		opt.SetBypassDocumentValidation(true)

		coll.DeleteMany(context.Background(), map[string]interface{}{"module": mod})

		for _, r := range gf.server.Routes() {
			if isFrameworkOnlyRoute(r.Path) {
				continue
			}
			data := NewRoute(r, mod)

			bsonMap, err := MarshalWithRegistry(data)
			if err != nil {
				panic(err)
			}

			var bsonM bson.M
			err = bson.Unmarshal(bsonMap, &bsonM)
			if err != nil {
				panic(err)
			}

			bsonM["active"] = true
			bsonM["tenantId"] = uuid.Nil

			if _, err = coll.InsertOne(context.Background(), bsonM, opt); err != nil {
				panic(err)
			}
		}
	}
}

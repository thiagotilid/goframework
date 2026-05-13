package goframework

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
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

	time.Local = location

	gf := &GoFramework{
		ioc:           dig.New(),
		configuration: initializeViper(),
		server:        gin.Default(),
		healthCheck:   make([]func() (string, bool), 0),
		mainCtx:       context.Background(),
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

	gf.server.GET("/health", func(ctx *gin.Context) {

		list := make(map[string]bool)
		httpCode := http.StatusOK
		for _, item := range gf.healthCheck {
			name, status := item()
			list[name] = status
			if !status {
				httpCode = http.StatusServiceUnavailable
			}
		}
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
	if err := v.ReadInConfig(); err != nil {
		log.Panic(err)
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

	err := gf.server.Run(":" + port)

	if gf.otelShutdown != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		gf.otelShutdown(shutdownCtx)
	}

	return err
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

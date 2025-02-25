package goframework

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/mongo/otelmongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/dig"
)

type GoFramework struct {
	ioc           *dig.Container
	configuration *viper.Viper
	server        *gin.Engine
	traceMonitor  *GoTelemetry
	healthCheck   []func(ctx context.Context) (string, bool)
}

type GoFrameworkOptions interface {
	run(gf *GoFramework)
}

func AddTenant(v *viper.Viper) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		tracer := otel.Tracer("")
		_, span := tracer.Start(ctx.Request.Context(), "AddTenant")
		// ctx.Request = ctx.Request.WithContext(c)

		correlation := uuid.New()
		if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
			if id, err := uuid.Parse(ctxCorrelation); err == nil {
				correlation = id
			}
		}
		ctx.Request.Header.Add(XCORRELATIONID, correlation.String())

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
			span.SetAttributes(attribute.String("tenant", fmt.Sprint(claims[TTENANTID])))
			span.SetAttributes(attribute.String("author.Id", fmt.Sprint(claims["sub"])))
			span.SetAttributes(attribute.String("author.name", fmt.Sprint(claims["name"])))
		}

		span.End()
		ctx.Next()

	}
}

func NewGoFramework(opts ...GoFrameworkOptions) *GoFramework {
	location, err := time.LoadLocation("UTC")
	ctx := context.Background()

	if err != nil {
		panic(err)
	}

	time.Local = location

	gf := &GoFramework{
		ioc:           dig.New(),
		configuration: initializeViper(),
		server:        gin.Default(),
		healthCheck:   make([]func(context.Context) (string, bool), 0),
		traceMonitor:  nil,
	}

	cconfig := cors.DefaultConfig()
	cconfig.AllowAllOrigins = true
	cconfig.AllowHeaders = []string{"*", "Authorization"}

	corsconfig := cors.New(cconfig)

	if len(opts) > 0 {
		gf.traceMonitor = opts[0].(*GoTelemetry)
		gf.traceMonitor.initTracer(ctx)
		gf.server.Use(otelgin.Middleware(gf.traceMonitor.ProjectName))
		gf.server.Use(Middleware())
	}

	gf.ioc.Provide(initializeViper)
	gf.ioc.Provide(newLog)
	// gf.ioc.Provide(func() GfAgentTelemetry { return gf.agentTelemetry })

	gf.server.GET("/health", func(ctx *gin.Context) {

		list := make(map[string]bool)
		httpCode := http.StatusOK
		for _, item := range gf.healthCheck {
			name, status := item(ctx.Request.Context())
			list[name] = status
			if !status {
				httpCode = http.StatusServiceUnavailable
			}
		}
		ctx.JSON(httpCode, list)
	})

	gf.ioc.Invoke(func(v *viper.Viper) {
		gf.server.Use(corsconfig, AddTenant(v))
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
	return gf.configuration.GetString(key)
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
	return gf.server.Run(":" + port)
}

func (gf *GoFramework) Invoke(function interface{}) {
	err := gf.ioc.Invoke(function)
	if err != nil {
		log.Panic(err)
	}
}

// mongo
func (gf *GoFramework) RegisterDbMongo(host string, user string, pass string, database string, normalize bool) {

	err := gf.ioc.Provide(func() *mongo.Database {
		opts := options.Client().ApplyURI(host)

		if user != "" {
			opts.SetAuth(options.Credential{Username: user, Password: pass})
		}

		if gf.traceMonitor != nil {
			opts = opts.SetMonitor(otelmongo.NewMonitor())
		}

		cli, err := newMongoClient(opts, normalize)
		if err != nil {
			return nil
		}
		return cli.Database(database)
	})

	gf.ioc.Provide(NewMongoTransaction)

	gf.healthCheck = append(gf.healthCheck, func(ctx context.Context) (string, bool) {
		var cli *mongo.Client
		gf.ioc.Invoke(func(db *mongo.Database) {
			cli = db.Client()
		})

		serviceName := "MDB"

		if err := cli.Ping(ctx, readpref.Nearest()); err != nil {
			return serviceName, false
		}
		return serviceName, true
	})

	if err != nil {
		log.Panic(err)
	}
}

// Redis
func (gf *GoFramework) RegisterRedis(address string, password string, db string) {

	dbInt, err := strconv.Atoi(db)
	if err != nil {
		log.Panic(err)
	}

	opts := &redis.Options{
		Addr:     address,
		Password: password,
		DB:       dbInt,
	}

	if opts.Addr != "" && opts.Addr != "localhost:6379" {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	gf.healthCheck = append(gf.healthCheck, func(ctx context.Context) (string, bool) {
		serviceName := "RDS"
		cli := newRedisClient(opts)
		if cli == nil {
			return serviceName, false
		}

		if _, err := cli.Ping(context.Background()).Result(); err != nil {
			return serviceName, false
		}
		return serviceName, true
	})

	err = gf.ioc.Provide(func() *redis.Client { return (newRedisClient(opts)) })
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

func (gf *GoFramework) RegisterKafka(server string,
	groupId string,
	securityprotocol string,
	saslmechanism string,
	saslusername string,
	saslpassword string) {
	err := gf.ioc.Provide(func() *GoKafka {
		return NewKafkaConfigMap(server, groupId, securityprotocol, saslmechanism, saslusername, saslpassword, gf.traceMonitor != nil)
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

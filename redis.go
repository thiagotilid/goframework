package goframework

import (
	"context"
	"crypto/tls"
	"encoding/json"

	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
)

type (
	RedisSettings struct {
		Addr     []string
		Password string
		DB       int
		Client   string
		Ttl      time.Duration
		Cluster  bool
	}
	Redis struct {
		settings *RedisSettings
		client   redis.UniversalClient
	}
)

func NewRedisSettings(v *viper.Viper) *RedisSettings {
	settings := &RedisSettings{}
	if v == nil {
		return settings
	}

	settings.Addr = v.GetStringSlice("redis.hosts")
	if len(v.GetString("redis.pass")) > 0 {
		settings.Password = v.GetString("redis.pass")
	}

	if v.IsSet("redis.db") {
		settings.DB = v.GetInt("redis.db")
	}

	if len(v.GetString("redis.client")) > 0 {
		settings.Client = v.GetString("redis.client")
	} else {
		settings.Client = "goframework"
	}

	if v.IsSet("redis.ttlSecs") {
		settings.Ttl = time.Duration(v.GetInt("redis.ttlSecs")) * time.Second
	} else {
		settings.Ttl = 10 * time.Second
	}

	if v.IsSet("redis.cluster") {
		settings.Cluster = v.GetBool("redis.cluster")
	} else {
		settings.Cluster = false
	}

	return settings
}

func NewRedisClient(rs *RedisSettings) ICache {
	var rdb redis.UniversalClient
	if rs.Cluster {
		rdb = NewClusterClient(rs)
	} else {
		rdb = NewDefaultClient(rs)
	}
	redisotel.InstrumentTracing(rdb)
	redisotel.InstrumentMetrics(rdb)
	return &Redis{
		settings: rs,
		client:   rdb,
	}
}

func NewClusterClient(rs *RedisSettings) *redis.ClusterClient {
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    rs.Addr,
		Password: rs.Password,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		ClientName:      rs.Client,
		ConnMaxIdleTime: 10 * time.Second,
		NewClient: func(opt *redis.Options) *redis.Client {
			return redis.NewClient(opt)
		},
		MaxRetries: 3,
	})
}

func NewDefaultClient(rs *RedisSettings) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:            rs.Addr[0],
		Password:        rs.Password,
		DB:              rs.DB,
		ConnMaxIdleTime: 30,
	})
}

func (rc *Redis) Set(ctx context.Context, key string, val interface{}, ttl time.Duration) error {

	mCtx := getContext(ctx)
	b, err := json.Marshal(val)
	if err != nil {
		return err
	}

	cmd := rc.client.Set(mCtx, key, b, ttl)

	if cmd.Err() != nil {
		return cmd.Err()
	}

	return nil
}

func (rc *Redis) Get(ctx context.Context, key string, pointer interface{}) error {
	mCtx := getContext(ctx)
	re, err := rc.client.Get(mCtx, key).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(re), pointer)
}

func (rc *Redis) Del(ctx context.Context, key string) error {
	mCtx := getContext(ctx)
	_, err := rc.client.Del(mCtx, key).Result()

	if err == redis.Nil {
		return nil
	}

	return err
}

func (rc *Redis) Keys(ctx context.Context, pattern string) ([]string, error) {
	mCtx := getContext(ctx)
	r, err := rc.client.Keys(mCtx, pattern).Result()

	if err == redis.Nil {
		return []string{}, nil
	}

	return r, nil
}

func (r *Redis) Publish(ctx context.Context, key string, obj any) error {
	mCtx := getContext(ctx)
	if err := r.client.Publish(mCtx, key, obj).Err(); err != nil {
		return err
	}
	return nil
}

func (r *Redis) Ping(ctx context.Context) error {
	mCtx := getContext(ctx)
	return r.client.Ping(mCtx).Err()
}

package goframework

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	client *redis.Client
}

func NewCache(client *redis.Client) ICache {
	return &RedisCache{
		client: client,
	}
}

func (rc *RedisCache) Get(ctx context.Context, key string, pointer interface{}) error {

	result := rc.client.Ping(ctx)
	log.Println(result.Result())

	re, err := rc.client.Get(ctx, key).Result()

	if err == redis.Nil {
		return nil
	}

	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(re), pointer)
}

func (rc *RedisCache) Set(ctx context.Context, key string, val interface{}, ttl time.Duration) error {

	b, err := json.Marshal(val)
	if err != nil {
		return err
	}

	result := rc.client.Ping(ctx)
	log.Println(result.Result())
	return rc.client.Set(ctx, key, b, ttl).Err()
}

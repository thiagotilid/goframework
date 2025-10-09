package goframework

import (
	"context"
	"time"
)

type (
	ICache interface {
		Set(ctx context.Context, key string, val interface{}, ttl time.Duration) error
		Get(ctx context.Context, key string, pointer interface{}) error
		Del(ctx context.Context, key string) error
		Keys(ctx context.Context, pattern string) ([]string, error)
		Publish(ctx context.Context, key string, obj any) error
		Ping(ctx context.Context) error
	}
)

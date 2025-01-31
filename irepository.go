package goframework

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type IRepository[T interface{}] interface {
	ChangeCollection(collectionName string)
	GetAll(ctx context.Context,
		filter map[string]interface{},
		optsFind ...*options.FindOptions) *[]T
	GetAllSkipTake(
		ctx context.Context,
		filter map[string]interface{},
		skip int64,
		take int64,
		optsFind ...*options.FindOptions) *DataList[T]
	GetFirst(ctx context.Context,
		filter map[string]interface{}) *T
	Insert(ctx context.Context,
		entity *T) error
	InsertAll(ctx context.Context,
		entities *[]T) error
	Replace(ctx context.Context,
		filter map[string]interface{},
		entity *T) error
	Update(ctx context.Context,
		filter map[string]interface{},
		fields interface{}) error
	Delete(ctx context.Context,
		filter map[string]interface{}) error
	DeleteMany(ctx context.Context,
		filter map[string]interface{}) error
	DeleteForce(ctx context.Context,
		filter map[string]interface{}) error
	DeleteManyForce(ctx context.Context,
		filter map[string]interface{}) error
	Aggregate(ctx context.Context,
		pipeline []interface{}) (*mongo.Cursor, error)
	DefaultAggregate(ctx context.Context,
		filter bson.A) (*mongo.Cursor, error)
	Count(ctx context.Context,
		filter map[string]interface{},
		optsFind ...*options.CountOptions) int64
	GetLock(ctx context.Context,
		id interface{}) (*T, error)
	Unlock(ctx context.Context,
		id interface{}) error
	UpdateMany(
		ctx context.Context,
		filter map[string]interface{},
		fields interface{}) error
	PushMany(
		ctx context.Context,
		filter map[string]interface{},
		fields interface{}) error
	PullMany(
		ctx context.Context,
		filter map[string]interface{},
		fields interface{}) error
	SetReaders(
		ctx context.Context,
		filter map[string]interface{}) error
	SetExpiredAfterInsert(ctx context.Context, seconds int32) error
	FindOneAndUpdate(
		ctx context.Context,
		filter map[string]interface{},
		fields map[string]interface{}) (*T, error)
}

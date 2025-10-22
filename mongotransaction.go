package goframework

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
)

type MongoTransaction struct {
	db *mongo.Database
}

func NewMongoTransaction(
	db *mongo.Database,
) *MongoTransaction {
	return &MongoTransaction{
		db: db,
	}
}

func (vuc *MongoTransaction) RunInTransaction(ctx context.Context, action func(mctx mongo.SessionContext) (interface{}, error)) (interface{}, error) {
	session, err := vuc.db.Client().StartSession()
	if err != nil {
		return nil, err
	}
	defer session.EndSession(ctx)

	return session.WithTransaction(ToContext(ctx), action)
}

package goframework

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LogData struct {
	Author   string
	AuthorId uuid.UUID
	ActionAt time.Time
}

type DataList[T interface{}] struct {
	Data  []T
	Total int64
}

type MongoDbRepository[T interface{}] struct {
	db         *mongo.Database
	collection *mongo.Collection
	dataList   *DataList[T]
	monitoring *Monitoring
	sourceName string
}

func NewMongoDbRepository[T interface{}](
	db *mongo.Database,
	monitoring *Monitoring,
	v *viper.Viper,
) IRepository[T] {
	var r T
	reg := regexp.MustCompile(`\[.*`)
	coll := db.Collection(reg.ReplaceAllString(strings.ToLower(reflect.TypeOf(r).Name()), ""))

	sourcename := v.GetString("kafka.groupid")
	if sourcename == "" {
		sourcename, _ = os.Hostname()
	}

	return &MongoDbRepository[T]{
		db:         db,
		collection: coll,
		dataList:   &DataList[T]{},
		monitoring: monitoring,
		sourceName: sourcename,
	}
}

func (r *MongoDbRepository[T]) ChangeCollection(collectionName string) {
	r.collection = r.db.Collection(collectionName)
}

func appendTenantToFilterAgg(ctx context.Context, filterAggregator map[string][]interface{}) {
	if tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID); tenantId != "" {
		if tid, err := uuid.Parse(tenantId); err == nil {
			filterAggregator["$and"] = append(filterAggregator["$and"], map[string]interface{}{"$or": bson.A{
				bson.D{{"tenantId", tid}},
				bson.D{{"tenantId", uuid.Nil}},
			},
			})
		}
	}
}

func (r *MongoDbRepository[T]) GetAll(
	ctx context.Context,
	filter map[string]interface{},
	optsFind ...*options.FindOptions) *[]T {

	filterAggregator := make(map[string][]interface{})
	filterAggregator["$and"] = append(filterAggregator["$and"], filter, bson.D{{"active", true}})

	appendTenantToFilterAgg(ctx, filterAggregator)
	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filterAggregator)
		fmt.Print(bson.Raw(obj), err)
	}

	cur, err := r.collection.Find(getContext(ctx), filterAggregator, optsFind...)
	if err != nil {
		panic(err)
	}
	result := []T{}
	for cur.Next(ctx) {
		var el T
		err = cur.Decode(&el)
		if err != nil {
			panic(err)
		}
		result = append(result, el)
	}

	return &result
}

func (r *MongoDbRepository[T]) GetAllSkipTake(
	ctx context.Context,
	filter map[string]interface{},
	skip int64,
	take int64,
	optsFind ...*options.FindOptions) *DataList[T] {

	result := &DataList[T]{}

	filterAggregator := make(map[string][]interface{})
	filterAggregator["$and"] = append(filterAggregator["$and"], filter, bson.D{{"active", true}})
	appendTenantToFilterAgg(ctx, filterAggregator)

	opts := make([]*options.FindOptions, 0)

	op := options.Find()
	op.SetSkip(skip)
	op.SetLimit(take)

	opts = append(opts, op)
	opts = append(opts, optsFind...)

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filterAggregator)
		fmt.Print(bson.Raw(obj), err)
	}

	mCtx := getContext(ctx)

	result.Total, _ = r.collection.CountDocuments(mCtx, filterAggregator)
	if result.Total > 0 {

		cur, err := r.collection.Find(mCtx, filterAggregator, opts...)

		if err != nil {
			panic(err)
		}
		for cur.Next(ctx) {
			var el T
			err = cur.Decode(&el)
			if err != nil {
				panic(err)
			}
			result.Data = append(result.Data, el)
		}
	}

	return result
}

func appendTenantToFilter(ctx context.Context, filter map[string]interface{}) {
	if tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID); tenantId != "" {
		if tid, err := uuid.Parse(tenantId); err == nil {
			filter["$or"] = bson.A{
				bson.D{{"tenantId", tid}},
				bson.D{{"tenantId", uuid.Nil}},
			}
			filter["active"] = true
		}
	}
}

func (r *MongoDbRepository[T]) GetFirst(
	ctx context.Context,
	filter map[string]interface{}) *T {
	var el T

	appendTenantToFilter(ctx, filter)

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Print(bson.Raw(obj), err)
	}

	err := r.collection.FindOne(getContext(ctx), filter).Decode(&el)

	if err == mongo.ErrNoDocuments {
		return nil
	}

	if err != nil {
		panic(err)
	}

	return &el
}

func (r *MongoDbRepository[T]) insertDefaultParam(ctx context.Context, entity *T) (bson.M, error) {
	bsonMap, err := bson.MarshalWithRegistry(MongoRegistry, entity)
	if err != nil {
		return nil, err
	}

	var bsonM bson.M
	err = bson.Unmarshal(bsonMap, &bsonM)
	if err != nil {
		return nil, err
	}
	// helperContext(ctx, bsonM, map[string]string{"createdBy": XAUTHOR, "updatedBy": XAUTHOR})
	if tenantid := GetContextHeader(ctx, XTENANTID, TTENANTID); tenantid != "" {
		if tid, err := uuid.Parse(tenantid); err == nil {
			bsonM["tenantId"] = tid
		}
	}

	var history = make(map[string]interface{})
	history["ActionAt"] = time.Now()
	helperContext(ctx, history, map[string]string{"author": XAUTHOR, "authorId": XAUTHORID})

	bsonM["created"] = history
	bsonM["updated"] = history
	bsonM["active"] = true

	return bsonM, nil
}

func (r *MongoDbRepository[T]) replaceDefaultParam(ctx context.Context, old bson.M, entity *T) (bson.M, error) {
	bsonMap, err := bson.MarshalWithRegistry(MongoRegistry, entity)
	if err != nil {
		return nil, err
	}

	var bsonM bson.M
	err = bson.Unmarshal(bsonMap, &bsonM)
	if err != nil {
		return nil, err
	}

	var history = make(map[string]interface{})
	history["ActionAt"] = time.Now()
	helperContext(ctx, history, map[string]string{"author": XAUTHOR, "authorId": XAUTHORID})

	bsonM["tenantId"] = old["tenantId"]
	bsonM["created"] = old["created"]
	bsonM["updated"] = history
	bsonM["active"] = old["active"]

	return bsonM, nil
}

func (r *MongoDbRepository[T]) Insert(
	ctx context.Context,
	entity *T) error {

	correlation := uuid.New()
	if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
		if id, err := uuid.Parse(ctxCorrelation); err == nil {
			correlation = id
		}
	}
	mt := r.monitoring.Start(correlation, r.sourceName, TracingTypeRepository)
	mt.AddContent(entity)
	mt.AddStack(100, "REPLACE")
	mt.End()

	opt := options.InsertOne()
	opt.SetBypassDocumentValidation(true)

	bsonM, err := r.insertDefaultParam(ctx, entity)
	if err != nil {
		return err
	}

	_, err = r.collection.InsertOne(ctx, bsonM, opt)
	if err != nil {
		return err
	}

	return nil
}

func (r *MongoDbRepository[T]) InsertAll(
	ctx context.Context,
	entities *[]T) error {

	correlation := uuid.New()
	if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
		if id, err := uuid.Parse(ctxCorrelation); err == nil {
			correlation = id
		}
	}

	mt := r.monitoring.Start(correlation, r.sourceName, TracingTypeRepository)
	mt.AddContent(entities)
	mt.AddStack(100, "REPLACE")
	mt.End()

	var uis []interface{}
	for _, ui := range *entities {
		bsonM, err := r.insertDefaultParam(ctx, &ui)
		if err != nil {
			return err
		}

		uis = append(uis, bsonM)
	}
	_, err := r.collection.InsertMany(getContext(ctx), uis)
	if err != nil {
		return err
	}

	return nil
}

func (r *MongoDbRepository[T]) Replace(
	ctx context.Context,
	filter map[string]interface{},
	entity *T) error {

	correlation := uuid.New()
	if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
		if id, err := uuid.Parse(ctxCorrelation); err == nil {
			correlation = id
		}
	}

	mt := r.monitoring.Start(correlation, r.sourceName, TracingTypeRepository)
	mt.AddContent(entity)
	mt.AddStack(100, "REPLACE")
	mt.End()

	if tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID); tenantId != "" {
		if tid, err := uuid.Parse(tenantId); err == nil {
			filter["tenantId"] = tid
		}
	}

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Print(bson.Raw(obj), err)
	}

	var el bson.M
	err := r.collection.FindOne(getContext(ctx), filter).Decode(&el)

	if err == mongo.ErrNoDocuments {
		return r.Insert(ctx, entity)
	}

	bsonM, err := r.replaceDefaultParam(ctx, el, entity)
	if err != nil {
		return err
	}

	_, err = r.collection.ReplaceOne(getContext(ctx), filter, bsonM, options.Replace().SetUpsert(true))
	if err != nil {
		return err
	}

	return nil
}

func (r *MongoDbRepository[T]) Update(
	ctx context.Context,
	filter map[string]interface{},
	fields interface{}) error {

	correlation := uuid.New()
	if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
		if id, err := uuid.Parse(ctxCorrelation); err == nil {
			correlation = id
		}
	}

	mt := r.monitoring.Start(correlation, r.sourceName, TracingTypeRepository)
	mt.AddContent(fields)
	mt.AddStack(100, "UPDATE")
	mt.End()

	if tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID); tenantId != "" {
		if tid, err := uuid.Parse(tenantId); err == nil {
			filter["tenantId"] = tid
		}
	}
	var setBson bson.M
	_, obj, err := bson.MarshalValueWithRegistry(MongoRegistry, fields)
	if err != nil {
		return err
	}
	bson.Unmarshal(obj, &setBson)

	// setBson := structToBson(fields)
	var history = make(map[string]interface{})
	history["ActionAt"] = time.Now()
	helperContext(ctx, history, map[string]string{"author": XAUTHOR, "authorId": XAUTHORID})
	setBson["updated"] = history
	delete(setBson, "_id")

	if os.Getenv("env") == "local" {
		// _, obj, err := bson.MarshalValue(filter)
		fmt.Print(bson.Raw(obj))
	}

	re, err := r.collection.UpdateOne(getContext(ctx), filter, map[string]interface{}{"$set": setBson})

	if err != nil {
		return err
	}

	if re.MatchedCount == 0 {
		return fmt.Errorf("MatchedCountZero")
	}

	return nil
}

func (r *MongoDbRepository[T]) FindOneAndUpdate(
	ctx context.Context,
	filter map[string]interface{},
	fields map[string]interface{}) (*T, error) {

	correlation := uuid.New()
	if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
		if id, err := uuid.Parse(ctxCorrelation); err == nil {
			correlation = id
		}
	}

	mt := r.monitoring.Start(correlation, r.sourceName, TracingTypeRepository)
	mt.AddContent(fields)
	mt.AddStack(100, "UPDATE")
	mt.End()

	if tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID); tenantId != "" {
		if tid, err := uuid.Parse(tenantId); err == nil {
			filter["tenantId"] = tid
		}
	}
	var setBson bson.M
	_, obj, err := bson.MarshalValueWithRegistry(MongoRegistry, fields)
	if err != nil {
		return nil, err
	}
	bson.Unmarshal(obj, &setBson)

	// setBson := structToBson(fields)
	var history = make(map[string]interface{})
	history["ActionAt"] = time.Now()
	helperContext(ctx, history, map[string]string{"author": XAUTHOR, "authorId": XAUTHORID})

	if os.Getenv("env") == "local" {
		// _, obj, err := bson.MarshalValue(filter)
		fmt.Print(bson.Raw(obj))
	}

	re := r.collection.FindOneAndUpdate(getContext(ctx),
		filter, setBson, options.FindOneAndUpdate().SetReturnDocument(options.After))

	switch re.Err() {
	case nil:
		var e T
		if err := re.Decode(&e); err != nil {
			return nil, err
		}
		return &e, nil
	case mongo.ErrNoDocuments:
		return nil, nil
	default:
		return nil, re.Err()
	}
}

func (r *MongoDbRepository[T]) UpdateMany(
	ctx context.Context,
	filter map[string]interface{},
	fields interface{}) error {

	correlation := uuid.New()
	if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
		if id, err := uuid.Parse(ctxCorrelation); err == nil {
			correlation = id
		}
	}

	mt := r.monitoring.Start(correlation, r.sourceName, TracingTypeRepository)
	mt.AddContent(fields)
	mt.AddStack(100, "UPDATE")
	mt.End()

	if tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID); tenantId != "" {
		if tid, err := uuid.Parse(tenantId); err == nil {
			filter["tenantId"] = tid
		}
	}
	var setBson bson.M
	_, obj, err := bson.MarshalValueWithRegistry(MongoRegistry, fields)
	if err != nil {
		return err
	}
	bson.Unmarshal(obj, &setBson)

	// setBson := structToBson(fields)
	var history = make(map[string]interface{})
	history["ActionAt"] = time.Now()
	helperContext(ctx, history, map[string]string{"author": XAUTHOR, "authorId": XAUTHORID})
	setBson["updated"] = history
	delete(setBson, "_id")

	if os.Getenv("env") == "local" {
		// _, obj, err := bson.MarshalValue(filter)
		fmt.Print(bson.Raw(obj))
	}

	re, err := r.collection.UpdateMany(getContext(ctx), filter, map[string]interface{}{"$set": setBson})

	if err != nil {
		return err
	}

	if re.MatchedCount == 0 {
		return fmt.Errorf("MatchedCountZero")
	}

	return nil
}

func (r *MongoDbRepository[T]) PushMany(
	ctx context.Context,
	filter map[string]interface{},
	fields interface{}) error {

	correlation := uuid.New()
	if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
		if id, err := uuid.Parse(ctxCorrelation); err == nil {
			correlation = id
		}
	}

	mt := r.monitoring.Start(correlation, r.sourceName, TracingTypeRepository)
	mt.AddContent(fields)
	mt.AddStack(100, "UPDATE")
	mt.End()

	if tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID); tenantId != "" {
		if tid, err := uuid.Parse(tenantId); err == nil {
			filter["tenantId"] = tid
		}
	}

	updt := map[string]interface{}{}

	var setBson bson.M
	_, obj, err := bson.MarshalValueWithRegistry(MongoRegistry, fields)
	if err != nil {
		return err
	}

	bson.Unmarshal(obj, &setBson)

	updt["$push"] = setBson

	var history = make(map[string]interface{})
	history["ActionAt"] = time.Now()
	helperContext(ctx, history, map[string]string{"author": XAUTHOR, "authorId": XAUTHORID})
	historyBson := bson.M{"updated": history}
	updt["$set"] = historyBson

	if os.Getenv("env") == "local" {
		fmt.Print(bson.Raw(obj))
	}

	re, err := r.collection.UpdateMany(getContext(ctx), filter, updt)
	if err != nil {
		return err
	}

	if re.MatchedCount == 0 {
		return fmt.Errorf("MatchedCountZero")
	}

	return nil
}

func (r *MongoDbRepository[T]) PullMany(
	ctx context.Context,
	filter map[string]interface{},
	fields interface{}) error {

	correlation := uuid.New()
	if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
		if id, err := uuid.Parse(ctxCorrelation); err == nil {
			correlation = id
		}
	}

	mt := r.monitoring.Start(correlation, r.sourceName, TracingTypeRepository)
	mt.AddContent(fields)
	mt.AddStack(100, "UPDATE")
	mt.End()

	if tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID); tenantId != "" {
		if tid, err := uuid.Parse(tenantId); err == nil {
			filter["tenantId"] = tid
		}
	}

	updt := map[string]interface{}{}

	var setBson bson.M
	_, obj, err := bson.MarshalValueWithRegistry(MongoRegistry, fields)
	if err != nil {
		return err
	}

	bson.Unmarshal(obj, &setBson)

	updt["$pull"] = setBson

	var history = make(map[string]interface{})
	history["ActionAt"] = time.Now()
	helperContext(ctx, history, map[string]string{"author": XAUTHOR, "authorId": XAUTHORID})
	historyBson := bson.M{"updated": history}
	updt["$set"] = historyBson

	if os.Getenv("env") == "local" {
		fmt.Print(bson.Raw(obj))
	}

	re, err := r.collection.UpdateMany(getContext(ctx), filter, updt)
	if err != nil {
		return err
	}

	if re.MatchedCount == 0 {
		return fmt.Errorf("MatchedCountZero")
	}

	return nil
}

func (r *MongoDbRepository[T]) Delete(
	ctx context.Context,
	filter map[string]interface{}) error {

	appendTenantToFilter(ctx, filter)

	correlation := uuid.New()
	if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
		if id, err := uuid.Parse(ctxCorrelation); err == nil {
			correlation = id
		}
	}

	mt := r.monitoring.Start(correlation, r.sourceName, TracingTypeRepository)
	mt.AddContent(filter)
	mt.AddStack(100, "DELETE")
	mt.End()

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Print(bson.Raw(obj), err)
	}

	setBson := bson.M{"active": false}
	re, err := r.collection.UpdateOne(getContext(ctx), filter, map[string]interface{}{"$set": setBson})

	if err != nil {
		return err
	}

	if re.MatchedCount == 0 {
		return fmt.Errorf("MatchedCountZero")
	}

	return nil
}

func (r *MongoDbRepository[T]) DeleteMany(
	ctx context.Context,
	filter map[string]interface{}) error {

	appendTenantToFilter(ctx, filter)

	correlation := uuid.New()
	if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
		if id, err := uuid.Parse(ctxCorrelation); err == nil {
			correlation = id
		}
	}

	mt := r.monitoring.Start(correlation, r.sourceName, TracingTypeRepository)
	mt.AddContent(filter)
	mt.AddStack(100, "DELETEMANY")
	mt.End()

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Print(bson.Raw(obj), err)
	}

	setBson := bson.M{"active": false}
	re, err := r.collection.UpdateMany(getContext(ctx), filter, map[string]interface{}{"$set": setBson})

	if err != nil {
		return err
	}

	if re.MatchedCount == 0 {
		return fmt.Errorf("MatchedCountZero")
	}

	return nil
}

const LOKED = "locked"
const LOKED_EXP = time.Second * 5

var UNLOCK = map[string]interface{}{
	"$set": map[string]interface{}{
		LOKED: false,
	},
}
var LOCK = map[string]interface{}{
	"$set": map[string]interface{}{
		LOKED: true,
	},
}

func rand_await() {
	l := rand.Intn(10)
	for i := 0; i < l; i++ {
		rt := rand.Intn(1000)
		time.Sleep(time.Nanosecond * time.Duration(rt))
	}
}

func (r *MongoDbRepository[T]) lock(ctx context.Context, key map[string]interface{}, d time.Time) error {
	if time.Until(d) > LOKED_EXP {
		return errors.New("lock register expired")
	}
	m := map[string]interface{}{}
	if err := r.collection.FindOne(ctx, key).Decode(&m); err != nil {
		return err
	}
	if v, ok := m[LOKED]; ok && v.(bool) {
		rand_await()
		return r.lock(ctx, key, d)
	}
	return nil
}

func (r *MongoDbRepository[T]) Unlock(
	ctx context.Context,
	id interface{}) error {
	key := map[string]interface{}{"_id": id, LOKED: true}
	appendTenantToFilter(ctx, key)
	rand_await()
	if _, err := r.collection.UpdateOne(ctx, key, UNLOCK); err != nil && err != mongo.ErrNoDocuments {
		return err
	}
	return nil
}

func (r *MongoDbRepository[T]) GetLock(
	ctx context.Context,
	id interface{}) (*T, error) {
	key := map[string]interface{}{"_id": id}
	appendTenantToFilter(ctx, key)
	var t T
	rand_await()
	if err := r.lock(ctx, key, time.Now()); err != nil {
		return nil, err
	}
	if err := r.collection.FindOneAndUpdate(ctx, key, LOCK).Decode(&t); err != nil {
		return nil, err
	}
	return &t, nil
}

func (r *MongoDbRepository[T]) DeleteForce(
	ctx context.Context,
	filter map[string]interface{}) error {

	appendTenantToFilter(ctx, filter)

	correlation := uuid.New()
	if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
		if id, err := uuid.Parse(ctxCorrelation); err == nil {
			correlation = id
		}
	}

	mt := r.monitoring.Start(correlation, r.sourceName, TracingTypeRepository)
	mt.AddContent(filter)
	mt.AddStack(100, "DELETEFORCE")
	mt.End()

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Print(bson.Raw(obj), err)
	}

	_, err := r.collection.DeleteOne(getContext(ctx), filter)

	if err == mongo.ErrNoDocuments {
		return nil
	}

	if err != nil {
		return err
	}

	return nil
}

func (r *MongoDbRepository[T]) DeleteManyForce(
	ctx context.Context,
	filter map[string]interface{}) error {

	appendTenantToFilter(ctx, filter)

	correlation := uuid.New()
	if ctxCorrelation := GetContextHeader(ctx, XCORRELATIONID); ctxCorrelation != "" {
		if id, err := uuid.Parse(ctxCorrelation); err == nil {
			correlation = id
		}
	}

	mt := r.monitoring.Start(correlation, r.sourceName, TracingTypeRepository)
	mt.AddContent(filter)
	mt.AddStack(100, "DELETEMANYFORCE")
	mt.End()

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Print(bson.Raw(obj), err)
	}

	_, err := r.collection.DeleteMany(getContext(ctx), filter)

	if err == mongo.ErrNoDocuments {
		return nil
	}

	if err != nil {
		return err
	}

	return nil
}

func (r *MongoDbRepository[T]) Aggregate(ctx context.Context, pipeline []interface{}) (*mongo.Cursor, error) {

	filter := bson.A{}

	tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID)
	if tid, err := uuid.Parse(tenantId); err == nil {
		filter = bson.A{
			bson.D{
				{"$match",
					bson.D{
						{"$or",
							bson.A{
								bson.D{{"tenantId", uuid.Nil}},
								bson.D{{"tenantId", tid}},
							},
						},
						{"active", true},
					},
				},
			},
		}
	} else {
		filter = bson.A{
			bson.D{
				{"$match",
					bson.D{
						{"active", true},
					},
				},
			},
		}
	}

	filter = append(filter, pipeline...)

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValueWithRegistry(MongoRegistry, filter)
		fmt.Println(bson.Raw(obj), err)
	}

	return r.collection.Aggregate(ctx, filter)
}

func (r *MongoDbRepository[T]) Count(ctx context.Context,
	filter map[string]interface{}, optsFind ...*options.CountOptions) int64 {
	filterAggregator := make(map[string][]interface{})
	filterAggregator["$and"] = append(filterAggregator["$and"], filter)

	appendTenantToFilterAgg(ctx, filterAggregator)
	filterAggregator["$and"] = append(filterAggregator["$and"], bson.D{{"active", true}})

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filterAggregator)
		fmt.Print(bson.Raw(obj), err)
	}

	count, err := r.collection.CountDocuments(getContext(ctx), filterAggregator, optsFind...)
	if err != nil {
		panic(err)
	}

	return count
}

func (r *MongoDbRepository[T]) SetExpiredAfterInsert(ctx context.Context, seconds int32) error {
	opts := options.Index()
	opts.SetExpireAfterSeconds(seconds)
	index := mongo.IndexModel{
		Keys:    bson.M{"created.ActionAt": 1},
		Options: opts,
	}

	_, err := r.collection.Indexes().CreateOne(ctx, index)
	if err != nil {
		panic(err)
	}

	return nil
}

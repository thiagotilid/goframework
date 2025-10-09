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
	sourceName string
}

func NewMongoDbRepository[T interface{}](
	db *mongo.Database,
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
		sourceName: sourcename,
	}
}

func (r *MongoDbRepository[T]) ChangeCollection(collectionName string) {
	r.collection = r.db.Collection(collectionName)
}

func (r *MongoDbRepository[T]) appendTenantToFilterAgg(ctx context.Context, filterAggregator map[string][]interface{}) {
	if tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID); tenantId != "" {
		if tid, err := uuid.Parse(tenantId); err == nil {

			f := bson.A{
				bson.M{"tenantId": tid},
				bson.M{"tenantId": uuid.Nil},
			}

			filterAggregator["$and"] = append(filterAggregator["$and"], map[string]interface{}{"$or": f})
		}
	}
}

func (r *MongoDbRepository[T]) appendTenantToFilter(ctx context.Context, filter map[string]interface{}) {
	if tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID); tenantId != "" {
		if tid, err := uuid.Parse(tenantId); err == nil {
			filter["$or"] = bson.A{
				bson.M{"tenantId": tid},
				bson.M{"tenantId": uuid.Nil},
			}
			filter["active"] = true
		}
	}
}

func (r *MongoDbRepository[T]) appendTenantToFilterWithoutNil(ctx context.Context, filter map[string]interface{}) {
	if tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID); tenantId != "" {
		if tid, err := uuid.Parse(tenantId); err == nil {
			filter["$or"] = bson.A{
				bson.M{"tenantId": tid},
			}
			filter["active"] = true
		}
	}
}

func (r *MongoDbRepository[T]) appendTenantPipeline(ctx context.Context, pipeline bson.A) bson.A {
	tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID)
	var filter bson.A
	if tid, err := uuid.Parse(tenantId); err == nil {
		filter = bson.A{
			bson.D{
				{Key: "$match",
					Value: bson.D{
						{Key: "$or",
							Value: bson.A{
								bson.M{"tenantId": uuid.Nil},
								bson.M{"tenantId": tid},
							},
						},
						{Key: "active", Value: true},
					},
				},
			},
		}
	} else {
		filter = bson.A{
			bson.D{
				{Key: "$match",
					Value: bson.M{"active": true},
				},
			},
		}
	}

	filter = append(filter, pipeline...)

	return filter
}

func (r *MongoDbRepository[T]) appendMatchParams(ctx context.Context, pipeline bson.A) (bson.A, error) {
	def := make(map[string]interface{})

	match, err := r.getInterfaceKey(pipeline[0], "$match")
	if err != nil {
		return nil, err
	}

	def["active"] = true
	tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID)
	if tid, err := uuid.Parse(tenantId); err == nil {
		def["$or"] = bson.A{
			bson.M{"tenantId": uuid.Nil},
			bson.M{"tenantId": tid},
		}
	}

	pipeline[0] = bson.D{{Key: "$match", Value: bson.D{{Key: "$and", Value: []interface{}{def, match}}}}}

	return pipeline, nil
}

func (r *MongoDbRepository[T]) GetAll(
	ctx context.Context,
	filter map[string]interface{},
	optsFind ...*options.FindOptions) *[]T {

	filterAggregator := make(map[string][]interface{})
	filterAggregator["$and"] = append(filterAggregator["$and"], filter, bson.M{"active": true})

	r.appendTenantToFilterAgg(ctx, filterAggregator)
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
	filterAggregator["$and"] = append(filterAggregator["$and"], filter, bson.M{"active": true})
	r.appendTenantToFilterAgg(ctx, filterAggregator)

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
		for cur.Next(mCtx) {
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

func (r *MongoDbRepository[T]) GetFirst(
	ctx context.Context,
	filter map[string]interface{}) *T {
	var el T

	r.appendTenantToFilter(ctx, filter)

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
	bsonMap, err := MarshalWithRegistry(entity)
	if err != nil {
		return nil, err
	}

	var bsonM bson.M
	err = bson.Unmarshal(bsonMap, &bsonM)
	if err != nil {
		return nil, err
	}

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
	bsonMap, err := MarshalWithRegistry(entity)
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

func (r *MongoDbRepository[T]) updateDefaultParam(ctx context.Context, entity interface{}) (bson.M, error) {
	bsonMap, err := MarshalWithRegistry(entity)
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
	bsonM["updated"] = history

	delete(bsonM, "_id")
	delete(bsonM, "tenantId")
	delete(bsonM, "created")
	delete(bsonM, "active")

	return bsonM, nil
}

func (r *MongoDbRepository[T]) pushDefaultParam(ctx context.Context, entity interface{}) (bson.M, error) {
	updt := bson.M{}

	bsonMap, err := MarshalWithRegistry(entity)
	if err != nil {
		return nil, err
	}

	var pushM bson.M
	err = bson.Unmarshal(bsonMap, &pushM)
	if err != nil {
		return nil, err
	}

	updt["$push"] = pushM

	var history = make(map[string]interface{})
	history["ActionAt"] = time.Now()
	helperContext(ctx, history, map[string]string{"author": XAUTHOR, "authorId": XAUTHORID})
	updt["$set"] = bson.M{"updated": history}

	return updt, nil
}

func (r *MongoDbRepository[T]) pullDefaultParam(ctx context.Context, entity interface{}) (bson.M, error) {
	updt := bson.M{}

	bsonMap, err := MarshalWithRegistry(entity)
	if err != nil {
		return nil, err
	}

	var pullM bson.M
	err = bson.Unmarshal(bsonMap, &pullM)
	if err != nil {
		return nil, err
	}

	updt["$pull"] = pullM

	var history = make(map[string]interface{})
	history["ActionAt"] = time.Now()
	helperContext(ctx, history, map[string]string{"author": XAUTHOR, "authorId": XAUTHORID})
	historyBson := bson.M{"updated": history}
	updt["$set"] = historyBson

	return updt, nil
}

func (r *MongoDbRepository[T]) Insert(
	ctx context.Context,
	entity *T) error {

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

	r.appendTenantToFilterWithoutNil(ctx, filter)

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Print(bson.Raw(obj), err)
	}

	var el bson.M
	err := r.collection.FindOne(getContext(ctx), filter).Decode(&el)

	if err == mongo.ErrNoDocuments {
		return r.Insert(ctx, entity)
	}

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

	r.appendTenantToFilter(ctx, filter)

	setBson, err := r.updateDefaultParam(ctx, fields)
	if err != nil {
		return err
	}

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Print(bson.Raw(obj), err)
	}

	payload := make(map[string]interface{})
	payload["$set"] = setBson
	re, err := r.collection.UpdateOne(getContext(ctx), filter, payload)

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

	if tenantId := GetContextHeader(ctx, XTENANTID, TTENANTID); tenantId != "" {
		if tid, err := uuid.Parse(tenantId); err == nil {
			filter["tenantId"] = tid
		}
	}

	setBson, err := r.updateDefaultParam(ctx, fields)
	if err != nil {
		return nil, err
	}

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(fields)
		fmt.Print(bson.Raw(obj), err)
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

	r.appendTenantToFilter(ctx, filter)

	setBson, err := r.updateDefaultParam(ctx, fields)
	if err != nil {
		return err
	}

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(fields)
		fmt.Print(bson.Raw(obj), err)
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

func (r *MongoDbRepository[T]) Push(
	ctx context.Context,
	filter map[string]interface{},
	fields interface{}) error {

	r.appendTenantToFilter(ctx, filter)

	updt, err := r.pushDefaultParam(ctx, fields)
	if err != nil {
		return err
	}

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(fields)
		fmt.Print(bson.Raw(obj), err)
	}

	re, err := r.collection.UpdateOne(getContext(ctx), filter, updt)
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

	r.appendTenantToFilter(ctx, filter)

	updt, err := r.pushDefaultParam(ctx, fields)
	if err != nil {
		return err
	}

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(fields)
		fmt.Print(bson.Raw(obj), err)
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

func (r *MongoDbRepository[T]) Pull(
	ctx context.Context,
	filter map[string]interface{},
	fields interface{}) error {

	r.appendTenantToFilter(ctx, filter)

	updt, err := r.pullDefaultParam(ctx, fields)
	if err != nil {
		return err
	}

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(fields)
		fmt.Print(bson.Raw(obj), err)
	}

	re, err := r.collection.UpdateOne(getContext(ctx), filter, updt)
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

	r.appendTenantToFilter(ctx, filter)

	updt, err := r.pullDefaultParam(ctx, fields)
	if err != nil {
		return err
	}

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(fields)
		fmt.Print(bson.Raw(obj), err)
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

	r.appendTenantToFilter(ctx, filter)

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

	r.appendTenantToFilter(ctx, filter)

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
	r.appendTenantToFilter(ctx, key)
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
	r.appendTenantToFilter(ctx, key)
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

	r.appendTenantToFilter(ctx, filter)

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

	r.appendTenantToFilter(ctx, filter)

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

func (r *MongoDbRepository[T]) Aggregate(ctx context.Context, pipeline bson.A) (*mongo.Cursor, error) {
	var filter bson.A

	ok, err := r.checkPipelineStartMatch(pipeline)
	if err != nil {
		return nil, err
	}

	if ok {
		filter, err = r.appendMatchParams(ctx, pipeline)
		if err != nil {
			return nil, err
		}
	} else {
		filter = r.appendTenantPipeline(ctx, pipeline)
	}

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Println(bson.Raw(obj), err)
	}

	return r.collection.Aggregate(ctx, filter)
}

func (r *MongoDbRepository[T]) DefaultAggregate(ctx context.Context, filter bson.A) (*mongo.Cursor, error) {
	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Println(bson.Raw(obj), err)
	}

	return r.collection.Aggregate(ctx, filter)
}

func (r *MongoDbRepository[T]) Count(ctx context.Context,
	filter map[string]interface{}, optsFind ...*options.CountOptions) int64 {
	filterAggregator := make(map[string][]interface{})
	filterAggregator["$and"] = append(filterAggregator["$and"], filter)

	r.appendTenantToFilterAgg(ctx, filterAggregator)
	filterAggregator["$and"] = append(filterAggregator["$and"], bson.M{"active": true})

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

func (r *MongoDbRepository[T]) checkPipelineStartMatch(pipeline bson.A) (bool, error) {
	if len(pipeline) == 0 {
		return false, nil
	}

	switch v := pipeline[0].(type) {
	case map[string]interface{}:
		_, ok := v["$match"]
		return ok, nil
	case bson.D:
		for _, e := range v {
			if e.Key == "$match" {
				return true, nil
			}
		}
		return false, nil
	case bson.M:
		_, ok := v["$match"]
		return ok, nil
	default:
		return false, errors.New("invalid type")
	}
}

func (r *MongoDbRepository[T]) getInterfaceKey(obj interface{}, key string) (interface{}, error) {

	switch v := obj.(type) {
	case map[string]interface{}:
		m, ok := v["$match"]
		if !ok {
			return nil, errors.New("key not found")
		}
		return m, nil
	case bson.D:
		for _, e := range v {
			if e.Key == "$match" {
				return e.Value, nil
			}
		}
		return nil, errors.New("key not found")
	case bson.M:
		m, ok := v["$match"]
		if !ok {
			return nil, errors.New("key not found")
		}
		return m, nil
	default:
		return false, errors.New("invalid type")
	}
}

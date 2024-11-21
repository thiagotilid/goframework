package goframework

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	tUUID       = reflect.TypeOf(uuid.UUID{})
	tstr        = reflect.TypeOf("")
	uuidSubtype = byte(0x04)

	MongoRegistry = newMongoRegistry()
)

func StringNormalizeEncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tstr {
		return bsoncodec.ValueEncoderError{Name: "stringNormalizeEncodeValue", Types: []reflect.Type{tstr}, Received: val}
	}
	b := val.Interface().(string)
	return vw.WriteString(strings.ToUpper(b))
}

func UuidEncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tUUID {
		return bsoncodec.ValueEncoderError{Name: "uuidEncodeValue", Types: []reflect.Type{tUUID}, Received: val}
	}
	b := val.Interface().(uuid.UUID)
	return vw.WriteBinaryWithSubtype(b[:], uuidSubtype)
}

func UuidDecodeValue(dc bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	if !val.CanSet() || val.Type() != tUUID {
		return bsoncodec.ValueDecoderError{Name: "uuidDecodeValue", Types: []reflect.Type{tUUID}, Received: val}
	}

	var data []byte
	var subtype byte
	var err error
	switch vrType := vr.Type(); vrType {
	case bson.TypeBinary:
		data, subtype, err = vr.ReadBinary()
		if subtype != uuidSubtype {
			return fmt.Errorf("unsupported binary subtype %v for UUID", subtype)
		}
	case bson.TypeNull:
		err = vr.ReadNull()
	case bson.TypeUndefined:
		err = vr.ReadUndefined()
	default:
		return fmt.Errorf("cannot decode %v into a UUID", vrType)
	}

	if err != nil {
		return err
	}
	uuid2, err := uuid.FromBytes(data)
	if err != nil {
		return err
	}
	val.Set(reflect.ValueOf(uuid2))
	return nil
}

func newMongoClient(opts *options.ClientOptions, normalize bool) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if normalize {
		MongoRegistry.RegisterTypeEncoder(tstr, bsoncodec.ValueEncoderFunc(StringNormalizeEncodeValue))
	}

	return mongo.Connect(ctx, opts.SetRegistry(MongoRegistry))

}

func newMongoRegistry() *bsoncodec.Registry {
	regs := bson.NewRegistry()

	regs.RegisterTypeEncoder(tUUID, bsoncodec.ValueEncoderFunc(UuidEncodeValue))
	regs.RegisterTypeDecoder(tUUID, bsoncodec.ValueDecoderFunc(UuidDecodeValue))

	return regs
}

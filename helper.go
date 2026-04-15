package goframework

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
)

const (
	XTENANTID      string = "X-Tenant-Id"
	TTENANTID      string = "tenant_id"
	XAUTHOR        string = "X-Author"
	XAUTHORID      string = "X-Author-Id"
	XCORRELATIONID string = "X-Correlation-Id"
	XCREATEDAT     string = "X-CreatedAt"

	B2B2C      string = "assistancecompanies"
	XCUSTOMATTR string = "X-Custom-Attr"
)

func helperContext(c context.Context, filter map[string]interface{}, addfilter map[string]string) {
	switch c := c.(type) {
	case *gin.Context:
		for k, v := range addfilter {
			value := string(c.Request.Header.Get(v))
			if value != "" {
				filter[k] = value
			}
		}
	case *ConsumerContext:
		for k, v := range addfilter {
			for _, kh := range c.Msg.Headers {
				if kh.Key == v {
					filter[k] = string(kh.Value)
					break
				}
			}
		}
	default:
		for k, v := range addfilter {
			value := fmt.Sprint(c.Value(v))
			if value != "" {
				filter[k] = value
				break
			}
		}
	}
}

func GetContextHeader(c context.Context, keys ...string) string {

	for _, key := range keys {
		switch c := c.(type) {
		case *gin.Context:
			if sid := c.Request.Header.Get(key); sid != "" {
				return sid
			}

		case *ConsumerContext:
			for _, kh := range c.Msg.Headers {
				if kh.Key == key && len(kh.Value) > 0 {
					return string(kh.Value)
				}
			}
		default:
			return fmt.Sprint(c.Value(key))
		}
	}

	return ""
}

func getContext(c context.Context) context.Context {
	switch c := c.(type) {
	case *gin.Context:
		return c.Request.Context()
	default:
		return c
	}
}

type kHeader struct {
	keys map[string]string
}

func (kh *kHeader) ToKafkaHeader() []kafka.Header {
	var header []kafka.Header
	for k, v := range kh.keys {
		header = append(header, kafka.Header{Key: k, Value: []byte(v)})
	}
	return header
}

func (kh *kHeader) ToMapStringSlice() map[string][]string {
	result := make(map[string][]string)
	for k, v := range kh.keys {
		result[k] = []string{v}
	}
	return result
}

func (kh *kHeader) GetString(key string) string {
	if v, ok := kh.keys[key]; ok {
		return v
	}
	return ""
}

func (kh *kHeader) GetUuid(key string) uuid.UUID {
	if v, ok := kh.keys[key]; ok {
		if id, err := uuid.Parse(v); err == nil {
			return id
		}
	}
	return uuid.New()
}

func helperContextKafka(c context.Context, addfilter []string) *kHeader {

	filter := &kHeader{keys: map[string]string{}}
	switch c := c.(type) {
	case *gin.Context:
		for _, k := range addfilter {
			value := c.Request.Header.Get(k)
			if value == "" {
				switch k {
				case XCORRELATIONID:
					value := uuid.NewString()
					c.Request.Header.Add(XCORRELATIONID, value)
				case XCREATEDAT:
					value := time.Now().Format(time.RFC3339)
					c.Request.Header.Add(XCREATEDAT, value)
				}
			}
			filter.keys[k] = value
		}
	case *ConsumerContext:
		for _, k := range addfilter {
			for _, kh := range c.Msg.Headers {
				if kh.Key == k {
					filter.keys[k] = string(kh.Value)
					break
				}
			}
			if _, ok := filter.keys[k]; !ok {
				switch k {
				case XCORRELATIONID:
					filter.keys[k] = uuid.NewString()
				case XCREATEDAT:
					filter.keys[k] = time.Now().Format(time.RFC3339)
				}
			}
		}
	default:
		for _, k := range addfilter {

			value := fmt.Sprint(c.Value(k))
			if value == "" {
				switch k {
				case XCORRELATIONID:
					value := uuid.NewString()
					c = context.WithValue(c, k, value)
				case XCREATEDAT:
					value := time.Now().Format(time.RFC3339)
					c = context.WithValue(c, k, value)
				}
			}
			filter.keys[k] = value
		}
	}

	return filter
}

func ToContext(c context.Context) context.Context {
	listContext := []string{XTENANTID, XAUTHOR, XAUTHORID, XCORRELATIONID, TTENANTID, XCREATEDAT, XCUSTOMATTR}

	cc := c
	switch c := c.(type) {
	case *gin.Context:
		cc = c.Request.Context()
		for _, v := range listContext {
			cc = context.WithValue(cc, v, c.Request.Header.Get(v))
		}
	case *ConsumerContext:
		cc = c.Context
		for _, v := range listContext {
			for _, kh := range c.Msg.Headers {
				if kh.Key == v {
					cc = context.WithValue(cc, v, string(kh.Value))
					break
				}
			}
		}
	default:
		for _, v := range listContext {
			cc = context.WithValue(cc, v, fmt.Sprint(c.Value(v)))
		}
	}
	return cc
}

func AddToContext(c context.Context, key string, value string) {
	switch c := c.(type) {
	case *gin.Context:
		c.Request.Header.Add(key, value)
	case *ConsumerContext:
		c.Msg.Headers = append(c.Msg.Headers, kafka.Header{Key: key, Value: []byte(value)})
	default:
		c = context.WithValue(c, key, value)
	}
}

func GetCustomAttr(ctx context.Context) map[string]string {
	raw := GetContextHeader(ctx, XCUSTOMATTR)
	if raw == "" {
		return nil
	}
	var attrs map[string]string
	if err := json.Unmarshal([]byte(raw), &attrs); err != nil {
		return nil
	}
	if len(attrs) == 0 {
		return nil
	}
	if len(globalCustomAttrMap) == 0 {
		return attrs
	}
	mapped := make(map[string]string, len(attrs))
	for k, v := range attrs {
		if replacement, ok := globalCustomAttrMap[k]; ok {
			mapped[replacement] = v
		} else {
			mapped[k] = v
		}
	}
	return mapped
}

func checkNestedFieldExists(doc bson.M, dotKey string) bool {
	parts := strings.Split(dotKey, ".")
	current := map[string]interface{}(doc)
	for _, part := range parts {
		val, ok := current[part]
		if !ok {
			return false
		}
		if nested, ok := val.(bson.M); ok {
			current = map[string]interface{}(nested)
		} else if nested, ok := val.(map[string]interface{}); ok {
			current = nested
		} else {
			if part == parts[len(parts)-1] {
				return true
			}
			return false
		}
	}
	return true
}

func setNestedField(doc bson.M, dotKey string, value string) {
	parts := strings.Split(dotKey, ".")
	current := map[string]interface{}(doc)
	for i, part := range parts {
		if i == len(parts)-1 {
			current[part] = value
			return
		}
		if nested, ok := current[part].(bson.M); ok {
			current = map[string]interface{}(nested)
		} else if nested, ok := current[part].(map[string]interface{}); ok {
			current = nested
		} else {
			return
		}
	}
}

func GetTenantByToken(ctx *gin.Context) (uuid.UUID, error) {
	tokenString := ctx.GetHeader("Authorization")

	tokenString = strings.Replace(tokenString, "Bearer ", "", 1)
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return uuid.Nil, err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		tenant := fmt.Sprint(claims[TTENANTID])
		if tenant == "" {
			return uuid.Nil, fmt.Errorf("tenant not found")
		}
		id, err := uuid.Parse(tenant)
		if err != nil {
			return uuid.Nil, fmt.Errorf("tenant not found")
		}

		return id, nil
	} else {
		return uuid.Nil, fmt.Errorf("tenant not found")
	}
}

func MarshalWithRegistry(val interface{}) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	vw, err := bsonrw.NewBSONValueWriter(buf)
	if err != nil {
		panic(err)
	}

	enc, err := bson.NewEncoder(vw)
	if err != nil {
		panic(err)
	}

	enc.SetRegistry(MongoRegistry)

	if err := enc.Encode(val); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func UnmarshalWithRegistry(data []byte, val interface{}) error {
	dec, err := bson.NewDecoder(bsonrw.NewBSONDocumentReader(data))
	if err != nil {
		panic(err)
	}
	dec.SetRegistry(MongoRegistry)

	return dec.Decode(val)
}

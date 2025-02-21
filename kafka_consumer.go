package goframework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type (
	KafkaConsumerSettings struct {
		Topic             string
		NumPartitions     int
		ReplicationFactor int
		GroupId           string
		AutoOffsetReset   string
		Retries           uint16
	}

	KafkaConsumer struct {
		kcs *KafkaConsumerSettings
		kcm *kafka.ConfigMap
	}

	KafkaContext struct {
		Msg              *kafka.Message
		RemainingRetries int
		Faulted          bool
	}
)

func NewKafkaConsumer(kcm *kafka.ConfigMap,
	kcs *KafkaConsumerSettings, fn ConsumerFunc) *KafkaConsumer {

	CreateKafkaTopic(context.Background(), kcm, &TopicConfiguration{
		Topic:             kcs.Topic,
		NumPartitions:     kcs.NumPartitions,
		ReplicationFactor: kcs.ReplicationFactor,
	})

	cmt := *kcm
	cmt.SetKey("group.id", kcs.GroupId)
	cmt.SetKey("auto.offset.reset", kcs.AutoOffsetReset)
	kc := &KafkaConsumer{
		kcs: kcs,
		kcm: &cmt,
	}
	return kc
}

func recover_error(fn func(error)) {
	if e := recover(); e != nil {
		switch ee := e.(type) {
		case error:
			fn(ee)
		case string:
			fn(errors.New(ee))
		default:
			fn(fmt.Errorf("undefined error: %v", ee))
		}
	}
}

func kafkaCallFnWithResilence(
	ctx context.Context,
	msg *kafka.Message,
	kcm *kafka.ConfigMap,
	kcs KafkaConsumerSettings,
	fn ConsumerFunc,
	telemetry bool) {

	cctx := &ConsumerContext{
		Context:          ctx,
		RemainingRetries: kcs.Retries,
		Faulted:          kcs.Retries == 0,
		Msg:              msg}

	if telemetry {
		g, _ := kcm.Get("group.id", "")
		nctx, span := otel.GetTracerProvider().Tracer("goframework.kafka.consumer").Start(cctx.Context, fmt.Sprintf("CONSUMER:%s", kcs.Topic), trace.WithAttributes(attribute.KeyValue{Key: "kafka.group", Value: attribute.StringValue(g.(string))}))
		cctx.Context = nctx
		defer span.End()
	}

	defer recover_error(func(err error) {
		fmt.Println(err.Error())
		if kcs.Retries > 1 {
			kcs.Retries--
			kafkaCallFnWithResilence(ctx, msg, kcm, kcs, fn, telemetry)
			return
		}
		kafkaSendToDlq(cctx, kcm, msg, err, debug.Stack())
	})

	fn(cctx)
}

type consumerError struct {
	Error   string
	Group   string
	Content map[string]interface{}
	Stack   string
}

func kafkaSendToDlq(
	ctx context.Context,
	kcm *kafka.ConfigMap,
	msg *kafka.Message,
	er error,
	stack []byte) {
	p, err := kafka.NewProducer(kcm)
	if err != nil {
		panic(err)
	}
	defer p.Close()

	emsg := *msg
	tpn := *emsg.TopicPartition.Topic + "_error"

	CreateKafkaTopic(ctx, kcm, &TopicConfiguration{
		Topic:             tpn,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})

	v, _ := kcm.Get("group.id", "")

	var content map[string]interface{}
	if err := json.Unmarshal(emsg.Value, &content); err != nil {
		fmt.Print(err)
	}

	emsg.TopicPartition.Topic = &tpn
	emsg.TopicPartition.Partition = kafka.PartitionAny
	msgErr := &consumerError{
		Error:   er.Error(),
		Group:   fmt.Sprint(v),
		Content: content,
		Stack:   string(stack),
	}
	msgbkp := emsg.Value
	emsg.Value, err = json.Marshal(msgErr)
	if err != nil {
		emsg.Value = msgbkp
	}

	dlc := make(chan kafka.Event)
	if er = p.Produce(&emsg, dlc); er != nil {
		panic(er)
	}
	<-dlc
}

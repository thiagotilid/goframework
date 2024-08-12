package goframework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"

	"github.com/confluentinc/confluent-kafka-go/kafka"
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
	tm *TracingMonitor,
	msg *kafka.Message,
	kcm *kafka.ConfigMap,
	kcs KafkaConsumerSettings,
	fn ConsumerFunc) {

	cctx := &ConsumerContext{
		Context:          ctx,
		RemainingRetries: kcs.Retries,
		Faulted:          kcs.Retries == 0,
		Msg:              msg}

	defer recover_error(func(err error) {
		fmt.Println(err.Error())
		if kcs.Retries > 1 {
			kcs.Retries--
			tm.AddStack(500, fmt.Sprintf("CONSUMER ERROR. RETRY %d: %s", kcs.Retries, err.Error()))
			kafkaCallFnWithResilence(ctx, tm, msg, kcm, kcs, fn)
			return
		}
		kafkaSendToDlq(cctx, tm, &kcs, kcm, msg, err, debug.Stack())
		tm.AddStack(500, "CONSUMER ERROR.")
	})
	tm.AddStack(100, "CONSUMING...")
	fn(cctx)
	tm.AddStack(200, "SUCCESSFULLY CONSUMED")
}

type consumerError struct {
	Error   string
	Group   string
	Content map[string]interface{}
	Stack   string
}

func kafkaSendToDlq(
	ctx context.Context,
	tm *TracingMonitor,
	kcs *KafkaConsumerSettings,
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
	if err != nil {
		v = ""
	}

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
		tm.AddStack(500, "ERROR TO PRODUCE ERROR MSG: "+er.Error())
		panic(er)
	}
	<-dlc
	tm.AddStack(100, "ERROR PRODUCED IN "+tpn)
}

package goframework

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type (
	TopicConfiguration struct {
		Topic             string
		NumPartitions     int
		ReplicationFactor int
	}

	GoKafka struct {
		server           string
		groupId          string
		securityprotocol string
		saslmechanism    string
		saslusername     string
		saslpassword     string

		// lc is set by GoFramework.RegisterKafka. A nil lc keeps the consumers running
		// until the process ends, as before graceful shutdown existed.
		lc *lifecycle
	}
)

func NewKafkaConfigMap(connectionString string,
	groupId string,
	securityprotocol string,
	saslmechanism string,
	saslusername string,
	saslpassword string) *GoKafka {
	return &GoKafka{
		server:           connectionString,
		groupId:          groupId,
		securityprotocol: securityprotocol,
		saslmechanism:    saslmechanism,
		saslusername:     saslusername,
		saslpassword:     saslpassword,
	}
}

func recover_all() {
	if e := recover(); e != nil {
		switch ee := e.(type) {
		case error:
			fmt.Println(ee)
		case string:
			fmt.Println(errors.New(ee))
		default:
			fmt.Println(fmt.Errorf("undefined error: %v", ee))
		}
	}
}

type (
	ConsumerSettings struct {
		AutoOffsetReset string
		Retries         int
	}

	ConsumerMultiRoutineSettings struct {
		Routines          int
		AutoOffsetReset   string
		Numpartitions     int
		Retries           int
		ReplicationFactor int
	}
)

// consumerPollTimeout bounds each ReadMessage so a consumer notices the shutdown signal.
const consumerPollTimeout = 500 * time.Millisecond

// messageSource is the part of *kafka.Consumer the consumer loops use. It exists so the
// shutdown behavior can be tested without a broker.
type messageSource interface {
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	CommitMessage(m *kafka.Message) ([]kafka.TopicPartition, error)
	Close() error
}

// Seams replaced by tests.
var (
	newMessageSource = func(cm *kafka.ConfigMap) (messageSource, error) {
		c, err := kafka.NewConsumer(cm)
		if err != nil {
			return nil, err
		}
		return c, nil
	}
	createTopic = CreateKafkaTopic
	sendToDlq   = kafkaSendToDlq
)

func (k *GoKafka) consumerConfig(autoOffsetReset string) *kafka.ConfigMap {
	kc := &kafka.ConfigMap{
		"bootstrap.servers":             k.server,
		"group.id":                      k.groupId,
		"auto.offset.reset":             autoOffsetReset,
		"partition.assignment.strategy": "cooperative-sticky",
		"enable.auto.commit":            false,
	}

	if len(k.securityprotocol) > 0 {
		kc.SetKey("security.protocol", k.securityprotocol)
	}

	if len(k.saslmechanism) > 0 {
		kc.SetKey("sasl.mechanism", k.saslmechanism)
	}

	if len(k.saslusername) > 0 {
		kc.SetKey("sasl.username", k.saslusername)
	}

	if len(k.saslpassword) > 0 {
		kc.SetKey("sasl.password", k.saslpassword)
	}

	return kc
}

func (k *GoKafka) openConsumer(kc *kafka.ConfigMap, topic string) messageSource {
	consumer, err := newMessageSource(kc)
	if err != nil {
		log.Fatalln(err.Error())
		panic(err)
	}

	err = consumer.SubscribeTopics([]string{topic}, rebalanceCallback)
	if err != nil {
		log.Fatalln(err.Error())
		panic(err)
	}

	return consumer
}

func isPollTimeout(err error) bool {
	var kerr kafka.Error
	return errors.As(err, &kerr) && kerr.Code() == kafka.ErrTimedOut
}

// consumeLoop reads messages until ctx is cancelled. The message being handled when the
// signal arrives is finished by handle; only the reading stops. ctx is never passed to
// handle, so handlers are not cancelled by the shutdown.
func consumeLoop(ctx context.Context, src messageSource, handle func(*kafka.Message)) {
	for ctx.Err() == nil {
		msg, err := src.ReadMessage(consumerPollTimeout)
		if err != nil {
			if isPollTimeout(err) {
				continue
			}
			log.Println(err.Error())
			continue
		}
		handle(msg)
	}
}

// handleMessage runs one message through the handler and commits it afterwards.
// The handler receives a *ConsumerContext built from the message, which is how the tenant
// reaches it (message headers).
func handleMessage(tracer trace.Tracer,
	topic string,
	consumer messageSource,
	kc *kafka.ConfigMap,
	kcs *KafkaConsumerSettings,
	fn ConsumerFunc,
	msg *kafka.Message) {
	carrier := kafkaHeaderCarrier{&msg.Headers}
	ctx := propagation.TraceContext{}.Extract(context.Background(), carrier)
	ctx, span := tracer.Start(ctx, fmt.Sprintf("KAFKA SUB %s", topic),
		trace.WithAttributes(attribute.String("messaging.system", "kafka")),
		trace.WithAttributes(attribute.String("messaging.destination.name", topic)),
	)

	kafkaCallFnWithResilence(ctx, msg, kc, *kcs, fn)
	_, span2 := tracer.Start(ctx, "KAFKA COMMIT",
		trace.WithAttributes(attribute.String("messaging.system", "kafka")),
		trace.WithAttributes(attribute.String("messaging.destination.name", topic)),
		trace.WithAttributes(attribute.String("messaging.message.id", string(msg.Key))),
	)
	commitMessage(consumer, msg)
	span2.End()
	span.End()
}

// commitMessage commits the offset of msg. A failed commit is not fatal (the message is
// delivered again after a restart or rebalance, which is the at-least-once contract), but it
// must not be silent: it is the only sign that offsets are not advancing.
func commitMessage(consumer messageSource, msg *kafka.Message) {
	if _, err := consumer.CommitMessage(msg); err != nil {
		topic := ""
		if msg.TopicPartition.Topic != nil {
			topic = *msg.TopicPartition.Topic
		}
		log.Printf("goframework: kafka commit failed: topic=%s partition=%d offset=%d: %v",
			topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, err)
	}
}

func (k *GoKafka) trackConsumer(topic string) bool {
	if k.lc.track() {
		return true
	}
	log.Printf("goframework: consumer for topic %s not started: shutting down", topic)
	return false
}

func (k *GoKafka) worker(id int, messages <-chan *kafka.Message, consumer messageSource, fn ConsumerFunc, kc *kafka.ConfigMap, kcs *KafkaConsumerSettings, done chan<- struct{}) {
	tracer := otel.Tracer("")

	for msg := range messages {
		func(cmsg *kafka.Message,
			cconsumer messageSource,
			ckc *kafka.ConfigMap,
			ckcs KafkaConsumerSettings,
			cfn ConsumerFunc) {
			defer recover_all()
			defer commitMessage(cconsumer, cmsg)

			carrier := kafkaHeaderCarrier{&msg.Headers}
			ctx := propagation.TraceContext{}.Extract(context.Background(), carrier)
			ctx, span := tracer.Start(ctx, fmt.Sprintf("KAFKA SUB %s", kcs.Topic),
				trace.WithAttributes(attribute.String("messaging.system", "kafka")),
				trace.WithAttributes(attribute.String("messaging.destination.name", kcs.Topic)),
			)
			kafkaCallFnWithResilence(ctx, cmsg, ckc, ckcs, cfn)
			_, span2 := tracer.Start(ctx, "KAFKA COMMIT",
				trace.WithAttributes(attribute.String("messaging.system", "kafka")),
				trace.WithAttributes(attribute.String("messaging.destination.name", kcs.Topic)),
				trace.WithAttributes(attribute.String("messaging.message.id", string(msg.Key))),
			)
			commitMessage(consumer, msg)
			span2.End()
			span.End()

		}(msg, consumer, kc, *kcs, fn)
	}
	done <- struct{}{}
}

func (k *GoKafka) ConsumerWithWorker(topic string,
	fn ConsumerFunc,
	cfg ConsumerMultiRoutineSettings) {
	if !k.trackConsumer(topic) {
		return
	}
	messages := make(chan *kafka.Message, 100)
	done := make(chan struct{}, cfg.Routines)
	go func(topic string) {
		defer k.lc.done()

		kcs := &KafkaConsumerSettings{
			Topic:           topic,
			AutoOffsetReset: cfg.AutoOffsetReset,
			Retries:         uint16(cfg.Retries),
		}
		kc := k.consumerConfig(kcs.AutoOffsetReset)
		fmt.Fprintf(os.Stdout,
			"%% Start consumer %s \n",
			k.groupId)
		createTopic(context.Background(), kc, &TopicConfiguration{
			Topic:             topic,
			NumPartitions:     cfg.Numpartitions,
			ReplicationFactor: cfg.ReplicationFactor,
		})
		consumer := k.openConsumer(kc, kcs.Topic)
		defer consumer.Close()

		for i := 0; i < cfg.Routines; i++ {
			go k.worker(i, messages, consumer, fn, kc, kcs, done)
		}

		consumeLoop(k.lc.Context(), consumer, func(msg *kafka.Message) {
			messages <- msg
		})

		// Reading stopped: let the workers finish what was already read, then leave the group.
		close(messages)
		for i := 0; i < cfg.Routines; i++ {
			<-done
		}
	}(topic)
}

func (k *GoKafka) ConsumerMultiRoutine(
	topic string,
	fn ConsumerFunc,
	cfg ConsumerMultiRoutineSettings) {
	if !k.trackConsumer(topic) {
		return
	}
	go func(topic string) {
		defer k.lc.done()
		tracer := otel.Tracer("")

		kcs := &KafkaConsumerSettings{
			Topic:           topic,
			AutoOffsetReset: cfg.AutoOffsetReset,
			Retries:         uint16(cfg.Retries),
		}
		kc := k.consumerConfig(kcs.AutoOffsetReset)
		fmt.Fprintf(os.Stdout,
			"%% Start consumer %s \n",
			k.groupId)
		createTopic(context.Background(), kc, &TopicConfiguration{
			Topic:             topic,
			NumPartitions:     cfg.Numpartitions,
			ReplicationFactor: cfg.ReplicationFactor,
		})
		consumer := k.openConsumer(kc, kcs.Topic)
		defer consumer.Close()

		// Runs before consumer.Close: in-flight messages finish and commit first.
		var inflight sync.WaitGroup
		defer inflight.Wait()

		lctx := k.lc.Context()
		var running atomic.Int32
		consumeLoop(lctx, consumer, func(msg *kafka.Message) {
			running.Add(1)
			inflight.Add(1)
			go func(cmsg *kafka.Message,
				cconsumer messageSource,
				ckc *kafka.ConfigMap,
				ckcs KafkaConsumerSettings,
				cfn ConsumerFunc) {
				defer inflight.Done()
				defer recover_all()
				defer commitMessage(cconsumer, cmsg)
				defer running.Add(-1)
				carrier := kafkaHeaderCarrier{&cmsg.Headers}
				ctx := propagation.TraceContext{}.Extract(context.Background(), carrier)
				ctx, span := tracer.Start(ctx, fmt.Sprintf("KAFKA SUB %s", topic),
					trace.WithAttributes(attribute.String("messaging.system", "kafka")),
					trace.WithAttributes(attribute.String("messaging.destination.name", topic)),
				)
				kafkaCallFnWithResilence(ctx, cmsg, ckc, ckcs, cfn)
				_, span2 := tracer.Start(ctx, "KAFKA COMMIT",
					trace.WithAttributes(attribute.String("messaging.system", "kafka")),
					trace.WithAttributes(attribute.String("messaging.destination.name", topic)),
					trace.WithAttributes(attribute.String("messaging.message.id", string(cmsg.Key))),
				)
				commitMessage(consumer, cmsg)
				span2.End()
				span.End()
			}(msg, consumer, kc, *kcs, fn)

			// Wait, a second at a time, while every routine is busy.
			for int(running.Load()) >= cfg.Routines && lctx.Err() == nil {
				select {
				case <-time.After(time.Second):
				case <-lctx.Done():
				}
			}
		})
	}(topic)
}

func (k *GoKafka) Consumer(topic string, fn ConsumerFunc) {
	if !k.trackConsumer(topic) {
		return
	}
	go func(topic string) {
		defer k.lc.done()
		tracer := otel.Tracer("")

		kcs := &KafkaConsumerSettings{
			Topic:           topic,
			AutoOffsetReset: "earliest",
			Retries:         5,
		}

		kc := k.consumerConfig(kcs.AutoOffsetReset)

		fmt.Fprintf(os.Stdout,
			"%% Start consumer %s \n",
			k.groupId)

		consumer := k.openConsumer(kc, kcs.Topic)
		defer consumer.Close()

		consumeLoop(k.lc.Context(), consumer, func(msg *kafka.Message) {
			handleMessage(tracer, topic, consumer, kc, kcs, fn, msg)
		})
	}(topic)
}

func (k *GoKafka) ConsumerWithSettings(topic string, fn ConsumerFunc, cs ConsumerSettings) {
	if !k.trackConsumer(topic) {
		return
	}
	go func(topic string) {
		defer k.lc.done()
		tracer := otel.Tracer("")

		kcs := &KafkaConsumerSettings{
			Topic:           topic,
			AutoOffsetReset: cs.AutoOffsetReset,
			Retries:         uint16(cs.Retries),
		}

		kc := k.consumerConfig(kcs.AutoOffsetReset)

		fmt.Fprintf(os.Stdout,
			"%% Start consumer %s \n",
			k.groupId)

		consumer := k.openConsumer(kc, kcs.Topic)
		defer consumer.Close()

		consumeLoop(k.lc.Context(), consumer, func(msg *kafka.Message) {
			handleMessage(tracer, topic, consumer, kc, kcs, fn, msg)
		})
	}(topic)
}

func rebalanceCallback(c *kafka.Consumer, event kafka.Event) error {

	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		fmt.Fprintf(os.Stderr,
			"%% %s rebalance: %d new partition(s) assigned: %v\n",
			c.GetRebalanceProtocol(), len(ev.Partitions),
			ev.Partitions)

		err := c.IncrementalAssign(ev.Partitions)
		if err != nil {
			panic(err)
		}

	case kafka.RevokedPartitions:
		fmt.Fprintf(os.Stderr,
			"%% %s rebalance: %d partition(s) revoked: %v\n",
			c.GetRebalanceProtocol(), len(ev.Partitions),
			ev.Partitions)
		if c.AssignmentLost() {
			fmt.Fprintf(os.Stderr, "%% Current assignment lost!\n")
		}
	}

	return nil
}

func NewKafkaAdminClient(cm *kafka.ConfigMap) *kafka.AdminClient {
	adm, err := kafka.NewAdminClient(cm)

	if err != nil {
		panic(err)
	}
	return adm
}

func CreateKafkaTopic(ctx context.Context,
	kcm *kafka.ConfigMap,
	tpc *TopicConfiguration) {

	admc := NewKafkaAdminClient(kcm)
	defer admc.Close()
	r, err := admc.CreateTopics(ctx,
		[]kafka.TopicSpecification{{
			Topic:             tpc.Topic,
			NumPartitions:     tpc.NumPartitions,
			ReplicationFactor: tpc.ReplicationFactor}},
		kafka.SetAdminOperationTimeout(time.Minute))

	if err != nil {
		panic(err)
	}

	if r[0].Error.Code() != kafka.ErrNoError &&
		r[0].Error.Code() != kafka.ErrTopicAlreadyExists {
		panic(r[0].Error.String())
	}
}

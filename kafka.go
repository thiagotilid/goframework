package goframework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
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
		monitoring       *Monitoring
		nrapp            GfAgentTelemetry
		securityprotocol string
		saslmechanism    string
		saslusername     string
		saslpassword     string
	}
)

func NewKafkaConfigMap(connectionString string,
	groupId string,
	securityprotocol string,
	saslmechanism string,
	saslusername string,
	saslpassword string,
	monitoring *Monitoring) *GoKafka {
	return &GoKafka{
		server:           connectionString,
		groupId:          groupId,
		securityprotocol: securityprotocol,
		saslmechanism:    saslmechanism,
		saslusername:     saslusername,
		saslpassword:     saslpassword,
		monitoring:       monitoring,
	}
}

func (k *GoKafka) newMonitor(nrapp GfAgentTelemetry) {
	k.nrapp = nrapp
}

func wait_until(fn func() bool) {
	for fn() {
		time.Sleep(time.Second)
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

func (k *GoKafka) worker(id int, messages <-chan *kafka.Message, consumer *kafka.Consumer, fn ConsumerFunc, kc *kafka.ConfigMap, kcs *KafkaConsumerSettings, done chan<- struct{}) {
	for msg := range messages {
		log.Printf("[Worker %d] Processando mensagem: %s", id, string(msg.Value))
		func(cmsg *kafka.Message,
			cconsumer *kafka.Consumer,
			ckc *kafka.ConfigMap,
			ckcs KafkaConsumerSettings,
			nrapp GfAgentTelemetry,
			cfn ConsumerFunc) {
			defer recover_all()
			defer cconsumer.CommitMessage(cmsg)
			ctx := context.Background()
			transaction := &GfSpan{}
			if nrapp != nil {
				ctx, transaction = k.nrapp.StartTransaction(ctx, "kafka/consumer")
			}
			correlation := uuid.New()
			for _, v := range msg.Headers {
				if v.Key == XCORRELATIONID && len(v.Value) > 0 {
					if id, err := uuid.Parse(string(v.Value)); err == nil {
						correlation = id
					}
				}
			}
			tm := k.monitoring.Start(correlation, k.groupId, TracingTypeConsumer)
			kafkaCallFnWithResilence(ctx, tm, cmsg, ckc, ckcs, cfn)
			tm.AddStack(100, "COMMITING MSG")
			consumer.CommitMessage(msg)
			tm.AddStack(100, "COMMIT SUCCESSFULLY")

			content := &map[string]interface{}{}
			if err := json.Unmarshal(msg.Value, content); err == nil {
				tm.AddContent(content)
			} else {
				tm.AddContent(msg.Value)
			}

			tm.End()

			if nrapp != nil {
				transaction.End()
			}
			<-messages

		}(msg, consumer, kc, *kcs, k.nrapp, fn)
	}
	done <- struct{}{}
}

func (k *GoKafka) ConsumerWithWorker(topic string,
	fn ConsumerFunc,
	cfg ConsumerMultiRoutineSettings) {
	messages := make(chan *kafka.Message, 100)
	done := make(chan struct{}, cfg.Routines)
	go func(topic string) {
		kcs := &KafkaConsumerSettings{
			Topic:           topic,
			AutoOffsetReset: cfg.AutoOffsetReset,
			Retries:         uint16(cfg.Retries),
		}
		kc := &kafka.ConfigMap{
			"bootstrap.servers":             k.server,
			"group.id":                      k.groupId,
			"auto.offset.reset":             kcs.AutoOffsetReset,
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
		fmt.Fprintf(os.Stdout,
			"%% Start consumer %s \n",
			k.groupId)
		CreateKafkaTopic(context.Background(), kc, &TopicConfiguration{
			Topic:             topic,
			NumPartitions:     cfg.Numpartitions,
			ReplicationFactor: cfg.ReplicationFactor,
		})
		consumer, err := kafka.NewConsumer(kc)
		if err != nil {
			log.Fatalln(err.Error())
			panic(err)
		}
		err = consumer.SubscribeTopics([]string{kcs.Topic}, rebalanceCallback)
		if err != nil {
			log.Fatalln(err.Error())
			panic(err)
		}

		for i := 0; i < cfg.Routines; i++ {
			go k.worker(i, messages, consumer, fn, kc, kcs, done)
		}

		for {
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			messages <- msg
		}
	}(topic)

}

func (k *GoKafka) ConsumerMultiRoutine(
	topic string,
	fn ConsumerFunc,
	cfg ConsumerMultiRoutineSettings) {
	go func(topic string) {
		kcs := &KafkaConsumerSettings{
			Topic:           topic,
			AutoOffsetReset: cfg.AutoOffsetReset,
			Retries:         uint16(cfg.Retries),
		}
		kc := &kafka.ConfigMap{
			"bootstrap.servers":             k.server,
			"group.id":                      k.groupId,
			"auto.offset.reset":             kcs.AutoOffsetReset,
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
		fmt.Fprintf(os.Stdout,
			"%% Start consumer %s \n",
			k.groupId)
		CreateKafkaTopic(context.Background(), kc, &TopicConfiguration{
			Topic:             topic,
			NumPartitions:     cfg.Numpartitions,
			ReplicationFactor: cfg.ReplicationFactor,
		})
		consumer, err := kafka.NewConsumer(kc)
		if err != nil {
			log.Fatalln(err.Error())
			panic(err)
		}
		err = consumer.SubscribeTopics([]string{kcs.Topic}, rebalanceCallback)
		if err != nil {
			log.Fatalln(err.Error())
			panic(err)
		}
		r := 0
		ptr_r := &r
		for {
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			*ptr_r++
			go func(cmsg *kafka.Message,
				cconsumer *kafka.Consumer,
				ckc *kafka.ConfigMap,
				ckcs KafkaConsumerSettings,
				nrapp GfAgentTelemetry,
				cfn ConsumerFunc) {
				defer recover_all()
				defer cconsumer.CommitMessage(cmsg)
				defer func() {
					*ptr_r--
				}()
				ctx := context.Background()
				transaction := &GfSpan{}
				if nrapp != nil {
					ctx, transaction = k.nrapp.StartTransaction(ctx, "kafka/consumer")
				}
				correlation := uuid.New()
				for _, v := range msg.Headers {
					if v.Key == XCORRELATIONID && len(v.Value) > 0 {
						if id, err := uuid.Parse(string(v.Value)); err == nil {
							correlation = id
						}
					}
				}
				tm := k.monitoring.Start(correlation, k.groupId, TracingTypeConsumer)
				kafkaCallFnWithResilence(ctx, tm, cmsg, ckc, ckcs, cfn)
				tm.AddStack(100, "COMMITING MSG")
				consumer.CommitMessage(msg)
				tm.AddStack(100, "COMMIT SUCCESSFULLY")

				content := &map[string]interface{}{}
				if err := json.Unmarshal(msg.Value, content); err == nil {
					tm.AddContent(content)
				} else {
					tm.AddContent(msg.Value)
				}

				tm.End()

				if nrapp != nil {
					transaction.End()
				}
			}(msg, consumer, kc, *kcs, k.nrapp, fn)
			wait_until(func() bool {
				return *ptr_r >= cfg.Routines
			})
		}
	}(topic)
}

func (k *GoKafka) Consumer(topic string, fn ConsumerFunc) {
	go func(topic string) {

		kcs := &KafkaConsumerSettings{
			Topic:           topic,
			AutoOffsetReset: "earliest",
			Retries:         5,
		}

		kc := &kafka.ConfigMap{
			"bootstrap.servers":             k.server,
			"group.id":                      k.groupId,
			"auto.offset.reset":             kcs.AutoOffsetReset,
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

		fmt.Fprintf(os.Stdout,
			"%% Start consumer %s \n",
			k.groupId)

		consumer, err := kafka.NewConsumer(kc)
		if err != nil {
			log.Fatalln(err.Error())
			panic(err)
		}

		err = consumer.SubscribeTopics([]string{kcs.Topic}, rebalanceCallback)
		if err != nil {
			log.Fatalln(err.Error())
			panic(err)
		}

		for {
			msg, err := consumer.ReadMessage(-1)

			ctx := context.Background()
			transaction := &GfSpan{}
			if k.nrapp != nil {
				ctx, transaction = k.nrapp.StartTransaction(ctx, "kafka/consumer")
			}

			if err != nil {
				log.Println(err.Error())
				continue
			}

			correlation := uuid.New()
			for _, v := range msg.Headers {
				if v.Key == XCORRELATIONID && len(v.Value) > 0 {
					if id, err := uuid.Parse(string(v.Value)); err == nil {
						correlation = id
					}
					break
				}
			}

			tm := k.monitoring.Start(correlation, k.groupId, TracingTypeConsumer)
			kafkaCallFnWithResilence(ctx, tm, msg, kc, *kcs, fn)
			tm.AddStack(100, "COMMITING MSG")
			consumer.CommitMessage(msg)
			tm.AddStack(100, "COMMIT SUCCESSFULLY")

			content := &map[string]interface{}{}
			if err := json.Unmarshal(msg.Value, content); err == nil {
				tm.AddContent(content)
			} else {
				tm.AddContent(msg.Value)
			}

			tm.End()

			if k.nrapp != nil {
				transaction.End()
			}
		}

	}(topic)
}

func (k *GoKafka) ConsumerWithSettings(topic string, fn ConsumerFunc, cs ConsumerSettings) {
	go func(topic string) {

		kcs := &KafkaConsumerSettings{
			Topic:           topic,
			AutoOffsetReset: cs.AutoOffsetReset,
			Retries:         uint16(cs.Retries),
		}

		kc := &kafka.ConfigMap{
			"bootstrap.servers":             k.server,
			"group.id":                      k.groupId,
			"auto.offset.reset":             kcs.AutoOffsetReset,
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

		fmt.Fprintf(os.Stdout,
			"%% Start consumer %s \n",
			k.groupId)

		consumer, err := kafka.NewConsumer(kc)
		if err != nil {
			log.Fatalln(err.Error())
			panic(err)
		}

		err = consumer.SubscribeTopics([]string{kcs.Topic}, rebalanceCallback)
		if err != nil {
			log.Fatalln(err.Error())
			panic(err)
		}

		for {
			msg, err := consumer.ReadMessage(-1)

			ctx := context.Background()
			transaction := &GfSpan{}
			if k.nrapp != nil {
				ctx, transaction = k.nrapp.StartTransaction(ctx, "kafka/consumer")
			}

			if err != nil {
				log.Println(err.Error())
				continue
			}

			correlation := uuid.New()
			for _, v := range msg.Headers {
				if v.Key == XCORRELATIONID && len(v.Value) > 0 {
					if id, err := uuid.Parse(string(v.Value)); err == nil {
						correlation = id
					}
					break
				}
			}

			tm := k.monitoring.Start(correlation, k.groupId, TracingTypeConsumer)
			kafkaCallFnWithResilence(ctx, tm, msg, kc, *kcs, fn)
			tm.AddStack(100, "COMMITING MSG")
			consumer.CommitMessage(msg)
			tm.AddStack(100, "COMMIT SUCCESSFULLY")

			content := &map[string]interface{}{}
			if err := json.Unmarshal(msg.Value, content); err == nil {
				tm.AddContent(content)
			} else {
				tm.AddContent(msg.Value)
			}

			tm.End()

			if k.nrapp != nil {
				transaction.End()
			}
		}

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

package goframework

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/newrelic/go-agent/v3/newrelic"
)

type (
	KafkaProducerSettings struct {
		Topic             string
		NumPartitions     int
		ReplicationFactor int

		Partition    int32
		Offset       kafka.Offset
		TimeoutFlush int
	}

	KafkaProducer[T interface{}] struct {
		kcs *KafkaProducerSettings
		kcm *kafka.ConfigMap
		k   *GoKafka
		kp  *kafka.Producer
	}
)

func NewKafkaProducer[T interface{}](k *GoKafka,
	kcs *KafkaProducerSettings,
) Producer[T] {

	hostname, _ := os.Hostname()

	kcm := &kafka.ConfigMap{
		"bootstrap.servers": k.server,
		"client.id":         hostname,
		"acks":              "all",
	}

	if len(k.securityprotocol) > 0 {
		kcm.SetKey("security.protocol", k.securityprotocol)
		if k.securityprotocol == "SASL_SSL" {
			kcm.SetKey("enable.ssl.certificate.verification", true)
		}
	}

	if len(k.saslmechanism) > 0 {
		kcm.SetKey("sasl.mechanism", k.saslmechanism)
	}

	if len(k.saslusername) > 0 {
		kcm.SetKey("sasl.username", k.saslusername)
	}

	if len(k.saslpassword) > 0 {
		kcm.SetKey("sasl.password", k.saslpassword)
	}

	CreateKafkaTopic(context.Background(), kcm, &TopicConfiguration{
		Topic:             kcs.Topic,
		NumPartitions:     kcs.NumPartitions,
		ReplicationFactor: kcs.ReplicationFactor,
	})
	kp, err := kafka.NewProducer(kcm)
	if err != nil {
		return nil
	}

	// go func() {
	// 	for e := range kp.Events() {
	// 		switch ev := e.(type) {
	// 		case *kafka.Message:
	// 			if ev.TopicPartition.Error != nil {
	// 				fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
	// 			} else {
	// 				fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
	// 					*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
	// 			}
	// 		}
	// 	}
	// }()

	return &KafkaProducer[T]{
		kcs: kcs,
		kcm: kcm,
		k:   k,
		kp:  kp,
	}
}

func (kp *KafkaProducer[T]) PublishWithKey(ctx context.Context, key []byte, msgs ...*T) error {
	txn := newrelic.FromContext(ctx)
	nrSegment := txn.StartSegment(kp.kcs.Topic)
	nrSegment.AddAttribute("span.kind", "client")
	defer nrSegment.End()

	headers := helperContextKafka(ctx,
		[]string{
			XTENANTID,
			XAUTHOR,
			XAUTHORID,
			XCORRELATIONID,
			XCREATEDAT,
			XREADERS,
			XNOTREADERS,
			XEDITORS,
		})

	for _, m := range msgs {
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		delivery_chan := make(chan kafka.Event)
		if err = kp.kp.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &kp.kcs.Topic,
				Partition: kafka.PartitionAny,
				Offset:    kp.kcs.Offset,
			},
			Key:     key,
			Value:   data,
			Headers: headers.ToKafkaHeader()}, delivery_chan); err != nil {
			return err
		}
		<-delivery_chan

		go func() {
			for e := range kp.kp.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						log.Fatalf("Failed to deliver message: %v\n", ev.TopicPartition)
					} else {
						log.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
							*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
				}
			}
			defer kp.kp.Close()
		}()

	}

	return nil
}

type baseStruct struct {
	Id interface{}
}

func (kp *KafkaProducer[T]) Publish(ctx context.Context, msgs ...*T) error {

	txn := newrelic.FromContext(ctx)
	nrSegment := txn.StartSegment(kp.kcs.Topic)
	nrSegment.AddAttribute("span.kind", "client")
	defer nrSegment.End()

	headers := helperContextKafka(ctx,
		[]string{
			XTENANTID,
			XAUTHOR,
			XAUTHORID,
			XCORRELATIONID,
			XCREATEDAT,
			XREADERS,
			XNOTREADERS,
			XEDITORS,
		})

	for _, m := range msgs {

		data, err := json.Marshal(m)
		if err != nil {
			return err
		}

		bId := []byte(uuid.NewString())
		var basestruct baseStruct
		if err := json.Unmarshal(data, &basestruct); err == nil {
			switch t := basestruct.Id.(type) {
			case uuid.UUID:
				bId = t.NodeID()
			case string:
				bId = []byte(t)
			}
		}

		delivery_chan := make(chan kafka.Event)
		if err = kp.kp.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{
			Topic:     &kp.kcs.Topic,
			Partition: kafka.PartitionAny,
			Offset:    kp.kcs.Offset,
		},
			Value:   data,
			Headers: headers.ToKafkaHeader(),
			Key:     bId}, delivery_chan); err != nil {
			fmt.Println(err.Error())
			return err
		}
		<-delivery_chan
	}

	return nil
}

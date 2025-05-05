package goframework

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/newrelic/go-agent/v3/newrelic"
)

type (
	KafkaProducer struct {
		kp *kafka.Producer
	}
)

func NewKafkaProducer(k *GoKafka) Producer {

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

	kp, err := kafka.NewProducer(kcm)
	if err != nil {
		return nil
	}

	go func() {
		for e := range kp.Events() {
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
		defer kp.Close()
	}()

	return &KafkaProducer{
		kp: kp,
	}
}

type baseStruct struct {
	Id interface{}
}

func (kp *KafkaProducer) Publish(ctx context.Context, tp string, msg any) error {

	txn := newrelic.FromContext(ctx)
	nrSegment := txn.StartSegment(tp)
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

	data, err := json.Marshal(msg)
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
	if err = kp.kp.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &tp,
			Partition: kafka.PartitionAny,
			Offset:    kafka.OffsetEnd,
		},
		Value:   data,
		Headers: headers.ToKafkaHeader(),
		Key:     bId,
	}, delivery_chan); err != nil {
		fmt.Println(err.Error())
		return err
	}
	<-delivery_chan

	return nil
}

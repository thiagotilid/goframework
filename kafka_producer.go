package goframework

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type (
	KafkaProducer struct {
		server string
		kp     *kafka.Producer
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
		kp:     kp,
		server: k.server,
	}
}

type baseStruct struct {
	Id interface{}
}

func (kp *KafkaProducer) Publish(ctx context.Context, tp string, msg any) error {

	mCtx := getContext(ctx)
	tracer := otel.Tracer("")
	tctx, span := tracer.Start(mCtx, fmt.Sprintf("KAFKA PUB %s", tp),
		trace.WithAttributes(attribute.String("messaging.system", "kafka")),
		trace.WithAttributes(attribute.String("messaging.destination.name", tp)),
	)
	defer span.End()

	headers := helperContextKafka(ctx,
		[]string{
			XTENANTID,
			XAUTHOR,
			XAUTHORID,
			XCORRELATIONID,
			XCREATEDAT,
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

	kHeader := headers.ToKafkaHeader()
	carrier := kafkaHeaderCarrier{&kHeader}
	otel.GetTextMapPropagator().Inject(tctx, carrier)

	delivery_chan := make(chan kafka.Event)
	if err = kp.kp.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &tp,
			Partition: kafka.PartitionAny,
			Offset:    kafka.OffsetEnd,
		},
		Value:   data,
		Headers: kHeader,
		Key:     bId,
	}, delivery_chan); err != nil {
		span.RecordError(err)
		fmt.Println(err.Error())
		return err
	}
	span.AddEvent("Message sent successfully")
	<-delivery_chan

	return nil
}

func (kp *KafkaProducer) PublishWithKey(ctx context.Context, tp string, key []byte, msg any) error {

	tracer := otel.Tracer("")
	mCtx := getContext(ctx)
	tctx, span := tracer.Start(mCtx, fmt.Sprintf("KAFKA PUB %s", tp),
		trace.WithAttributes(attribute.String("messaging.system", "kafka")),
		trace.WithAttributes(attribute.String("messaging.destination.name", tp)),
		trace.WithAttributes(attribute.String("messaging.kafka.message.key", string(key))),
	)
	defer span.End()

	headers := helperContextKafka(ctx,
		[]string{
			XTENANTID,
			XAUTHOR,
			XAUTHORID,
			XCORRELATIONID,
			XCREATEDAT,
		})

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	kHeader := headers.ToKafkaHeader()
	carrier := kafkaHeaderCarrier{&kHeader}
	otel.GetTextMapPropagator().Inject(tctx, carrier)

	delivery_chan := make(chan kafka.Event)
	if err = kp.kp.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &tp,
			Partition: kafka.PartitionAny,
			Offset:    kafka.OffsetEnd,
		},
		Value:   data,
		Headers: kHeader,
		Key:     key,
	}, delivery_chan); err != nil {
		span.RecordError(err)
		fmt.Println(err.Error())
		return err
	}
	span.AddEvent("Message sent successfully")
	<-delivery_chan

	return nil
}

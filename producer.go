package goframework

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type (
	ProducerSettings struct {
		Topic             string
		NumPartitions     int
		ReplicationFactor int

		Partition    int32
		Offset       kafka.Offset
		TimeoutFlush int
	}

	TopicProducer[T any] struct {
		producer Producer
		settings *ProducerSettings
	}
)

func NewTopicProducer[T any](p Producer, ps *ProducerSettings) *TopicProducer[T] {
	return &TopicProducer[T]{
		producer: p,
		settings: ps,
	}
}

func (kp *TopicProducer[T]) Publish(ctx context.Context, msg *T) error {
	return kp.producer.Publish(ctx, kp.settings.Topic, msg)
}

func (kp *TopicProducer[T]) PublishWithKey(ctx context.Context, key []byte, msg *T) error {
	return kp.producer.PublishWithKey(ctx, kp.settings.Topic, key, msg)
}

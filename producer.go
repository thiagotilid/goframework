package goframework

import (
	"context"
)

type (
	TopicProducer[T any] struct {
		producer Producer
		topic    string
	}
)

func NewTopicProducer[T any](p Producer, t string) *TopicProducer[T] {
	return &TopicProducer[T]{
		producer: p,
		topic:    t,
	}
}

func (kp *TopicProducer[T]) Publish(ctx context.Context, msg *T) error {
	return kp.producer.Publish(ctx, kp.topic, msg)
}

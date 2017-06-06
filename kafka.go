package msgqueue

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

// Kafka stores the necessary values to setup of Kafka consumer and/or producer
type Kafka struct {
	Brokers        []string
	ConsumerTopics []string
	ConsumerGroup  string
	OffsetInitial  string
}

//KafkaConsumer is a sarama-cluster Consumer
type KafkaConsumer struct {
	Consumer *cluster.Consumer
}

// KafkaProducer is a sarama AsyncProducer
type KafkaProducer struct {
	Producer sarama.AsyncProducer
}

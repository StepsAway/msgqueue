package msgqueue

import (
	"encoding/json"
	"github.com/bsm/sarama-cluster"
	"log"
	"github.com/Shopify/sarama"
)

// SetupConsumer initializes a new consumer
func (k *Kafka) SetupConsumer() *KafkaConsumer {
	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// Set offset
	if k.OffsetInitial == "OffsetOldest" {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	// init consumer
	consumer, err := cluster.NewConsumer(k.Brokers, k.ConsumerGroup, k.ConsumerTopics, config)
	if err != nil {
		log.Fatalln("Failed to create Kafka consumer:", err)
	}

	return &KafkaConsumer{Consumer: consumer}
}

// ParseActivityLog takes a string of bytes and returns json with type interface{}
// This is designed to handle dynamic json structure
func ParseActivityLog(msg *[]byte) interface{} {
	var j interface{}
	err := json.Unmarshal(*msg, &j)
	if err != nil {
		log.Printf("Failed to parse value %v\n Error: %v\n", *msg, err)
	}
	return j
}

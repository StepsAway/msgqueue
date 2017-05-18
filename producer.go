package msgqueue

import (
	"github.com/Shopify/sarama"
	"log"
	"strconv"
	"time"
)

//ProducerMessage defines the fields for creating a message to be consumed by a producer
type ProducerMessage struct {
	Topic   string
	Message string
}

//SetupProducer initializes a new consumer
func (k *Kafka) SetupProducer() *KafkaProducer {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to ack
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms

	producer, err := sarama.NewAsyncProducer(k.Brokers, config)
	if err != nil {
		log.Fatalln("Failed to create Kafka producer:", err)
	}
	return &KafkaProducer{Producer: producer}
}

//ProduceMessage creates a message that can be sent to AsyncProducer Input
func (k *Kafka) ProduceMessage(message *ProducerMessage) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{Topic: message.Topic, Value: sarama.StringEncoder(message.Message)}
}

//GenerateTopic Unmarshaled JSON and extracts the app name and hour to create a topic
func GenerateTopic(msg interface{}) string {
	msgmap := msg.(map[string]interface{})
	t := time.Unix(int64(msgmap["time"].(float64)), 0)
	h := strconv.Itoa(t.Hour())
	return msgmap["app"].(string) + h
}

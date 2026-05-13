package consumer

import "github.com/segmentio/kafka-go"

func InitKafkaConsumer() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "execute-stage",
	})
}

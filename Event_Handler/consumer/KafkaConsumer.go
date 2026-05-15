package consumer

import (
	"Shared/config"

	"github.com/segmentio/kafka-go"
)

func InitKafkaConsumer(cfg config.Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.KafkaExecuteTopic,
		GroupID:  "event-handler-workers",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
}

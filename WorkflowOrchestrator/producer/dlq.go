package producer

import (
	"time"

	"Shared/config"

	"github.com/segmentio/kafka-go"
)

func InitDLQProducer(cfg config.Config) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaDLQTopic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
}

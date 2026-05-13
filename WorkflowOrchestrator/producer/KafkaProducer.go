package producer

import (
	"time"

	"github.com/segmentio/kafka-go"
)

func InitKafkaProducer() *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092"),
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		Topic:        "execute-stage",
	}
}

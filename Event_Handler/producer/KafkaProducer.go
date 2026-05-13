package producer

import (
	"Shared/shared_models"
	"context"
	"encoding/json"
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
		Topic:        "execution-response",
	}
}

func SendMessage(producer *kafka.Writer, response shared_models.OperationResponse) error {
	message, err := serializeMessage(response)
	if err != nil {
		return err
	}

	return producer.WriteMessages(context.Background(),
		kafka.Message{
			Value: message,
		},
	)
}

func serializeMessage(response shared_models.OperationResponse) ([]byte, error) {
	return json.Marshal(response)
}

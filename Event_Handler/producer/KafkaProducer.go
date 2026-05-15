package producer

import (
	"Shared/shared_models"
	"context"
	"encoding/json"
	"time"

	"Shared/config"
	"Shared/metrics"

	"github.com/segmentio/kafka-go"
)

const serviceName = "event-handler"

func InitKafkaProducer(cfg config.Config) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaResponseTopic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
}

func SendMessage(ctx context.Context, producer *kafka.Writer, response shared_models.OperationResponse) error {
	message, err := json.Marshal(response)
	if err != nil {
		return err
	}
	if err := producer.WriteMessages(ctx, kafka.Message{Value: message}); err != nil {
		return err
	}
	metrics.KafkaProduced.WithLabelValues(serviceName, producer.Topic).Inc()
	return nil
}

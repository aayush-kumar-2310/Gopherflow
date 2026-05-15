package producer

import (
	"context"
	"time"

	"Shared/config"
	"Shared/metrics"

	"github.com/segmentio/kafka-go"
)

const serviceName = "workflow-orchestrator"

func InitKafkaProducer(cfg config.Config) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaExecuteTopic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
}

func WriteExecuteMessage(ctx context.Context, w *kafka.Writer, msg kafka.Message) error {
	if err := w.WriteMessages(ctx, msg); err != nil {
		return err
	}
	metrics.KafkaProduced.WithLabelValues(serviceName, w.Topic).Inc()
	return nil
}

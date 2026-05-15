package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"Shared/metrics"

	"github.com/segmentio/kafka-go"
)

// Publish writes a DLQ record to Kafka.
func Publish(ctx context.Context, writer *kafka.Writer, service string, msg Message) error {
	if writer == nil {
		return fmt.Errorf("dlq writer is nil")
	}
	if msg.At.IsZero() {
		msg.At = time.Now().UTC()
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msg.WorkflowID + ":" + msg.StageID),
		Value: body,
	})
	if err != nil {
		return err
	}
	metrics.DLQPublished.WithLabelValues(service, msg.Reason).Inc()
	metrics.KafkaProduced.WithLabelValues(service, writer.Topic).Inc()
	return nil
}

package worker

import (
	"Shared/shared_models"
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

func ConsumeStageResult(kafkaConsumer *kafka.Reader) {
	for {
		msg, err := kafkaConsumer.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
		}
		log.Printf("Received message: %s", string(msg.Value))

	}
}

func DeserializeMessageToObject(message kafka.Message) shared_models.OperationResponse {
	var response shared_models.OperationResponse

	err := json.Unmarshal(message.Value, &response)
	if err != nil {
		log.Printf("Error unmarshaling message: %v", err)
	}
	return response
}

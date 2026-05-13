package executors

import (
	"Event_Handler/producer"
	"Shared/shared_models"
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
)

func ConsumeExecuteStageMessages(kafkaConsumer *kafka.Reader, kafkaProducer *kafka.Writer) {
	for {
		msg, err := kafkaConsumer.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}
		log.Printf("Received message: %s", string(msg.Value))
		currStage := mapKafkaMessageToStageObject(msg)
		log.Printf("Mapped message to stage object: %+v", currStage)
		response := executeStage(currStage, kafkaProducer)
		log.Printf("Execution response: %+v", response)
	}
}

func mapKafkaMessageToStageObject(msg kafka.Message) shared_models.OperationRequest {
	var currStage shared_models.OperationRequest
	err := json.Unmarshal(msg.Value, &currStage)
	if err != nil {
		log.Printf("Error unmarshaling message: %v", err)
	}
	return currStage
}

func executeStage(stage shared_models.OperationRequest, kafkaProducer *kafka.Writer) shared_models.OperationResponse {
	var response shared_models.OperationResponse
	switch stage.StageType {
	case shared_models.StageType("EXECUTE_SCRIPT"):
		response = ExecuteScriptStage(stage)
	default:
		log.Printf("Unsupported stage type: %s", stage.StageType)
	}

	handleStageResponse(response, kafkaProducer)
	return response
}

func handleStageResponse(response shared_models.OperationResponse, kafkaProducer *kafka.Writer) {
	log.Printf("Handling stage response: %+v", response)
	producer.SendMessage(kafkaProducer, response)
}

package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"workflow-orchestrator/models"

	"Shared/shared_models"

	redis "github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

func ExecuteJob(stageId, workflowId string, db *gorm.DB, redisClient *redis.Client, kafkaProducer *kafka.Writer, requestId string) {
	fmt.Println("Executing stage job:", stageId)

	stageInfo, err := fetchStageDetailsFromDB(stageId, workflowId, db)
	if err != nil {
		fmt.Printf("Failed to fetch stage details for %s: %v\n", stageId, err)
		return
	}

	operationRequest := mapStageDetailsToOperationRequest(stageInfo)
	operationRequest.RequestId = requestId
	operationRequestJson, err := json.Marshal(operationRequest)
	if err != nil {
		fmt.Printf("Failed to marshal operation request for stage %s: %v\n", stageId, err)
		return
	}

	fmt.Println("Details received for: ", stageId+": ", stageInfo)

	err = kafkaProducer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(stageId),
		Value: operationRequestJson,
	})
	if err != nil {
		fmt.Printf("Failed to send message to Kafka for stage %s: %v\n", stageId, err)
		return
	}

	fmt.Printf("Stage %s execution request sent to Kafka successfully\n", stageId)
}

func fetchStageDetailsFromDB(stageId string, workflowId string, db *gorm.DB) (models.Stage, error) {
	var stage models.Stage
	err := db.Where("stage_id = ? AND workflow_id = ?", stageId, workflowId).First(&stage).Error
	return stage, err
}

func mapStageDetailsToOperationRequest(stage models.Stage) shared_models.OperationRequest {
	return shared_models.OperationRequest{
		WorkflowId:     stage.WorkflowId,
		StageId:        stage.StageId,
		StageType:      shared_models.StageType(stage.Operation),
		Script:         stage.Script,
		RemoteFilePath: stage.RemoteFilePath,
		LocalFilePath:  stage.LocalFilePath,
		HttpEndpoint:   stage.HttpEndpoint,
		HttpMethod:     stage.HttpMethod,
		HttpPayload:    stage.HttpPayload,
	}
}

package worker

import (
	"context"
	"fmt"
	"strings"
	"time"
	"workflow-orchestrator/executors"
	"workflow-orchestrator/models"

	redis "github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

func ExecuteJob(stageId, workflowId string, db *gorm.DB, redisClient *redis.Client) {
	resultChannel := make(chan models.JobResult, 1)
	// Here we would parse the stage information and execute the corresponding task
	fmt.Println("Executing stage job:", stageId)

	stageInfo, err := fetchStageDetailsFromDB(stageId, workflowId, db)
	if err != nil {
		fmt.Printf("Failed to fetch stage details for %s: %v\n", stageId, err)
		return
	}

	fmt.Println("Details received for: ", stageId+": ", stageInfo)

	switch stageInfo.Operation {
	case models.ExecuteScript:
		fmt.Printf("Executing script for stage %s\n", stageId)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		go executors.ExecutePythonScript(stageInfo.Script, resultChannel, ctx)
		select {
		case res := <-resultChannel:
			if res.Error != "" {
				fmt.Printf("Error executing stage %s: %s\n", stageId, res.Error)
			} else {
				fmt.Printf("Output of stage %s: %s\n", stageId, res.Output)
			}
		case <-time.After(30 * time.Second):
			fmt.Printf("Timeout executing stage %s\n", stageId)
		}
	default:
		fmt.Printf("Unknown operation for stage %s\n", stageId)
	}

}

func splitStageKey(key string) (string, string) {
	parts := strings.Split(key, ":")
	if len(parts) < 2 {
		return "", ""
	}
	return parts[0], parts[1] // stageId, workflowId
}

func fetchStageDetailsFromDB(stageId string, workflowId string, db *gorm.DB) (models.Stage, error) {
	var stage models.Stage
	err := db.Where("stage_id = ? AND workflow_id = ?", stageId, workflowId).First(&stage).Error
	return stage, err
}

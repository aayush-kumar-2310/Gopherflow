package worker

import (
	"context"
	"fmt"
	"strings"
	"time"
	"workflow-orchestrator/models"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

func StartRedisPoller(ctx context.Context, db *gorm.DB, redisClient *redis.Client, kafkaProducer *kafka.Writer) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			now := time.Now().Unix()
			fmt.Println("Polling: ", now)
			// 1. Handle Workflow Triggers (Cron Pulse)
			handleWorkflowTriggers(ctx, now, db, redisClient)
			// This MUST be independent so it can pick up delayed stages
			handleStageTriggers(ctx, now, db, redisClient, kafkaProducer)
		case <-ctx.Done():
			return
		}
	}
}

func handleWorkflowTriggers(ctx context.Context, now int64, db *gorm.DB, redisClient *redis.Client) {
	// 1. Find workflows due
	workflows, err := redisClient.ZRangeByScoreWithScores(
		ctx,
		"workflow-trigger",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: fmt.Sprintf("%d", now),
		}).Result()
	if err != nil {
		fmt.Printf("Error fetching workflow triggers from Redis: %v\n", err)
		return
	}

	for _, wfKey := range workflows {
		// 2. THE CLAIM: Only proceed if we can remove it
		if n, _ := redisClient.ZRem(ctx, "workflow-trigger", wfKey.Member).Result(); n > 0 {
			fmt.Printf("Initializing Workflow: %v\n", wfKey.Member)
			workflowId := strings.Split(wfKey.Member.(string), ":")[0]
			fmt.Printf("Parsed Workflow Key - WorkflowID: %s\n", workflowId)
			go InitializeWorkflow(workflowId, db, redisClient)
			// Note: We don't reschedule here. The workflow initializer will handle that after completion.
			// This ensures that if the workflow execution takes longer than the cron interval, we won't have overlapping runs.
			// Rescheduling in the initializer also allows us to adjust the next run time based on actual completion time, if needed.

		}
	}
}

func InitializeWorkflow(workflowId string, db *gorm.DB, redisClient *redis.Client) {
	// Fetch workflow details from DB using workflowId
	// For each stage in the workflow, push a job trigger to Redis with the stage details
	fmt.Printf("Fetching workflow details for WorkflowID: %s\n", workflowId)

	var workflow models.Workflow
	db.Model(&models.Workflow{}).Where("workflow_id = ?", workflowId).Preload("Stages").First(&workflow)

	fmt.Printf("Workflow details fetched: %v\n", workflow)

	for _, stage := range workflow.Stages {
		if len(stage.DependentOn) == 0 {
			requestId := uuid.New().String()
			key := stage.StageId + ":" + workflow.WorkflowId + ":" + requestId
			_, err := redisClient.ZAdd(
				context.Background(),
				"job-trigger",
				redis.Z{
					Member: key,
					Score:  float64(time.Now().Unix()),
				},
			).Result()

			if err != nil {
				fmt.Printf("Error scheduling job in Redis: %v\n", err)
			}
		}

	}

	schedule, err := cron.ParseStandard(workflow.CronExpression)
	if err != nil {
		fmt.Printf("Error parsing cron expression for workflow %s: %v\n", workflow.WorkflowName, err)
		return
	}
	nextRun := schedule.Next(time.Now())

	fmt.Printf("Rescheduling workflow %s with cron expression: %s for next run at %v\n", workflow.WorkflowName, workflow.CronExpression, nextRun)

	// Reschedule the workflow for its next run based on its cron expression
	_, err = redisClient.ZAdd(
		context.Background(),
		"workflow-trigger",
		redis.Z{
			Member: workflow.WorkflowId + ":" + uuid.New().String(),
			Score:  float64(nextRun.Unix()),
		},
	).Result()

	if err != nil {
		fmt.Printf("Error rescheduling workflow in Redis: %v\n", err)
	}
}

func handleStageTriggers(ctx context.Context, now int64, db *gorm.DB, redisClient *redis.Client, kafkaProducer *kafka.Writer) {
	stages, err := redisClient.ZRangeByScoreWithScores(
		ctx,
		"job-trigger",
		&redis.ZRangeBy{
			Min: "-inf",
			Max: fmt.Sprintf("%d", now),
		}).Result()

	if err != nil {
		fmt.Printf("Error fetching stage triggers from Redis: %v\n", err)
		return
	}

	for _, stageKey := range stages {
		// 3. THE CLAIM: Prevents duplicate execution
		if n, _ := redisClient.ZRem(ctx, "job-trigger", stageKey.Member).Result(); n > 0 {
			fmt.Printf("Executing Stage: %v\n", stageKey.Member)
			stageId, workflowId, _ := splitKey(stageKey.Member.(string))
			fmt.Printf("Parsed Stage Key - StageID: %s, WorkflowID: %s\n", stageId, workflowId)
			go ExecuteJob(stageId, workflowId, db, redisClient, kafkaProducer)
		}
	}
}

func splitKey(jobKey string) (string, string, string) {
	parts := strings.Split(jobKey, ":")
	if len(parts) < 3 {
		fmt.Printf("Invalid stage job format: %s\n", jobKey)
		return "", "", ""
	}
	return parts[0], parts[1], parts[2]
}

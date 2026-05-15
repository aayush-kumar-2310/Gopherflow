package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
	"workflow-orchestrator/models"
	"workflow-orchestrator/producer"

	"Shared/config"
	"Shared/ctxutil"
	"Shared/dlq"
	"Shared/metrics"
	"Shared/shared_models"

	redis "github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

func ExecuteJob(
	ctx context.Context,
	cfg config.Config,
	stageId, workflowId string,
	db *gorm.DB,
	redisClient *redis.Client,
	kafkaProducer *kafka.Writer,
	dlqWriter *kafka.Writer,
	runId string,
) {
	log := slog.With("workflow_id", workflowId, "run_id", runId, "stage_id", stageId)

	attempt, err := IncrementAttempt(ctx, redisClient, workflowId, runId, stageId)
	if err != nil {
		log.Error("increment attempt", "error", err)
		return
	}

	_ = SetStageStatus(ctx, redisClient, workflowId, runId, stageId, shared_models.StageRunning)

	dbCtx, cancel := ctxutil.WithDBTimeout(ctx, cfg)
	stageInfo, err := fetchStageDetailsFromDB(dbCtx, stageId, workflowId, db)
	cancel()
	if err != nil {
		log.Error("fetch stage", "error", err)
		_ = SetStageStatus(ctx, redisClient, workflowId, runId, stageId, shared_models.StageFailedExhausted)
		publishDLQ(ctx, dlqWriter, workflowId, runId, stageId, "", attempt, err.Error(), dlq.ReasonDispatchFailed)
		recordTerminalStage(ctx, cfg, db, redisClient, workflowId, runId)
		return
	}

	parentOutputs := make(map[string]string)
	for _, parentStage := range stageInfo.DependentOn {
		output, err := redisClient.Get(ctx, outputKey(workflowId, parentStage, runId)).Result()
		if err == nil {
			parentOutputs[parentStage] = output
		}
	}

	weight := stageInfo.Weight
	if weight < 1 {
		weight = 1
	}

	req := mapStageDetailsToOperationRequest(stageInfo, runId, attempt, weight)
	req.ParentOutputs = parentOutputs

	payload, err := json.Marshal(req)
	if err != nil {
		log.Error("marshal operation request", "error", err)
		return
	}

	kafkaCtx, kafkaCancel := context.WithTimeout(ctx, 10*time.Second)
	defer kafkaCancel()

	if err := producer.WriteExecuteMessage(kafkaCtx, kafkaProducer, kafka.Message{
		Key:   []byte(stageId),
		Value: payload,
	}); err != nil {
		log.Warn("kafka publish failed, scheduling retry", "error", err)
		metrics.StageRetries.WithLabelValues("workflow-orchestrator").Inc()
		_ = SetStageStatus(ctx, redisClient, workflowId, runId, stageId, shared_models.StageFailedRetry)
		_ = ScheduleStage(ctx, redisClient, stageId, workflowId, runId, weight)
		return
	}

	metrics.StagesDispatched.WithLabelValues("workflow-orchestrator").Inc()
	log.Info("stage dispatched", "attempt", attempt, "stage_type", stageInfo.Operation)
}

func publishDLQ(ctx context.Context, writer *kafka.Writer, workflowID, runID, stageID, stageType string, attempt int, errMsg, reason string) {
	if writer == nil {
		return
	}
	dlqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = dlq.Publish(dlqCtx, writer, "workflow-orchestrator", dlq.Message{
		WorkflowID: workflowID,
		RunID:      runID,
		StageID:    stageID,
		StageType:  stageType,
		Attempt:    attempt,
		Error:      errMsg,
		Reason:     reason,
		Source:     "workflow-orchestrator",
	})
}

func fetchStageDetailsFromDB(ctx context.Context, stageId, workflowId string, db *gorm.DB) (models.Stage, error) {
	var stage models.Stage
	err := db.WithContext(ctx).Where("stage_id = ? AND workflow_id = ?", stageId, workflowId).First(&stage).Error
	return stage, err
}

func mapStageDetailsToOperationRequest(stage models.Stage, runId string, attempt, weight int) shared_models.OperationRequest {
	return shared_models.OperationRequest{
		RequestId:      runId,
		WorkflowId:     stage.WorkflowId,
		StageId:        stage.StageId,
		StageType:      shared_models.StageType(stage.Operation),
		Attempt:        attempt,
		Weight:         weight,
		Script:         stage.Script,
		SFTPHost:       stage.SFTPHost,
		SFTPPort:       stage.SFTPPort,
		SFTPUsername:   stage.SFTPUsername,
		SFTPPassword:   stage.SFTPPassword,
		RemoteFilePath: stage.RemoteFilePath,
		LocalFilePath:  stage.LocalFilePath,
		HttpEndpoint:   stage.HttpEndpoint,
		HttpMethod:     stage.HttpMethod,
		HttpPayload:    stage.HttpPayload,
		HttpHeaders:    stage.HttpHeaders,
		Prompt:         stage.Prompt,
		Context:        stage.Context,
	}
}

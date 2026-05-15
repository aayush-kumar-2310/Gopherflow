package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
	"workflow-orchestrator/models"
	"workflow-orchestrator/notify"

	"Shared/config"
	"Shared/ctxutil"
	"Shared/dlq"
	"Shared/metrics"
	"Shared/shared_models"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

func ConsumeStageResult(
	ctx context.Context,
	cfg config.Config,
	kafkaConsumer *kafka.Reader,
	redisClient *redis.Client,
	db *gorm.DB,
	dlqWriter *kafka.Writer,
	pool *Pool,
) {
	topic := kafkaConsumer.Config().Topic
	for {
		if ctx.Err() != nil {
			slog.Info("kafka result consumer stopped")
			return
		}

		readCtx, cancel := context.WithTimeout(ctx, cfg.KafkaReadTimeout)
		msg, err := kafkaConsumer.ReadMessage(readCtx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if readCtx.Err() == context.DeadlineExceeded {
				continue
			}
			slog.Warn("kafka read error", "error", err)
			continue
		}

		metrics.KafkaConsumed.WithLabelValues("workflow-orchestrator", topic).Inc()
		resp := deserializeMessage(msg)
		if resp.WorkflowId == "" || resp.StageId == "" || resp.RequestId == "" {
			slog.Warn("invalid result message", "raw", string(msg.Value))
			continue
		}

		processStageResult(ctx, cfg, db, redisClient, dlqWriter, pool, resp)
	}
}

func processStageResult(
	ctx context.Context,
	cfg config.Config,
	db *gorm.DB,
	rdb *redis.Client,
	dlqWriter *kafka.Writer,
	pool *Pool,
	resp shared_models.OperationResponse,
) {
	attempt := resp.Attempt
	if attempt < 1 {
		attempt = 1
	}

	first, err := MarkResultSeen(ctx, rdb, resp.WorkflowId, resp.RequestId, resp.StageId, attempt)
	if err != nil || !first {
		return
	}

	weights, _ := loadStageWeights(ctx, rdb, resp.WorkflowId, resp.RequestId)
	weight := weights[resp.StageId]
	if weight < 1 {
		weight = 1
	}

	if resp.Status == "SUCCESS" {
		handleStageSuccess(ctx, cfg, db, rdb, resp, weights)
		return
	}

	handleStageFailure(ctx, cfg, db, rdb, dlqWriter, pool, resp, weight)
}

func handleStageSuccess(
	ctx context.Context,
	cfg config.Config,
	db *gorm.DB,
	rdb *redis.Client,
	resp shared_models.OperationResponse,
	weights map[string]int,
) {
	metrics.StagesCompleted.WithLabelValues("workflow-orchestrator", "success").Inc()
	_ = SetStageStatus(ctx, rdb, resp.WorkflowId, resp.RequestId, resp.StageId, shared_models.StageFinished)
	_ = rdb.Set(ctx, outputKey(resp.WorkflowId, resp.StageId, resp.RequestId), resp.ResultOutput(), 48*time.Hour).Err()

	if err := UnblockChildStages(ctx, rdb, resp.WorkflowId, resp.StageId, resp.RequestId, weights); err != nil {
		slog.Error("unblock children", "stage_id", resp.StageId, "error", err)
	}

	recordTerminalStage(ctx, cfg, db, rdb, resp.WorkflowId, resp.RequestId)
}

func handleStageFailure(
	ctx context.Context,
	cfg config.Config,
	db *gorm.DB,
	rdb *redis.Client,
	dlqWriter *kafka.Writer,
	pool *Pool,
	resp shared_models.OperationResponse,
	weight int,
) {
	log := slog.With("workflow_id", resp.WorkflowId, "run_id", resp.RequestId, "stage_id", resp.StageId)

	attempt := resp.Attempt
	if attempt < 1 {
		attempt, _ = GetAttempt(ctx, rdb, resp.WorkflowId, resp.RequestId, resp.StageId)
	}

	if attempt < shared_models.MaxStageAttempts {
		metrics.StageRetries.WithLabelValues("workflow-orchestrator").Inc()
		_ = SetStageStatus(ctx, rdb, resp.WorkflowId, resp.RequestId, resp.StageId, shared_models.StageFailedRetry)
		_ = rdb.Set(ctx, outputKey(resp.WorkflowId, resp.StageId, resp.RequestId), resp.Error, 48*time.Hour).Err()

		if !ctxutil.Sleep(ctx, time.Duration(attempt)*time.Second) {
			return
		}
		if err := ScheduleStage(ctx, rdb, resp.StageId, resp.WorkflowId, resp.RequestId, weight); err != nil {
			log.Error("requeue stage", "error", err)
		}
		return
	}

	metrics.StagesCompleted.WithLabelValues("workflow-orchestrator", "failure").Inc()
	_ = SetStageStatus(ctx, rdb, resp.WorkflowId, resp.RequestId, resp.StageId, shared_models.StageFailedExhausted)
	_ = MarkRunFailed(ctx, rdb, resp.WorkflowId, resp.RequestId)
	skipDescendantStages(ctx, rdb, resp.WorkflowId, resp.RequestId, resp.StageId)
	recordTerminalStage(ctx, cfg, db, rdb, resp.WorkflowId, resp.RequestId)

	publishDLQ(ctx, dlqWriter, resp.WorkflowId, resp.RequestId, resp.StageId, string(resp.StageType), attempt, resp.Error, dlq.ReasonRetryExhausted)

	pool.Go(ctx, func(notifyCtx context.Context) {
		sendFailureNotification(notifyCtx, cfg, db, resp)
	})
}

func sendFailureNotification(ctx context.Context, cfg config.Config, db *gorm.DB, resp shared_models.OperationResponse) {
	dbCtx, cancel := ctxutil.WithDBTimeout(ctx, cfg)
	defer cancel()

	var wf models.Workflow
	name := ""
	if err := db.WithContext(dbCtx).Where("workflow_id = ?", resp.WorkflowId).First(&wf).Error; err == nil {
		name = wf.WorkflowName
	}
	errMsg := resp.Error
	if errMsg == "" {
		errMsg = "stage failed after maximum retries"
	}
	if err := notify.SendStageFailure(cfg, resp.WorkflowId, name, resp.RequestId, resp.StageId, errMsg); err != nil {
		slog.Error("smtp notification failed", "error", err)
	}
}

func recordTerminalStage(ctx context.Context, cfg config.Config, db *gorm.DB, rdb *redis.Client, workflowId, runId string) {
	if _, err := IncrementRunDone(ctx, rdb, workflowId, runId); err != nil {
		slog.Error("increment run done", "error", err)
		return
	}
	maybeFinalizeRun(ctx, cfg, db, rdb, workflowId, runId)
}

func deserializeMessage(message kafka.Message) shared_models.OperationResponse {
	var response shared_models.OperationResponse
	if err := json.Unmarshal(message.Value, &response); err != nil {
		slog.Error("unmarshal result message", "error", err)
	}
	return response
}

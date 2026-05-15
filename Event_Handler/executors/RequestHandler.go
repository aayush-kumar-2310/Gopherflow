package executors

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
	"Event_Handler/producer"
	ehworker "Event_Handler/worker"

	"Shared/config"
	"Shared/dlq"
	"Shared/metrics"
	"Shared/shared_models"

	"github.com/segmentio/kafka-go"
)

func ConsumeExecuteStageMessages(
	ctx context.Context,
	cfg config.Config,
	kafkaConsumer *kafka.Reader,
	kafkaProducer *kafka.Writer,
	dlqWriter *kafka.Writer,
	pool *ehworker.Pool,
) {
	topic := kafkaConsumer.Config().Topic
	for {
		if ctx.Err() != nil {
			slog.Info("kafka execute consumer stopped")
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

		metrics.KafkaConsumed.WithLabelValues("event-handler", topic).Inc()
		m := msg
		pool.Go(ctx, func(jobCtx context.Context) {
			processMessage(jobCtx, cfg, m, kafkaProducer, dlqWriter)
		})
	}
}

func processMessage(
	ctx context.Context,
	cfg config.Config,
	msg kafka.Message,
	kafkaProducer *kafka.Writer,
	dlqWriter *kafka.Writer,
) {
	start := time.Now()
	stage, err := mapKafkaMessageToStageObject(msg)
	if err != nil {
		slog.Warn("skipping invalid kafka message", "error", err, "raw", truncate(string(msg.Value), 200))
		publishRawDLQ(ctx, dlqWriter, string(msg.Value), err.Error())
		return
	}

	if stage.WorkflowId == "" || stage.StageId == "" {
		slog.Warn("invalid execute message", "raw", truncate(string(msg.Value), 200))
		publishDLQ(ctx, dlqWriter, stage, dlq.ReasonInvalidMessage, "missing workflow or stage id")
		return
	}

	log := slog.With("workflow_id", stage.WorkflowId, "run_id", stage.RequestId, "stage_id", stage.StageId)
	response := executeStage(ctx, stage)

	stageType := string(stage.StageType)
	elapsed := time.Since(start)
	if response.Status == "SUCCESS" {
		metrics.StagesCompleted.WithLabelValues("event-handler", "success").Inc()
		log.Info("stage executed",
			"stage_type", stageType,
			"status", response.Status,
			"attempt", response.Attempt,
			"duration_sec", elapsed.Seconds(),
		)
	} else {
		metrics.StagesCompleted.WithLabelValues("event-handler", "failure").Inc()
		log.Warn("stage execution failed",
			"stage_type", stageType,
			"attempt", response.Attempt,
			"duration_sec", elapsed.Seconds(),
			"error", response.Error,
		)
	}
	metrics.StageDuration.WithLabelValues(stageType).Observe(elapsed.Seconds())

	publishCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := producer.SendMessage(publishCtx, kafkaProducer, response); err != nil {
		log.Error("publish response failed", "error", err)
	}
}

func mapKafkaMessageToStageObject(msg kafka.Message) (shared_models.OperationRequest, error) {
	var currStage shared_models.OperationRequest
	if err := json.Unmarshal(msg.Value, &currStage); err != nil {
		return currStage, err
	}
	return currStage, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func publishRawDLQ(ctx context.Context, writer *kafka.Writer, raw, errMsg string) {
	if writer == nil {
		return
	}
	dlqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = dlq.Publish(dlqCtx, writer, "event-handler", dlq.Message{
		Error:   errMsg,
		Reason:  dlq.ReasonInvalidMessage,
		Source:  "event-handler",
		Payload: raw,
	})
}

func executeStage(ctx context.Context, stage shared_models.OperationRequest) shared_models.OperationResponse {
	switch stage.StageType {
	case shared_models.ExecuteScript:
		return ExecuteScriptStage(ctx, stage)
	case shared_models.FetchSftp:
		return ExecuteSFTPStage(stage, "download")
	case shared_models.UploadSftp:
		return ExecuteSFTPStage(stage, "upload")
	case shared_models.HTTPRequest:
		return ExecuteHTTPStage(ctx, stage)
	case shared_models.LLM:
		return ExecuteOllamaStage(ctx, stage)
	default:
		resp := failureResponse(stage, "unsupported stage type: "+string(stage.StageType))
		return resp
	}
}

func failureResponse(stage shared_models.OperationRequest, msg string) shared_models.OperationResponse {
	return shared_models.OperationResponse{
		RequestId:  stage.RequestId,
		WorkflowId: stage.WorkflowId,
		StageId:    stage.StageId,
		StageType:  stage.StageType,
		Attempt:    stage.Attempt,
		Status:     "FAILURE",
		Error:      msg,
	}
}

func publishDLQ(ctx context.Context, writer *kafka.Writer, stage shared_models.OperationRequest, reason, errMsg string) {
	if writer == nil {
		return
	}
	dlqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = dlq.Publish(dlqCtx, writer, "event-handler", dlq.Message{
		WorkflowID: stage.WorkflowId,
		RunID:      stage.RequestId,
		StageID:    stage.StageId,
		StageType:  string(stage.StageType),
		Attempt:    stage.Attempt,
		Error:      errMsg,
		Reason:     reason,
		Source:     "event-handler",
	})
}

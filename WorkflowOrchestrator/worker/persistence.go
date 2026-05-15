package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"Shared/config"
	"Shared/ctxutil"
	"Shared/metrics"
	"Shared/shared_models"
	"workflow-orchestrator/models"

	redis "github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

func createWorkflowRun(db *gorm.DB, workflowId, runId string, stageCount int) error {
	run := models.WorkflowRun{
		RunId:      runId,
		WorkflowId: workflowId,
		Status:     string(shared_models.RunRunning),
		StartedAt:  time.Now(),
		StageCount: stageCount,
	}
	return db.Create(&run).Error
}

func finalizeWorkflowRun(
	ctx context.Context,
	cfg config.Config,
	db *gorm.DB,
	rdb *redis.Client,
	workflow *models.Workflow,
	runId string,
) error {
	failed := IsRunFailed(ctx, rdb, workflow.WorkflowId, runId)
	status := shared_models.RunCompleted
	if failed {
		status = shared_models.RunPartialFailure
	}
	metrics.WorkflowRuns.WithLabelValues(string(status)).Inc()

	now := time.Now()
	var errorSummary string

	for _, stage := range workflow.Stages {
		stageStatus, err := GetStageStatus(ctx, rdb, workflow.WorkflowId, runId, stage.StageId)
		if err != nil {
			if failed {
				stageStatus = shared_models.StageSkipped
			} else {
				stageStatus = shared_models.StagePending
			}
		}

		attempt, _ := GetAttempt(ctx, rdb, workflow.WorkflowId, runId, stage.StageId)
		if attempt == 0 {
			attempt = 1
		}

		output, _ := rdb.Get(ctx, outputKey(workflow.WorkflowId, stage.StageId, runId)).Result()

		exec := models.StageExecution{
			RunId:        runId,
			WorkflowId:   workflow.WorkflowId,
			StageId:      stage.StageId,
			Status:       string(stageStatus),
			AttemptCount: attempt,
			Output:       output,
			CompletedAt:  &now,
		}
		if stageStatus == shared_models.StageFailedExhausted {
			errorSummary = fmt.Sprintf("stage %s failed after %d attempts", stage.StageId, attempt)
			exec.Error = errorSummary
		}

		if err := db.WithContext(ctx).Create(&exec).Error; err != nil {
			return fmt.Errorf("persist stage execution %s: %w", stage.StageId, err)
		}
	}

	updates := map[string]interface{}{
		"status":       string(status),
		"completed_at": now,
	}
	if errorSummary != "" {
		updates["error_summary"] = errorSummary
	}
	return db.WithContext(ctx).Model(&models.WorkflowRun{}).Where("run_id = ?", runId).Updates(updates).Error
}

func maybeFinalizeRun(
	ctx context.Context,
	cfg config.Config,
	db *gorm.DB,
	rdb *redis.Client,
	workflowId,
	runId string,
) {
	done, total, err := GetRunProgress(ctx, rdb, workflowId, runId)
	if err != nil || total == 0 || done < total {
		return
	}

	dbCtx, cancel := ctxutil.WithDBTimeout(ctx, cfg)
	defer cancel()

	var workflow models.Workflow
	if err := db.WithContext(dbCtx).Where("workflow_id = ?", workflowId).Preload("Stages").First(&workflow).Error; err != nil {
		slog.Error("finalize run: load workflow", "workflow_id", workflowId, "error", err)
		return
	}

	if err := finalizeWorkflowRun(dbCtx, cfg, db, rdb, &workflow, runId); err != nil {
		slog.Error("finalize run", "run_id", runId, "error", err)
		return
	}
	slog.Info("workflow run finalized", "workflow_id", workflowId, "run_id", runId)
}

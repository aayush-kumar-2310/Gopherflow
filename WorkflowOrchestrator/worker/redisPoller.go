package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"workflow-orchestrator/models"

	"Shared/config"
	"Shared/ctxutil"
	"Shared/metrics"
	"Shared/shared_models"

	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"github.com/segmentio/kafka-go"
	"gorm.io/gorm"
)

func StartRedisPoller(
	ctx context.Context,
	cfg config.Config,
	db *gorm.DB,
	redisClient *redis.Client,
	kafkaProducer *kafka.Writer,
	dlqWriter *kafka.Writer,
	pool *Pool,
) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rctx, cancel := ctxutil.WithRedisTimeout(ctx, cfg)
			now := time.Now().Unix()
			// Redis ops use rctx (short); pool jobs use root ctx so tick cancel does not kill them.
			handleWorkflowTriggers(ctx, rctx, cfg, now, db, redisClient, pool)
			handleStageTriggers(ctx, rctx, cfg, now, db, redisClient, kafkaProducer, dlqWriter, pool)
			cancel()
		case <-ctx.Done():
			slog.Info("redis poller stopped")
			return
		}
	}
}

func handleWorkflowTriggers(
	rootCtx context.Context,
	redisCtx context.Context,
	cfg config.Config,
	now int64,
	db *gorm.DB,
	redisClient *redis.Client,
	pool *Pool,
) {
	workflows, err := redisClient.ZRangeByScoreWithScores(
		redisCtx,
		"workflow-trigger",
		&redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%d", now)},
	).Result()
	if err != nil {
		slog.Error("fetch workflow triggers", "error", err)
		return
	}

	for _, wfKey := range workflows {
		if redisCtx.Err() != nil {
			return
		}
		if n, _ := redisClient.ZRem(redisCtx, "workflow-trigger", wfKey.Member).Result(); n > 0 {
			workflowId := strings.Split(wfKey.Member.(string), ":")[0]
			pool.Go(rootCtx, func(jobCtx context.Context) {
				opCtx, cancel := ctxutil.JobContext(jobCtx, 5*time.Minute)
				defer cancel()
				InitializeWorkflow(opCtx, cfg, workflowId, db, redisClient)
			})
		}
	}
}

func InitializeWorkflow(ctx context.Context, cfg config.Config, workflowId string, db *gorm.DB, redisClient *redis.Client) {
	dbCtx, cancel := ctxutil.WithDBTimeout(ctx, cfg)
	defer cancel()

	var workflow models.Workflow
	if err := db.WithContext(dbCtx).Where("workflow_id = ?", workflowId).Preload("Stages").First(&workflow).Error; err != nil {
		slog.Error("load workflow", "workflow_id", workflowId, "error", err)
		return
	}

	runId := uuid.New().String()
	metrics.WorkflowRuns.WithLabelValues("started").Inc()
	slog.Info("workflow run started", "workflow_id", workflowId, "run_id", runId)

	weights := make(map[string]int)
	for _, stage := range workflow.Stages {
		w := stage.Weight
		if w < 1 {
			w = 1
		}
		weights[stage.StageId] = w
	}

	if err := createWorkflowRun(db.WithContext(dbCtx), workflow.WorkflowId, runId, len(workflow.Stages)); err != nil {
		slog.Error("create workflow run", "workflow_id", workflowId, "error", err)
		return
	}
	if err := InitRunCounters(ctx, redisClient, workflow.WorkflowId, runId, len(workflow.Stages)); err != nil {
		slog.Error("init run counters", "workflow_id", workflowId, "error", err)
		return
	}
	if err := setStageWeights(ctx, redisClient, workflow.WorkflowId, runId, weights); err != nil {
		slog.Error("set stage weights", "workflow_id", workflowId, "error", err)
		return
	}

	reverseDeps := make(map[string][]string)
	for _, stage := range workflow.Stages {
		_ = SetStageStatus(ctx, redisClient, workflow.WorkflowId, runId, stage.StageId, shared_models.StagePending)
		for _, dep := range stage.DependentOn {
			reverseDeps[dep] = append(reverseDeps[dep], stage.StageId)
		}
	}

	for parent, children := range reverseDeps {
		key := childrenKey(workflow.WorkflowId, parent, runId)
		for _, child := range children {
			if err := redisClient.RPush(ctx, key, child).Err(); err != nil {
				slog.Error("store children", "parent", parent, "error", err)
			}
		}
		_ = redisClient.Expire(ctx, key, 48*time.Hour).Err()
	}

	for _, stage := range workflow.Stages {
		if len(stage.DependentOn) == 0 {
			if err := ScheduleStage(ctx, redisClient, stage.StageId, workflow.WorkflowId, runId, weights[stage.StageId]); err != nil {
				slog.Error("schedule root stage", "stage_id", stage.StageId, "error", err)
			} else {
				slog.Info("root stage queued", "workflow_id", workflowId, "run_id", runId, "stage_id", stage.StageId)
			}
		} else {
			key := depsKey(workflow.WorkflowId, stage.StageId, runId)
			if err := redisClient.Set(ctx, key, len(stage.DependentOn), 48*time.Hour).Err(); err != nil {
				slog.Error("store deps count", "stage_id", stage.StageId, "error", err)
			}
		}
	}

	schedule, err := cron.ParseStandard(workflow.CronExpression)
	if err != nil {
		slog.Error("parse cron", "workflow_id", workflowId, "error", err)
		return
	}
	nextRun := schedule.Next(time.Now())
	_, err = redisClient.ZAdd(ctx, "workflow-trigger", redis.Z{
		Member: workflow.WorkflowId + ":" + uuid.New().String(),
		Score:  float64(nextRun.Unix()),
	}).Result()
	if err != nil {
		slog.Error("reschedule workflow", "workflow_id", workflowId, "error", err)
	}
}

func handleStageTriggers(
	rootCtx context.Context,
	redisCtx context.Context,
	cfg config.Config,
	now int64,
	db *gorm.DB,
	redisClient *redis.Client,
	kafkaProducer *kafka.Writer,
	dlqWriter *kafka.Writer,
	pool *Pool,
) {
	// Scores are (6-weight)*1e13 + unix, not raw unix — Max must include priority band.
	maxScore := ReadyStagesMaxScore(now)
	stages, err := redisClient.ZRangeByScoreWithScores(
		redisCtx,
		"job-trigger",
		&redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%f", maxScore)},
	).Result()
	if err != nil {
		slog.Error("fetch stage triggers", "error", err)
		return
	}

	if len(stages) > 0 {
		slog.Debug("job-trigger ready stages", "count", len(stages), "max_score", maxScore)
	}

	for _, stageKey := range stages {
		if redisCtx.Err() != nil {
			return
		}
		if n, _ := redisClient.ZRem(redisCtx, "job-trigger", stageKey.Member).Result(); n > 0 {
			stageId, workflowId, runId := splitKey(stageKey.Member.(string))
			if stageId == "" {
				continue
			}
			pool.Go(rootCtx, func(jobCtx context.Context) {
				opCtx, cancel := ctxutil.JobContext(jobCtx, 2*time.Minute)
				defer cancel()
				ExecuteJob(opCtx, cfg, stageId, workflowId, db, redisClient, kafkaProducer, dlqWriter, runId)
			})
		}
	}
}

func splitKey(jobKey string) (stageId, workflowId, runId string) {
	parts := strings.Split(jobKey, ":")
	if len(parts) < 3 {
		slog.Warn("invalid stage job key", "key", jobKey)
		return "", "", ""
	}
	return parts[0], parts[1], parts[2]
}

func stageWeightsKey(workflowId, runId string) string {
	return fmt.Sprintf("run:weights:%s:%s", workflowId, runId)
}

func setStageWeights(ctx context.Context, rdb *redis.Client, workflowId, runId string, weights map[string]int) error {
	key := stageWeightsKey(workflowId, runId)
	pipe := rdb.Pipeline()
	for stageId, w := range weights {
		pipe.HSet(ctx, key, stageId, w)
	}
	pipe.Expire(ctx, key, 48*time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

func loadStageWeights(ctx context.Context, rdb *redis.Client, workflowId, runId string) (map[string]int, error) {
	raw, err := rdb.HGetAll(ctx, stageWeightsKey(workflowId, runId)).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string]int, len(raw))
	for id, w := range raw {
		weight, _ := strconv.Atoi(w)
		if weight < 1 {
			weight = 1
		}
		out[id] = weight
	}
	return out, nil
}

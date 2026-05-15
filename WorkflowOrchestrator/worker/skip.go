package worker

import (
	"context"
	"log/slog"

	"Shared/shared_models"

	redis "github.com/redis/go-redis/v9"
)

func skipDescendantStages(ctx context.Context, rdb *redis.Client, workflowId, runId, failedStageId string) {
	queue := []string{failedStageId}
	seen := map[string]bool{failedStageId: true}

	for len(queue) > 0 {
		if ctx.Err() != nil {
			return
		}
		parent := queue[0]
		queue = queue[1:]

		children, err := rdb.LRange(ctx, childrenKey(workflowId, parent, runId), 0, -1).Result()
		if err != nil {
			slog.Error("skip descendants: list children", "parent", parent, "error", err)
			continue
		}

		for _, child := range children {
			if seen[child] {
				continue
			}
			seen[child] = true

			status, err := GetStageStatus(ctx, rdb, workflowId, runId, child)
			if err != nil || status == shared_models.StageFinished {
				continue
			}
			if status == shared_models.StageRunning || status == shared_models.StageFailedRetry {
				continue
			}

			_ = SetStageStatus(ctx, rdb, workflowId, runId, child, shared_models.StageSkipped)
			if _, err := IncrementRunDone(ctx, rdb, workflowId, runId); err != nil {
				slog.Error("skip descendants: increment done", "error", err)
			}

			queue = append(queue, child)
		}
	}
}

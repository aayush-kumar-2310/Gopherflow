package worker

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"Shared/shared_models"

	redis "github.com/redis/go-redis/v9"
)

const priorityScale = 1e13

func stageStatusKey(workflowId, runId, stageId string) string {
	return fmt.Sprintf("stage-status:%s:%s:%s", workflowId, runId, stageId)
}

func outputKey(workflowId, stageId, runId string) string {
	return fmt.Sprintf("output:%s:%s:%s", workflowId, stageId, runId)
}

func attemptKey(workflowId, runId, stageId string) string {
	return fmt.Sprintf("attempt:%s:%s:%s", workflowId, runId, stageId)
}

func childrenKey(workflowId, parentStageId, runId string) string {
	return fmt.Sprintf("children:%s:%s:%s", workflowId, parentStageId, runId)
}

func depsKey(workflowId, stageId, runId string) string {
	return fmt.Sprintf("deps:%s:%s:%s", workflowId, stageId, runId)
}

func runTotalKey(workflowId, runId string) string {
	return fmt.Sprintf("run:total:%s:%s", workflowId, runId)
}

func runDoneKey(workflowId, runId string) string {
	return fmt.Sprintf("run:done:%s:%s", workflowId, runId)
}

func runFailedKey(workflowId, runId string) string {
	return fmt.Sprintf("run:failed:%s:%s", workflowId, runId)
}

func seenResultKey(workflowId, runId, stageId string, attempt int) string {
	return fmt.Sprintf("seen:%s:%s:%s:%d", workflowId, runId, stageId, attempt)
}

func jobMember(stageId, workflowId, runId string) string {
	return fmt.Sprintf("%s:%s:%s", stageId, workflowId, runId)
}

// PriorityScore: higher weight (5) yields a lower score so ZSET pops it first.
// Score layout: (6-weight)*priorityScale + unix — must use ReadyStagesMaxScore when polling.
func PriorityScore(weight int, unix int64) float64 {
	if weight < 1 {
		weight = 1
	}
	if weight > 5 {
		weight = 5
	}
	return float64(6-weight)*priorityScale + float64(unix)
}

// ReadyStagesMaxScore is the ZSET upper bound for stages scheduled at or before unix.
func ReadyStagesMaxScore(unix int64) float64 {
	// Weight 1 → highest score among ready jobs: 5*priorityScale + unix
	return 5*priorityScale + float64(unix)
}

func SetStageStatus(ctx context.Context, rdb *redis.Client, workflowId, runId, stageId string, status shared_models.StageStatus) error {
	return rdb.Set(ctx, stageStatusKey(workflowId, runId, stageId), string(status), 48*time.Hour).Err()
}

func GetStageStatus(ctx context.Context, rdb *redis.Client, workflowId, runId, stageId string) (shared_models.StageStatus, error) {
	val, err := rdb.Get(ctx, stageStatusKey(workflowId, runId, stageId)).Result()
	if err != nil {
		return "", err
	}
	return shared_models.StageStatus(val), nil
}

func GetAttempt(ctx context.Context, rdb *redis.Client, workflowId, runId, stageId string) (int, error) {
	val, err := rdb.Get(ctx, attemptKey(workflowId, runId, stageId)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(val)
}

func IncrementAttempt(ctx context.Context, rdb *redis.Client, workflowId, runId, stageId string) (int, error) {
	n, err := rdb.Incr(ctx, attemptKey(workflowId, runId, stageId)).Result()
	if err != nil {
		return 0, err
	}
	_ = rdb.Expire(ctx, attemptKey(workflowId, runId, stageId), 48*time.Hour).Err()
	return int(n), nil
}

func ScheduleStage(ctx context.Context, rdb *redis.Client, stageId, workflowId, runId string, weight int) error {
	member := jobMember(stageId, workflowId, runId)
	score := PriorityScore(weight, time.Now().Unix())
	return rdb.ZAdd(ctx, "job-trigger", redis.Z{Member: member, Score: score}).Err()
}

func MarkResultSeen(ctx context.Context, rdb *redis.Client, workflowId, runId, stageId string, attempt int) (bool, error) {
	ok, err := rdb.SetNX(ctx, seenResultKey(workflowId, runId, stageId, attempt), "1", 24*time.Hour).Result()
	return ok, err
}

func InitRunCounters(ctx context.Context, rdb *redis.Client, workflowId, runId string, totalStages int) error {
	pipe := rdb.Pipeline()
	pipe.Set(ctx, runTotalKey(workflowId, runId), totalStages, 48*time.Hour)
	pipe.Set(ctx, runDoneKey(workflowId, runId), 0, 48*time.Hour)
	pipe.Del(ctx, runFailedKey(workflowId, runId))
	_, err := pipe.Exec(ctx)
	return err
}

func IncrementRunDone(ctx context.Context, rdb *redis.Client, workflowId, runId string) (int64, error) {
	n, err := rdb.Incr(ctx, runDoneKey(workflowId, runId)).Result()
	if err != nil {
		return 0, err
	}
	_ = rdb.Expire(ctx, runDoneKey(workflowId, runId), 48*time.Hour).Err()
	return n, nil
}

func MarkRunFailed(ctx context.Context, rdb *redis.Client, workflowId, runId string) error {
	return rdb.Set(ctx, runFailedKey(workflowId, runId), "1", 48*time.Hour).Err()
}

func IsRunFailed(ctx context.Context, rdb *redis.Client, workflowId, runId string) bool {
	n, _ := rdb.Exists(ctx, runFailedKey(workflowId, runId)).Result()
	return n > 0
}

func GetRunProgress(ctx context.Context, rdb *redis.Client, workflowId, runId string) (done, total int64, err error) {
	total, err = rdb.Get(ctx, runTotalKey(workflowId, runId)).Int64()
	if err != nil {
		return 0, 0, err
	}
	done, err = rdb.Get(ctx, runDoneKey(workflowId, runId)).Int64()
	return done, total, err
}

func UnblockChildStages(ctx context.Context, rdb *redis.Client, workflowId, parentStageId, runId string, childWeights map[string]int) error {
	children, err := rdb.LRange(ctx, childrenKey(workflowId, parentStageId, runId), 0, -1).Result()
	if err != nil {
		return err
	}
	for _, childStage := range children {
		if err := decrementDepsAndMaybeSchedule(ctx, rdb, workflowId, childStage, runId, childWeights); err != nil {
			return err
		}
	}
	return nil
}

func decrementDepsAndMaybeSchedule(ctx context.Context, rdb *redis.Client, workflowId, stageId, runId string, weights map[string]int) error {
	remaining, err := rdb.Decr(ctx, depsKey(workflowId, stageId, runId)).Result()
	if err != nil {
		return err
	}
	if remaining == 0 {
		weight := weights[stageId]
		if weight == 0 {
			weight = 1
		}
		return ScheduleStage(ctx, rdb, stageId, workflowId, runId, weight)
	}
	return nil
}

package executors

import (
	"context"
	"os/exec"
	"time"

	"Shared/shared_models"
)

func ExecuteScriptStage(ctx context.Context, stage shared_models.OperationRequest) shared_models.OperationResponse {
	runCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(runCtx, "python3", "-c", stage.Script)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if runCtx.Err() == context.DeadlineExceeded {
			return failureResponse(stage, "script execution timed out after 5 minutes")
		}
		return failureResponse(stage, err.Error())
	}

	return shared_models.OperationResponse{
		RequestId:      stage.RequestId,
		WorkflowId:     stage.WorkflowId,
		StageId:        stage.StageId,
		StageType:      stage.StageType,
		Attempt:        stage.Attempt,
		Status:         "SUCCESS",
		ScriptResponse: string(output),
	}
}

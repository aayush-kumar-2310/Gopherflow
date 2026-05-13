package executors

import (
	"Shared/shared_models"
	"os/exec"
)

func ExecuteScriptStage(stage shared_models.OperationRequest) shared_models.OperationResponse {

	pythonScript := stage.Script

	cmd := exec.Command("python3", "-c", pythonScript)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return shared_models.OperationResponse{
			WorkflowId: stage.WorkflowId,
			StageId:    stage.StageId,
			StageType:  stage.StageType,
			Status:     "FAILURE",
			Error:      err.Error(),
		}
	}

	return shared_models.OperationResponse{
		RequestId:      stage.RequestId,
		WorkflowId:     stage.WorkflowId,
		StageId:        stage.StageId,
		StageType:      stage.StageType,
		Status:         "SUCCESS",
		ScriptResponse: string(output),
	}

}

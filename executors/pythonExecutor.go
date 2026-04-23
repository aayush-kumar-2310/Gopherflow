package executors

import (
	"context"
	"fmt"
	"os/exec"
	"workflow-orchestrator/models"
)

type ScriptRequest struct {
	Scripts []string `json:"scripts"`
}

func ExecutePythonScript(script string, resultChannel chan models.JobResult, ctx context.Context) {
	cmd := exec.CommandContext(ctx, "python3", "-c", script)
	output, err := cmd.CombinedOutput()
	if err != nil {
		resultChannel <- models.JobResult{
			Output: "",
			Error:  fmt.Errorf("error executing script %s: %s", script, err.Error()).Error(),
		}
		return
	}
	resultChannel <- models.JobResult{
		Output: string(output),
		Error:  "",
	}
}

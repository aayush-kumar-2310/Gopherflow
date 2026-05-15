package util

import (
	"net/http"
	"workflow-orchestrator/models"

	"github.com/gin-gonic/gin"
)

var allowedOperations = map[string]bool{
	"EXECUTE_SCRIPT": true,
	"FETCH_SFTP":     true,
	"UPLOAD_SFTP":    true,
	"HTTP_REQUEST":   true,
	"LLM":            true,
}

func ValidateWorkflow(workflow models.Workflow) bool {
	return workflow.WorkflowName != "" &&
		workflow.WorkflowId != "" &&
		workflow.CronExpression != ""
}

func ValidateStage(stage models.Stage) bool {
	if stage.StageId == "" || stage.WorkflowId == "" || stage.Operation == "" {
		return false
	}
	if !allowedOperations[stage.Operation] {
		return false
	}
	if stage.Weight < 1 || stage.Weight > 5 {
		return false
	}
	return true
}

func ValidateDependencyGraph(c *gin.Context, workflow models.Workflow) bool {
	hasLoop, errMsg := checkDependencyLoop(workflow)
	if hasLoop {
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return false
	}
	return true
}

func checkDependencyLoop(workflow models.Workflow) (bool, string) {
	graph := make(map[string][]string)
	allStageIds := make(map[string]bool)

	for _, s := range workflow.Stages {
		allStageIds[s.StageId] = true
	}

	for _, stage := range workflow.Stages {
		for _, dep := range stage.DependentOn {
			if !allStageIds[dep] {
				return true, "Dependency " + dep + " does not exist"
			}
		}
		graph[stage.StageId] = stage.DependentOn
	}

	visited := make(map[string]bool)
	recStack := make(map[string]bool)
	var errorMsg string

	var dfs func(string) bool
	dfs = func(node string) bool {
		if recStack[node] {
			errorMsg = "Cycle detected at stage " + node
			return true
		}
		if visited[node] {
			return false
		}
		visited[node] = true
		recStack[node] = true
		for _, neighbor := range graph[node] {
			if dfs(neighbor) {
				return true
			}
		}
		recStack[node] = false
		return false
	}

	for stageId := range graph {
		if dfs(stageId) {
			return true, errorMsg
		}
	}
	return false, ""
}

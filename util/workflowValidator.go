package util

import (
	"net/http"
	"workflow-orchestrator/models"

	"github.com/gin-gonic/gin"
)

func ValidateWorkflow(workflow models.Workflow) bool {
	if workflow.WorkflowName == "" {
		return false
	} else if workflow.WorkflowId == "" {
		return false
	} else if workflow.CronExpression == "" {
		return false
	}
	return true
}

func ValidateStage(stage models.Stage) bool {
	if stage.StageId == "" {
		return false
	} else if stage.WorkflowId == "" {
		return false
	} else if stage.Operation == "" {
		return false
	}
	return true
}

func ValidateDependencyGraph(c *gin.Context, workflow models.Workflow) bool {
	// We capture both the boolean and the error string here
	hasLoop, errMsg := checkDependencyLoop(workflow)

	if hasLoop {
		// Now you can actually tell the user WHY it failed
		c.JSON(http.StatusBadRequest, gin.H{"error": errMsg})
		return false
	}
	return true
}

func checkDependencyLoop(workflow models.Workflow) (bool, string) {
	graph := make(map[string][]string)
	allStageIds := make(map[string]bool)

	// 1. Map all IDs for O(1) existence check
	for _, s := range workflow.Stages {
		allStageIds[s.StageId] = true // Just assign, no need for _
	}

	// 2. Build graph and check for "Ghost" dependencies
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

	// 3. DFS Closure
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

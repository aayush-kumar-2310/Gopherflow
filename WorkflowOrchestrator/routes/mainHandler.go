package routes

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
	"workflow-orchestrator/models"
	"workflow-orchestrator/util"

	gin "github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
)

func createWorkflowHandler(c *gin.Context, db *gorm.DB, redisClient *redis.Client) {
	var newWorkflow models.Workflow

	if err := c.BindJSON(&newWorkflow); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	ctx := c.Request.Context()

	var existingWorkflow models.Workflow
	err := db.WithContext(ctx).Where("workflow_id = ?", newWorkflow.WorkflowId).First(&existingWorkflow).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to check workflow"})
		return
	}
	if err == nil && existingWorkflow.WorkflowId != "" {
		c.JSON(http.StatusConflict, gin.H{"error": "Workflow with the same ID already exists"})
		return
	}

	if !util.ValidateWorkflow(newWorkflow) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid workflow data"})
		return
	}

	if !util.ValidateDependencyGraph(c, newWorkflow) {
		return
	}

	for i := range newWorkflow.Stages {
		if newWorkflow.Stages[i].Weight == 0 {
			newWorkflow.Stages[i].Weight = 1
		}
		if !util.ValidateStage(newWorkflow.Stages[i]) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid stage data in workflow"})
			return
		}
	}

	if err := db.WithContext(ctx).Create(&newWorkflow).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create workflow"})
		return
	}

	if err := pushJobToRedis(newWorkflow, redisClient, ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to schedule workflow"})
		return
	}

	c.JSON(http.StatusCreated, newWorkflow)
}

func getWorkflowsHandler(c *gin.Context, db *gorm.DB) {
	var workflows []models.Workflow
	db.WithContext(c.Request.Context()).Preload("Stages").Find(&workflows)
	c.JSON(http.StatusOK, workflows)
}

func getWorkflowRunsHandler(c *gin.Context, db *gorm.DB) {
	workflowId := c.Query("workflowId")
	q := db.WithContext(c.Request.Context()).Order("started_at desc").Limit(100)
	if workflowId != "" {
		q = q.Where("workflow_id = ?", workflowId)
	}
	var runs []models.WorkflowRun
	q.Find(&runs)
	c.JSON(http.StatusOK, runs)
}

func getRunStagesHandler(c *gin.Context, db *gorm.DB) {
	runId := c.Param("runId")
	var executions []models.StageExecution
	db.WithContext(c.Request.Context()).Where("run_id = ?", runId).Find(&executions)
	c.JSON(http.StatusOK, executions)
}

func pushJobToRedis(workflow models.Workflow, redisClient *redis.Client, ctx context.Context) error {
	schedule, err := cron.ParseStandard(workflow.CronExpression)
	if err != nil {
		return fmt.Errorf("invalid cron expression for workflow %s: %w", workflow.WorkflowId, err)
	}

	nextRun := schedule.Next(time.Now())
	key := workflow.WorkflowId + ":" + uuid.New().String()

	_, err = redisClient.ZAdd(ctx, "workflow-trigger", redis.Z{
		Member: key,
		Score:  float64(nextRun.Unix()),
	}).Result()
	if err != nil {
		return fmt.Errorf("error scheduling workflow in Redis: %w", err)
	}
	return nil
}

func SetupRouter(db *gorm.DB, redisClient *redis.Client) *gin.Engine {
	router := gin.Default()
	router.POST("/createWorkflow", func(c *gin.Context) {
		createWorkflowHandler(c, db, redisClient)
	})
	router.GET("/workflows", func(c *gin.Context) {
		getWorkflowsHandler(c, db)
	})
	router.GET("/runs", func(c *gin.Context) {
		getWorkflowRunsHandler(c, db)
	})
	router.GET("/runs/:runId/stages", func(c *gin.Context) {
		getRunStagesHandler(c, db)
	})
	return router
}

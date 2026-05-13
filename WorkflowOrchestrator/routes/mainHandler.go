package routes

import (
	"context"
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
		c.JSON(400, gin.H{"error": "Invalid request body"})
		return
	}

	var existingWorkflow models.Workflow
	db.Where("workflow_id = ?", newWorkflow.WorkflowId).First(&existingWorkflow)
	if existingWorkflow.WorkflowId != "" {
		c.JSON(http.StatusConflict, gin.H{"error": "Workflow with the same ID already exists"})
		return
	}

	if !util.ValidateWorkflow(newWorkflow) {
		c.JSON(400, gin.H{"error": "Invalid workflow data"})
		return
	}

	if !util.ValidateDependencyGraph(c, newWorkflow) {
		return
	}

	for _, stage := range newWorkflow.Stages {
		if !util.ValidateStage(stage) {
			c.JSON(400, gin.H{"error": "Invalid stage data in workflow"})
			return
		}
	}

	result := db.Create(&newWorkflow)
	if result.Error != nil {
		c.JSON(500, gin.H{"error": "Failed to create workflow"})
		return
	}

	err := pushJobToRedis(newWorkflow, redisClient, c.Request.Context())
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to schedule workflow"})
		return
	}

	c.JSON(http.StatusCreated, newWorkflow)
}

func getWorkflowsHandler(c *gin.Context, db *gorm.DB) {
	var workflows []models.Workflow
	db.Find(&workflows)
	c.JSON(http.StatusOK, workflows)
}

func pushJobToRedis(workflow models.Workflow, redisClient *redis.Client, ctx context.Context) error {
	now := time.Now()

	schedule, err := cron.ParseStandard(workflow.CronExpression)
	if err != nil {
		return fmt.Errorf("invalid cron expression for workflow %s: %w", workflow.WorkflowId, err)
	}

	nextRun := schedule.Next(now)
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
	return router
}

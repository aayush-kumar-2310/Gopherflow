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

	err := pushJobToRedis(newWorkflow, redisClient)
	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to schedule workflow"})
		return
	}

	c.IndentedJSON(http.StatusCreated, newWorkflow)
}

func getWorkflowsHandler(c *gin.Context, db *gorm.DB) {
	var workflows []models.Workflow
	db.Find(&workflows)
	c.IndentedJSON(http.StatusOK, workflows)
}

func pushJobToRedis(workflow models.Workflow, redisClient *redis.Client) error {
	now := time.Now()
	schedule, _ := cron.ParseStandard(workflow.CronExpression)
	nextRun := schedule.Next(now)
	fmt.Printf("Scheduling workflow %s with cron expression: %s\n", workflow.WorkflowName, workflow.CronExpression)
	key := workflow.WorkflowId + ":" + uuid.New().String()

	_, err := redisClient.ZAdd(
		context.Background(),
		"workflow-trigger",
		redis.Z{
			Member: key,
			Score:  float64(nextRun.Unix()),
		},
	).Result()

	if err != nil {
		fmt.Printf("Error scheduling workflow in Redis: %v\n", err)
	}

	for _, stage := range workflow.Stages {

		key = stage.StageId + ":" + workflow.WorkflowId + ":" + uuid.New().String()
		_, err = redisClient.ZAdd(
			context.Background(),
			"job-trigger",
			redis.Z{
				Member: key,
				Score:  float64(nextRun.Unix()),
			},
		).Result()

		if err != nil {
			fmt.Printf("Error scheduling job in Redis: %v\n", err)
		}
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

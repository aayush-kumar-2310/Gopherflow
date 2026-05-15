package models

import (
	"time"

	"gorm.io/gorm"
)

type WorkflowRun struct {
	gorm.Model
	RunId        string    `gorm:"uniqueIndex" json:"runId"`
	WorkflowId   string    `gorm:"index" json:"workflowId"`
	Status       string    `json:"status"`
	StartedAt    time.Time `json:"startedAt"`
	CompletedAt  *time.Time `json:"completedAt,omitempty"`
	ErrorSummary string    `json:"errorSummary,omitempty"`
	StageCount   int       `json:"stageCount"`
}

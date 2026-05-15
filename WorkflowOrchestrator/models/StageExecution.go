package models

import (
	"time"

	"gorm.io/gorm"
)

type StageExecution struct {
	gorm.Model
	RunId        string     `gorm:"index" json:"runId"`
	WorkflowId   string     `gorm:"index" json:"workflowId"`
	StageId      string     `json:"stageId"`
	Status       string     `json:"status"`
	AttemptCount int        `json:"attemptCount"`
	Output       string     `gorm:"type:text" json:"output,omitempty"`
	Error        string     `gorm:"type:text" json:"error,omitempty"`
	StartedAt    *time.Time `json:"startedAt,omitempty"`
	CompletedAt  *time.Time `json:"completedAt,omitempty"`
}

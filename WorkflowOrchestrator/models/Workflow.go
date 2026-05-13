package models

import "gorm.io/gorm"

type Workflow struct {
	gorm.Model
	WorkflowId     string  `gorm:"uniqueIndex"`
	WorkflowName   string  `json:"workflowName"`
	CronExpression string  `json:"cronExpression"`
	Stages         []Stage `gorm:"foreignKey:WorkflowId;references:WorkflowId"`
}

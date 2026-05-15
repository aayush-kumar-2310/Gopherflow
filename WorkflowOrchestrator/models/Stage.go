package models

import "gorm.io/gorm"

type Stage struct {
	gorm.Model
	StageId        string   `gorm:"uniqueIndex:idx_workflow_stage,priority:2" json:"stageId"`
	WorkflowId     string   `gorm:"uniqueIndex:idx_workflow_stage,priority:1" json:"workflowId"`
	Operation      string   `json:"operation"`
	DependentOn    []string `gorm:"serializer:json" json:"dependsOn"`
	Weight         int      `json:"weight"` // 1 (lowest) – 5 (highest)
	Script         string   `json:"script,omitempty"`
	RemoteFilePath string   `json:"remoteFilePath,omitempty"`
	LocalFilePath  string   `json:"localFilePath,omitempty"`
	SFTPHost       string   `json:"sftpHost,omitempty"`
	SFTPPort       int      `json:"sftpPort,omitempty"`
	SFTPUsername   string   `json:"sftpUsername,omitempty"`
	SFTPPassword   string   `json:"sftpPassword,omitempty"`
	HttpEndpoint   string   `json:"httpEndpoint,omitempty"`
	HttpMethod     string   `json:"httpMethod,omitempty"`
	HttpPayload    string   `json:"httpPayload,omitempty"`
	HttpHeaders    map[string]string `gorm:"serializer:json" json:"httpHeaders,omitempty"`
	Prompt         string   `json:"prompt,omitempty"`
	Context        string   `json:"context,omitempty"`
}

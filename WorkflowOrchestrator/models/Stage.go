package models

import "gorm.io/gorm"

type StageType string

const (
	ExecuteScript StageType = "EXECUTE_SCRIPT"
	FetchSftp     StageType = "FETCH_SFTP"
	UploadSftp    StageType = "UPLOAD_SFTP"
	Summarize     StageType = "SUMMARIZE"
	HTTPRequest   StageType = "HTTP_REQUEST"
)

type Stage struct {
	gorm.Model
	StageId        string    `gorm:"uniqueIndex"`
	WorkflowId     string    `json:"workflowId"`
	Operation      StageType `json:"operation"`
	DependentOn    []string  `gorm:"serializer:json"`
	Script         string    `json:"script,omitempty"`
	RemoteFilePath string    `json:"remoteFilePath,omitempty"`
	LocalFilePath  string    `json:"localFilePath,omitempty"`
	HttpEndpoint   string    `json:"httpEndpoint,omitempty"`
	HttpMethod     string    `json:"httpMethod,omitempty"`
	HttpPayload    string    `json:"httpPayload,omitempty"`
}

package models

import "gorm.io/gorm"

type StageType string

const (
	ExecuteScript  StageType = "EXECUTE_SCRIPT"
	FetchSftp      StageType = "FETCH_SFTP"
	UploadSftp     StageType = "UPLOAD_SFTP"
	FetchS3        StageType = "FETCH_S3"
	UploadS3       StageType = "UPLOAD_S3"
	Summarize      StageType = "SUMMARIZE"
	GenerateReport StageType = "GENERATE_REPORT"
)

type SftpDetails struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Path     string `json:"path"`
}

type S3Details struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Region string `json:"region"`
}

type Stage struct {
	gorm.Model
	StageId     string       `gorm:"uniqueIndex"`
	WorkflowId  string       `json:"workflowId"`
	Operation   StageType    `json:"operation"`
	DependentOn []string     `gorm:"serializer:json"`
	Script      string       `json:"script,omitempty"`
	SftpDetails *SftpDetails `gorm:"serializer:json" json:"sftpDetails,omitempty"`
	S3Details   *S3Details   `gorm:"serializer:json" json:"s3Details,omitempty"`
}

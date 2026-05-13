package models

type SftpUploadRequest struct {
	LocalPath  string `json:"localPath"`
	RemotePath string `json:"remotePath"`
}

package executors

import (
	"Shared/shared_models"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

func ExecuteSFTPStage(stage shared_models.OperationRequest, operation string) shared_models.OperationResponse {
	// SSH client config
	config := &ssh.ClientConfig{
		User: stage.SFTPUsername,
		Auth: []ssh.AuthMethod{
			ssh.Password(stage.SFTPPassword),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // In production, verify host key
	}

	// Connect
	addr := fmt.Sprintf("%s:%d", stage.SFTPHost, stage.SFTPPort)
	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return failureResponse(stage, fmt.Sprintf("SSH connection failed: %v", err))
	}
	defer conn.Close()

	// SFTP client
	client, err := sftp.NewClient(conn)
	if err != nil {
		return failureResponse(stage, fmt.Sprintf("SFTP client creation failed: %v", err))
	}
	defer client.Close()

	// Execute operation
	var result string
	switch operation {
	case "upload":
		result, err = uploadFile(client, stage.LocalFilePath, stage.RemoteFilePath)
	case "download":
		result, err = downloadFile(client, stage.RemoteFilePath, stage.LocalFilePath)
	default:
		return failureResponse(stage, "invalid SFTP operation: must be upload or download")
	}

	if err != nil {
		return failureResponse(stage, err.Error())
	}

	return shared_models.OperationResponse{
		RequestId:      stage.RequestId,
		WorkflowId:     stage.WorkflowId,
		StageId:        stage.StageId,
		StageType:      stage.StageType,
		Attempt:        stage.Attempt,
		Status:         "SUCCESS",
		ScriptResponse: result,
	}
}

func uploadFile(client *sftp.Client, localPath, remotePath string) (string, error) {
	// Open local file
	srcFile, err := os.Open(localPath)
	if err != nil {
		return "", fmt.Errorf("failed to open local file: %v", err)
	}
	defer srcFile.Close()

	// Create remote file
	dstFile, err := client.Create(remotePath)
	if err != nil {
		return "", fmt.Errorf("failed to create remote file: %v", err)
	}
	defer dstFile.Close()

	// Copy
	bytes, err := io.Copy(dstFile, srcFile)
	if err != nil {
		return "", fmt.Errorf("file transfer failed: %v", err)
	}

	return fmt.Sprintf("Uploaded %d bytes to %s", bytes, remotePath), nil
}

func downloadFile(client *sftp.Client, remotePath, localPath string) (string, error) {
	// Open remote file
	srcFile, err := client.Open(remotePath)
	if err != nil {
		return "", fmt.Errorf("failed to open remote file: %v", err)
	}
	defer srcFile.Close()

	// Create local directory if needed
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return "", fmt.Errorf("failed to create local directory: %v", err)
	}

	// Create local file
	dstFile, err := os.Create(localPath)
	if err != nil {
		return "", fmt.Errorf("failed to create local file: %v", err)
	}
	defer dstFile.Close()

	// Copy
	bytes, err := io.Copy(dstFile, srcFile)
	if err != nil {
		return "", fmt.Errorf("file transfer failed: %v", err)
	}

	return fmt.Sprintf("Downloaded %d bytes to %s", bytes, localPath), nil
}

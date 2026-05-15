package executors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"Shared/shared_models"
)

type OllamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
}

type OllamaResponse struct {
	Response string `json:"response"`
	Done     bool   `json:"done"`
}

func ExecuteOllamaStage(ctx context.Context, stage shared_models.OperationRequest) shared_models.OperationResponse {
	reqCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	fullPrompt := buildLLMPrompt(stage)
	reqBody := OllamaRequest{Model: "llama3.2", Prompt: fullPrompt, Stream: false}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return failureResponse(stage, fmt.Sprintf("marshal request: %v", err))
	}

	endpoint := runtimeConfig.OllamaURL
	if endpoint == "" {
		endpoint = "http://localhost:11434/api/generate"
	}

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return failureResponse(stage, fmt.Sprintf("create request: %v", err))
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		if reqCtx.Err() == context.DeadlineExceeded {
			return failureResponse(stage, "ollama request timed out after 120 seconds")
		}
		return failureResponse(stage, fmt.Sprintf("ollama request: %v", err))
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return failureResponse(stage, fmt.Sprintf("read response: %v", err))
	}

	var ollamaResp OllamaResponse
	if err := json.Unmarshal(body, &ollamaResp); err != nil {
		return failureResponse(stage, fmt.Sprintf("parse ollama response: %v", err))
	}

	return shared_models.OperationResponse{
		RequestId:   stage.RequestId,
		WorkflowId:  stage.WorkflowId,
		StageId:     stage.StageId,
		StageType:   stage.StageType,
		Attempt:     stage.Attempt,
		Status:      "SUCCESS",
		LlmResponse: ollamaResp.Response,
	}
}

func buildLLMPrompt(stage shared_models.OperationRequest) string {
	var b strings.Builder
	if stage.Context != "" {
		b.WriteString("Context:\n")
		b.WriteString(stage.Context)
		b.WriteString("\n\n")
	}
	if len(stage.ParentOutputs) > 0 {
		b.WriteString("Parent stage outputs:\n")
		for parent, out := range stage.ParentOutputs {
			b.WriteString(parent)
			b.WriteString(":\n")
			b.WriteString(out)
			b.WriteString("\n\n")
		}
	}
	b.WriteString("Task:\n")
	b.WriteString(stage.Prompt)
	return b.String()
}

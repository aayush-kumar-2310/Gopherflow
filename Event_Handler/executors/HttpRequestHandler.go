package executors

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"Shared/shared_models"
)

func ExecuteHTTPStage(ctx context.Context, stage shared_models.OperationRequest) shared_models.OperationResponse {
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	client := &http.Client{Timeout: 30 * time.Second}

	var reqBody io.Reader
	if stage.HttpPayload != "" {
		reqBody = bytes.NewBufferString(stage.HttpPayload)
	}

	req, err := http.NewRequestWithContext(reqCtx, stage.HttpMethod, stage.HttpEndpoint, reqBody)
	if err != nil {
		return failureResponse(stage, fmt.Sprintf("request creation: %v", err))
	}

	for key, value := range stage.HttpHeaders {
		req.Header.Set(key, value)
	}
	if stage.HttpPayload != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := client.Do(req)
	if err != nil {
		return failureResponse(stage, fmt.Sprintf("http request: %v", err))
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return failureResponse(stage, fmt.Sprintf("read response: %v", err))
	}

	if resp.StatusCode >= 400 {
		return failureResponse(stage, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(body)))
	}

	return shared_models.OperationResponse{
		RequestId:    stage.RequestId,
		WorkflowId:   stage.WorkflowId,
		StageId:      stage.StageId,
		StageType:    stage.StageType,
		Attempt:      stage.Attempt,
		Status:       "SUCCESS",
		Output:       string(body),
		HttpResponse: string(body),
	}
}

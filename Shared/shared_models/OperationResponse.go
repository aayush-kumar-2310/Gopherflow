package shared_models

type OperationResponse struct {
	RequestId      string    `json:"requestId"`
	WorkflowId     string    `json:"workflowId"`
	StageId        string    `json:"stageId"`
	StageType      StageType `json:"stageType"`
	Attempt        int       `json:"attempt"`
	Status         string    `json:"status"` // SUCCESS or FAILURE
	Output         string    `json:"output,omitempty"`
	Error          string    `json:"error,omitempty"`
	ScriptResponse string    `json:"scriptResponse,omitempty"`
	HttpResponse   string    `json:"httpResponse,omitempty"`
	LlmResponse    string    `json:"llmResponse,omitempty"`
}

// ResultOutput returns the primary payload for downstream stages.
func (r OperationResponse) ResultOutput() string {
	if r.Output != "" {
		return r.Output
	}
	if r.ScriptResponse != "" {
		return r.ScriptResponse
	}
	if r.HttpResponse != "" {
		return r.HttpResponse
	}
	return r.LlmResponse
}

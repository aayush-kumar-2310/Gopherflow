package shared_models

type OperationResponse struct {
	RequestId      string    `json:"requestId"`
	WorkflowId     string    `json:"workflowId"`
	StageId        string    `json:"stageId"`
	StageType      StageType `json:"stageType"`
	Status         string    `json:"status"` // e.g., "SUCCESS", "FAILURE"
	Output         string    `json:"output,omitempty"`
	Error          string    `json:"error,omitempty"`
	ScriptResponse string    `json:"scriptResponse,omitempty"`
	HttpResponse   string    `json:"httpResponse,omitempty"`
}

package shared_models

type StageOutput struct {
	Output         string `json:"output,omitempty"`
	Error          string `json:"error,omitempty"`
	ScriptResponse string `json:"scriptResponse,omitempty"`
	HttpResponse   string `json:"httpResponse,omitempty"`
	LlmResponse    string `json:"llmResponse,omitempty"`
}

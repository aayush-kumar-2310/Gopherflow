package shared_models

type OperationRequest struct {
	RequestId      string            `json:"requestId"`
	WorkflowId     string            `json:"workflowId"`
	StageId        string            `json:"stageId"`
	StageType      StageType         `json:"stageType"`
	Attempt        int               `json:"attempt"`
	Weight         int               `json:"weight"`
	Script         string            `json:"script,omitempty"`
	SFTPHost       string            `json:"sftpHost,omitempty"`
	SFTPPort       int               `json:"sftpPort,omitempty"`
	SFTPUsername   string            `json:"sftpUsername,omitempty"`
	SFTPPassword   string            `json:"sftpPassword,omitempty"`
	RemoteFilePath string            `json:"remoteFilePath,omitempty"`
	LocalFilePath  string            `json:"localFilePath,omitempty"`
	HttpEndpoint   string            `json:"httpEndpoint,omitempty"`
	HttpMethod     string            `json:"httpMethod,omitempty"`
	HttpPayload    string            `json:"httpPayload,omitempty"`
	HttpHeaders    map[string]string `json:"httpHeaders,omitempty"`
	Prompt         string            `json:"prompt,omitempty"`
	Context        string            `json:"context,omitempty"`
	ParentOutputs  map[string]string `json:"parentOutputs,omitempty"`
}

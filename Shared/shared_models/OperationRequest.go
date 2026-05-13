package shared_models

type StageType string

const (
	ExecuteScript StageType = "EXECUTE_SCRIPT"
	FetchSftp     StageType = "FETCH_SFTP"
	UploadSftp    StageType = "UPLOAD_SFTP"
	Summarize     StageType = "SUMMARIZE"
	HTTPRequest   StageType = "HTTP_REQUEST"
)

type OperationRequest struct {
	RequestId      string    `json:"requestId"`
	WorkflowId     string    `json:"workflowId"`
	StageId        string    `json:"stageId"`
	StageType      StageType `json:"stageType"`
	Script         string    `json:"script,omitempty"`
	RemoteFilePath string    `json:"remoteFilePath,omitempty"`
	LocalFilePath  string    `json:"localFilePath,omitempty"`
	HttpEndpoint   string    `json:"httpEndpoint,omitempty"`
	HttpMethod     string    `json:"httpMethod,omitempty"`
	HttpPayload    string    `json:"httpPayload,omitempty"`
}

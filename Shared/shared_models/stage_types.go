package shared_models

// StageType identifies what the Event_Handler should execute.
type StageType string

const (
	ExecuteScript StageType = "EXECUTE_SCRIPT"
	FetchSftp     StageType = "FETCH_SFTP"
	UploadSftp    StageType = "UPLOAD_SFTP"
	HTTPRequest   StageType = "HTTP_REQUEST"
	LLM           StageType = "LLM"
)

// StageStatus is stored in Redis during a workflow run.
type StageStatus string

const (
	StagePending         StageStatus = "PENDING"
	StageRunning         StageStatus = "RUNNING"
	StageFinished        StageStatus = "FIN"
	StageFailedRetry     StageStatus = "FAILED-RETRY"
	StageFailedExhausted StageStatus = "FAILED-EXHAUSTED"
	StageSkipped         StageStatus = "SKIPPED"
)

// RunStatus is the persisted workflow-run outcome in PostgreSQL.
type RunStatus string

const (
	RunRunning        RunStatus = "RUNNING"
	RunCompleted      RunStatus = "COMPLETED"
	RunPartialFailure RunStatus = "PARTIAL_FAILURE"
	RunFailed         RunStatus = "FAILED"
)

const MaxStageAttempts = 3

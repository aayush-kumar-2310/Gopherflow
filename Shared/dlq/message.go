package dlq

import "time"

// Message is published when a stage cannot be recovered.
type Message struct {
	WorkflowID string    `json:"workflowId"`
	RunID      string    `json:"runId"`
	StageID    string    `json:"stageId"`
	StageType  string    `json:"stageType,omitempty"`
	Attempt    int       `json:"attempt"`
	Error      string    `json:"error"`
	Reason     string    `json:"reason"`
	Source     string    `json:"source"`
	Payload    string    `json:"payload,omitempty"`
	At         time.Time `json:"at"`
}

const (
	ReasonRetryExhausted = "retry_exhausted"
	ReasonDispatchFailed = "dispatch_failed"
	ReasonUnsupported    = "unsupported_stage"
	ReasonInvalidMessage = "invalid_message"
)

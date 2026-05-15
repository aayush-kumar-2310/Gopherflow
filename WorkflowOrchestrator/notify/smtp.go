package notify

import (
	"fmt"
	"net/smtp"
	"strings"

	"Shared/config"
)

func SendStageFailure(cfg config.Config, workflowID, workflowName, runID, stageID, errMsg string) error {
	if !cfg.SMTPEnabled || cfg.SMTPTo == "" {
		return nil
	}

	subject := fmt.Sprintf("[Gopherflow] Stage failed: %s / %s", workflowID, stageID)
	body := strings.Builder{}
	body.WriteString("A workflow stage exhausted all retry attempts.\n\n")
	body.WriteString(fmt.Sprintf("Workflow ID:   %s\n", workflowID))
	if workflowName != "" {
		body.WriteString(fmt.Sprintf("Workflow name: %s\n", workflowName))
	}
	body.WriteString(fmt.Sprintf("Run ID:        %s\n", runID))
	body.WriteString(fmt.Sprintf("Stage ID:      %s\n", stageID))
	body.WriteString(fmt.Sprintf("Error:         %s\n", errMsg))

	msg := []byte(
		"From: " + cfg.SMTPFrom + "\r\n" +
			"To: " + cfg.SMTPTo + "\r\n" +
			"Subject: " + subject + "\r\n" +
			"Content-Type: text/plain; charset=UTF-8\r\n" +
			"\r\n" +
			body.String(),
	)

	addr := fmt.Sprintf("%s:%d", cfg.SMTPHost, cfg.SMTPPort)
	auth := smtp.PlainAuth("", cfg.SMTPUser, cfg.SMTPPassword, cfg.SMTPHost)
	return smtp.SendMail(addr, auth, cfg.SMTPFrom, []string{cfg.SMTPTo}, msg)
}

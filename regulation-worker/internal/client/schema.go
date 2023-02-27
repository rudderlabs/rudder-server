package client

type jobSchema struct {
	JobID          string                 `json:"jobId"`
	WorkspaceId    string                 `json:"workspaceId"`
	DestinationID  string                 `json:"destinationId"`
	UserAttributes []userAttributesSchema `json:"userAttributes"`
	FailedAttempts int                    `json:"failedAttempts"`
}

type statusJobSchema struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

type userAttributesSchema map[string]string

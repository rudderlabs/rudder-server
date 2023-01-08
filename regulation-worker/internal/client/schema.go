package client

type jobSchema struct {
	JobID          string                 `json:"jobId"`
	WorkspaceId    string                 `json:"workspaceId"`
	DestinationID  string                 `json:"destinationId"`
	UserAttributes []userAttributesSchema `json:"userAttributes"`
}

type statusJobSchema struct {
	Status string `json:"status"`
}

type userAttributesSchema map[string]string

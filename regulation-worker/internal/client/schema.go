package client

type singleTenantJobSchema struct {
	JobID          string                 `json:"jobId"`
	DestinationID  string                 `json:"destinationId"`
	UserAttributes []userAttributesSchema `json:"userAttributes"`
}

type multiTenantJobSchema struct {
	JobID          string                 `json:"jobId"`
	WorkspaceID    string                 `json:"workspaceId"`
	DestinationID  string                 `json:"destinationId"`
	UserAttributes []userAttributesSchema `json:"userAttributes"`
}

type statusJobSchema struct {
	Status string `json:"status"`
}

type userAttributesSchema map[string]string

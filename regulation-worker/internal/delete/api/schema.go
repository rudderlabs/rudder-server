package api

type userAttributesSchema struct {
	UserID string  `json:"userId"`
	Phone  *string `json:"phone,omitempty"`
	Email  *string `json:"email,omitempty"`
}

type apiDeletionPayloadSchema struct {
	JobID          string                 `json:"jobId"`
	DestType       string                 `json:"destType"`
	Config         map[string]interface{} `json:"config"`
	UserAttributes []userAttributesSchema `json:"userAttributes"`
}

type JobRespSchema struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

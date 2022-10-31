package api

type userAttributesSchema map[string]string

type apiDeletionPayloadSchema struct {
	JobID          string                 `json:"jobId"`
	DestType       string                 `json:"destType"`
	Config         map[string]interface{} `json:"config"`
	UserAttributes []userAttributesSchema `json:"userAttributes"`
}

type JobRespSchema struct {
	Status            string `json:"status"`
	Error             string `json:"error"`
	AuthErrorCategory string `json:"authErrorCategory"`
}

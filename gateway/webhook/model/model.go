package model

type FailedWebhookPayload struct {
	WriteKey   string
	Payload    []byte
	SourceType string
	Reason     string
}

package wunderkind

const (
	InvocationType       = "RequestResponse"
	WunderkindRegion     = "WUNDERKIND_REGION"
	WunderkindIamRoleArn = "WUNDERKIND_IAM_ROLE_ARN"
	WunderkindExternalId = "WUNDERKIND_EXTERNAL_ID"
	WunderkindLambda     = "WUNDERKIND_LAMBDA"
)

// Config is the config that is required to send data to Wunderkind Lambda
type destinationConfig struct {
	Lambda string `json:"lambda"`
}

type inputData struct {
	Payload string `json:"payload"`
}

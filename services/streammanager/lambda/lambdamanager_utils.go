package lambda

import "github.com/rudderlabs/rudder-go-kit/logger"

// Config is the config that is required to send data to Lambda
type destinationConfig struct {
	InvocationType string `json:"invocationType"`
	ClientContext  string `json:"clientContext"`
	Lambda         string `json:"lambda"`
}

type inputData struct {
	Payload string `json:"payload"`
}

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("lambda")
}

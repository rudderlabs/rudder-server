package kinesis

import "github.com/rudderlabs/rudder-go-kit/logger"

var pkgLogger logger.Logger

// Config is the config that is required to send data to Kinesis
type Config struct {
	Stream       string
	UseMessageID bool
}

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("kinesis")
}

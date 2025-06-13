package personalize

import "github.com/rudderlabs/rudder-go-kit/logger"

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("personalize")
}

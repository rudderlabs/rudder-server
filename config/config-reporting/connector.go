package config_reporting

import (
	"encoding/json"
	"net/url"
	"os"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/httpconnector"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

var (
	pkgLogger        logger.LoggerI
	configBackendURL string
)

func Init() {
	pkgLogger = logger.NewLogger().Child("config-reporting")
	loadConfig()
}

func loadConfig() {
	configBackendURL = config.GetEnv("CONFIG_BACKEND_URL", "https://api.rudderlabs.com")
}

// ReportDataPlaneConfig sends the data-plane information to the control-plane.
func ReportDataPlaneConfig() error {
	planeConfig := dataPlaneConfig{
		Namespace: os.Getenv("NAMESPACE"),
	}
	rawJson, err := json.Marshal(planeConfig)
	if err != nil {
		return err
	}
	parsedURL, err := url.Parse(configBackendURL)
	if err != nil {
		pkgLogger.Errorf("Failed to get the control plane host URL to report the data-plane config: %s", err.Error())
	}
	parsedURL.Path = "data-plane-planeConfig"
	op := func() error {
		return httpconnector.MakeHTTPPostRequest(parsedURL.String(), rawJson)
	}
	backoffWithMaxRetry := backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3)
	err = backoff.RetryNotify(op, backoffWithMaxRetry, func(err error, t time.Duration) {
		pkgLogger.Errorf("Failed to report data-plane Config to control-plane with error: %s, retrying after: %v",
			err.Error(), t)
	})
	if err != nil {
		pkgLogger.Error("Error reporting data-plane planeConfig to the control-plane", err)
		return err
	}
	return nil
}

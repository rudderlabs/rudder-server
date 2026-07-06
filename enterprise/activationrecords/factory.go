package activationrecords

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

// Factory constructs an ActivationRecordsReporter.
type Factory struct {
	Log logger.Logger
}

// Setup returns a NoopActivationRecordsReporter when the feature is disabled,
// otherwise a UniqueActivationRecordsReporter.
func (f *Factory) Setup(conf *config.Config) (ActivationRecordsReporter, error) {
	if !conf.GetBoolVar(false, "ActivationRecords.enabled") {
		return NewNoopActivationRecordsReporter(), nil
	}
	return NewUniqueActivationRecordsReporter(f.Log, conf, stats.Default)
}

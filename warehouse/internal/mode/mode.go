package mode

import "github.com/rudderlabs/rudder-go-kit/config"

const DegradedMode = "degraded"

func IsStandAlone(mode string) bool {
	switch mode {
	case config.EmbeddedMode, config.EmbeddedMasterMode:
		return false
	default:
		return true
	}
}

func IsMaster(mode string) bool {
	switch mode {
	case config.MasterMode, config.MasterSlaveMode, config.EmbeddedMode, config.EmbeddedMasterMode:
		return true
	default:
		return false
	}
}

func IsSlave(mode string) bool {
	switch mode {
	case config.SlaveMode, config.MasterSlaveMode, config.EmbeddedMode:
		return true
	default:
		return false
	}
}

func IsStandAloneSlave(mode string) bool {
	return mode == config.SlaveMode
}

func IsDegraded(runningMode string) bool {
	return runningMode == DegradedMode
}

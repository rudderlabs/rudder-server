package warehouse

import "github.com/rudderlabs/rudder-go-kit/config"

const degradedMode = "degraded"

func isStandAlone(mode string) bool {
	switch mode {
	case config.EmbeddedMode, config.EmbeddedMasterMode:
		return false
	default:
		return true
	}
}

func isMaster(mode string) bool {
	switch mode {
	case config.MasterMode, config.MasterSlaveMode, config.EmbeddedMode, config.EmbeddedMasterMode:
		return true
	default:
		return false
	}
}

func isSlave(mode string) bool {
	switch mode {
	case config.SlaveMode, config.MasterSlaveMode, config.EmbeddedMode:
		return true
	default:
		return false
	}
}

func isStandAloneSlave(mode string) bool {
	return mode == config.SlaveMode
}

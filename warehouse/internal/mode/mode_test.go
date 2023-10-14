package mode_test

import (
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/internal/mode"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
)

func TestIsStandAlone(t *testing.T) {
	testCases := []struct {
		name         string
		isStandAlone bool
	}{
		{
			name:         config.EmbeddedMode,
			isStandAlone: false,
		},
		{
			name:         config.EmbeddedMasterMode,
			isStandAlone: false,
		},
		{
			name:         config.MasterMode,
			isStandAlone: true,
		},
		{
			name:         config.MasterSlaveMode,
			isStandAlone: true,
		},
		{
			name:         config.SlaveMode,
			isStandAlone: true,
		},
		{
			name:         config.OffMode,
			isStandAlone: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, mode.IsStandAlone(tc.name), tc.isStandAlone)
		})
	}
}

func TestIsMaster(t *testing.T) {
	testCases := []struct {
		name     string
		isMaster bool
	}{
		{
			name:     config.EmbeddedMode,
			isMaster: true,
		},
		{
			name:     config.EmbeddedMasterMode,
			isMaster: true,
		},
		{
			name:     config.MasterMode,
			isMaster: true,
		},
		{
			name:     config.MasterSlaveMode,
			isMaster: true,
		},
		{
			name:     config.SlaveMode,
			isMaster: false,
		},
		{
			name:     config.OffMode,
			isMaster: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, mode.IsMaster(tc.name), tc.isMaster)
		})
	}
}

func TestIsSlave(t *testing.T) {
	testCases := []struct {
		name    string
		isSlave bool
	}{
		{
			name:    config.EmbeddedMode,
			isSlave: true,
		},
		{
			name:    config.EmbeddedMasterMode,
			isSlave: false,
		},
		{
			name:    config.MasterMode,
			isSlave: false,
		},
		{
			name:    config.MasterSlaveMode,
			isSlave: true,
		},
		{
			name:    config.SlaveMode,
			isSlave: true,
		},
		{
			name:    config.OffMode,
			isSlave: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, mode.IsSlave(tc.name), tc.isSlave)
		})
	}
}

func TestIsStandAloneSlave(t *testing.T) {
	testCases := []struct {
		name              string
		isStandAloneSlave bool
	}{
		{
			name:              config.EmbeddedMode,
			isStandAloneSlave: false,
		},
		{
			name:              config.EmbeddedMasterMode,
			isStandAloneSlave: false,
		},
		{
			name:              config.MasterMode,
			isStandAloneSlave: false,
		},
		{
			name:              config.MasterSlaveMode,
			isStandAloneSlave: false,
		},
		{
			name:              config.SlaveMode,
			isStandAloneSlave: true,
		},
		{
			name:              config.OffMode,
			isStandAloneSlave: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, mode.IsStandAloneSlave(tc.name), tc.isStandAloneSlave)
		})
	}
}

func TestIsDegraded(t *testing.T) {
	testCases := []struct {
		name        string
		runningMode string
		isDegraded  bool
	}{
		{
			name:        "normal",
			runningMode: "",
			isDegraded:  false,
		},
		{
			name:        "degraded",
			runningMode: mode.DegradedMode,
			isDegraded:  true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, mode.IsDegraded(tc.runningMode), tc.isDegraded)
		})
	}
}

package manager

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

const (
	testDestinationID = "destination-1"
	testWorkspaceID   = "workspace-1"
)

func clickhouseKey(scope ...string) string {
	if len(scope) == 0 {
		return "Warehouse.clickhouse.useV2Driver"
	}
	return "Warehouse.clickhouse." + scope[0] + ".useV2Driver"
}

func TestUseV2Driver(t *testing.T) {
	testCases := []struct {
		name     string
		set      map[string]bool
		expected bool
	}{
		{
			name:     "nothing set defaults to v1",
			expected: false,
		},
		{
			name:     "global on",
			set:      map[string]bool{clickhouseKey(): true},
			expected: true,
		},
		{
			name:     "workspace on ahead of the global flag",
			set:      map[string]bool{clickhouseKey(testWorkspaceID): true},
			expected: true,
		},
		{
			name:     "destination on ahead of its workspace",
			set:      map[string]bool{clickhouseKey(testDestinationID): true},
			expected: true,
		},
		{
			// The case a rollout depends on: one destination misbehaves after
			// the global flag is on and has to go back to v1 on its own.
			name:     "destination off wins over the global flag",
			set:      map[string]bool{clickhouseKey(): true, clickhouseKey(testDestinationID): false},
			expected: false,
		},
		{
			name:     "destination off wins over its workspace",
			set:      map[string]bool{clickhouseKey(testWorkspaceID): true, clickhouseKey(testDestinationID): false},
			expected: false,
		},
		{
			name:     "workspace off wins over the global flag",
			set:      map[string]bool{clickhouseKey(): true, clickhouseKey(testWorkspaceID): false},
			expected: false,
		},
		{
			name:     "destination on wins over its workspace being off",
			set:      map[string]bool{clickhouseKey(testWorkspaceID): false, clickhouseKey(testDestinationID): true},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conf := config.New()
			for k, v := range tc.set {
				conf.Set(k, v)
			}

			warehouse := model.Warehouse{
				WorkspaceID: testWorkspaceID,
				Destination: backendconfig.DestinationT{ID: testDestinationID},
			}
			require.Equal(t, tc.expected, useV2Driver(conf, warehouse))
		})
	}

	t.Run("unidentified warehouse falls back to the global flag", func(t *testing.T) {
		// createDummyWarehouse and the router tests build warehouses with no
		// ids. The empty ones have to be skipped rather than turned into keys,
		// or they would all collide on "Warehouse.clickhouse..useV2Driver".
		conf := config.New()
		conf.Set(clickhouseKey(), true)
		require.True(t, useV2Driver(conf, model.Warehouse{}))

		require.False(t, useV2Driver(config.New(), model.Warehouse{}))
	})
}

func TestClickhouseSelector(t *testing.T) {
	warehouse := model.Warehouse{
		WorkspaceID: testWorkspaceID,
		Destination: backendconfig.DestinationT{ID: testDestinationID},
	}

	t.Run("selects per destination", func(t *testing.T) {
		testCases := []struct {
			name     string
			set      map[string]bool
			expected any
		}{
			{"defaults to v1", nil, &clickhouse.Clickhouse{}},
			{"global on", map[string]bool{clickhouseKey(): true}, &clickhouse.ClickhouseV2{}},
			{
				"destination off under a global on",
				map[string]bool{clickhouseKey(): true, clickhouseKey(testDestinationID): false},
				&clickhouse.Clickhouse{},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				conf := config.New()
				for k, v := range tc.set {
					conf.Set(k, v)
				}

				s, ok := newClickhouse(conf, logger.NOP, stats.NOP).(*clickhouseSelector)
				require.True(t, ok)
				require.IsType(t, tc.expected, s.selectFor(warehouse))
			})
		}
	})

	t.Run("before any selection it is v1", func(t *testing.T) {
		// Nothing should reach the embedded field before a warehouse-carrying
		// method has run, but if it does it must not be nil.
		s, ok := newClickhouse(config.New(), logger.NOP, stats.NOP).(*clickhouseSelector)
		require.True(t, ok)
		require.IsType(t, &clickhouse.Clickhouse{}, s.WarehouseOperations)
	})

	// Guards the invariant documented on clickhouseSelector. A new
	// WarehouseOperations method that carries a warehouse needs its own
	// override, or it would silently run v1 against a v2 destination.
	t.Run("every method carrying a warehouse is overridden", func(t *testing.T) {
		overridden := map[string]bool{
			"Setup":          true,
			"Connect":        true,
			"IsEmpty":        true,
			"TestConnection": true,
		}

		iface := reflect.TypeOf((*WarehouseOperations)(nil)).Elem()
		warehouseType := reflect.TypeOf(model.Warehouse{})

		carrying := make(map[string]bool)
		for i := 0; i < iface.NumMethod(); i++ {
			method := iface.Method(i)
			for j := 0; j < method.Type.NumIn(); j++ {
				if method.Type.In(j) == warehouseType {
					carrying[method.Name] = true
					break
				}
			}
		}

		require.Equal(t, overridden, carrying,
			"a WarehouseOperations method takes a model.Warehouse; give clickhouseSelector an override that calls selectFor, then add it here",
		)
	})
}

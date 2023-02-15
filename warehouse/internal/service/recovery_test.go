package service_test

import (
	"context"
	"fmt"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

type mockRepo struct {
	m   map[string][]string
	err error
}

type mockDestination struct {
	recovered int
	err       error
}

func (r *mockRepo) InterruptedDestinations(_ context.Context, destinationType string) ([]string, error) {
	return r.m[destinationType], r.err
}

func (d *mockDestination) CrashRecover(_ warehouseutils.Warehouse) error {
	d.recovered += 1
	return d.err
}

func TestRecovery(t *testing.T) {
	testCases := []struct {
		name          string
		whType        string
		destinationID string

		recovery bool

		repoErr error
		destErr error
		wantErr error
	}{
		{
			name:          "interrupted postgres warehouse",
			whType:        warehouseutils.POSTGRES,
			destinationID: "1",
			recovery:      true,
		},
		{
			name:          "non-interrupted postgres warehouse",
			whType:        warehouseutils.POSTGRES,
			destinationID: "3",
			recovery:      false,
		},
		{
			name:          "interrupted snowflake - skipped - warehouse",
			whType:        warehouseutils.SNOWFLAKE,
			destinationID: "6",
			recovery:      false,
		},

		{
			name:          "repo error",
			whType:        warehouseutils.POSTGRES,
			destinationID: "1",
			repoErr:       fmt.Errorf("repo error"),
			wantErr:       fmt.Errorf("repo interrupted destinations: repo error"),
		},
		{
			name:          "destination error",
			whType:        warehouseutils.POSTGRES,
			destinationID: "1",
			recovery:      true,
			destErr:       fmt.Errorf("dest error"),
			wantErr:       fmt.Errorf("dest error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &mockRepo{
				m: map[string][]string{
					warehouseutils.POSTGRES:  {"1", "2"},
					warehouseutils.SNOWFLAKE: {"6", "8"},
				},
				err: tc.repoErr,
			}

			d := &mockDestination{
				err: tc.destErr,
			}

			recovery := service.NewRecovery(tc.whType, repo)

			for i := 0; i < 2; i++ {
				err := recovery.Recover(context.Background(), d, warehouseutils.Warehouse{
					Destination: backendconfig.DestinationT{
						ID: tc.destinationID,
					},
				})

				if tc.wantErr != nil {
					require.EqualError(t, err, tc.wantErr.Error())
				} else {
					require.NoError(t, err)
				}
			}

			if tc.recovery {
				require.Equal(t, 1, d.recovered)
			} else {
				require.Equal(t, 0, d.recovered)
			}
		})
	}
}

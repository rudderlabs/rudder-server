package service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type mockRepo struct {
	m   map[string][]string
	err error
}

type mockDestination struct {
	recovered   int
	recoveryErr error
}

func (r *mockRepo) InterruptedDestinations(_ context.Context, destinationType string) ([]string, error) {
	return r.m[destinationType], r.err
}

func (d *mockDestination) CrashRecover(_ context.Context) error {
	d.recovered += 1
	return d.recoveryErr
}

func TestRecovery(t *testing.T) {
	testCases := []struct {
		name          string
		whType        string
		destinationID string
		repoErr       error
		destErr       error

		recoveryCalled bool
		wantErr        error
	}{
		{
			name:           "interrupted mssql warehouse",
			whType:         warehouseutils.MSSQL,
			destinationID:  "1",
			recoveryCalled: true,
		},
		{
			name:           "non-interrupted mssql warehouse",
			whType:         warehouseutils.MSSQL,
			destinationID:  "3",
			recoveryCalled: false,
		},
		{
			name:           "interrupted snowflake - skipped - warehouse",
			whType:         warehouseutils.SNOWFLAKE,
			destinationID:  "6",
			recoveryCalled: false,
		},

		{
			name:          "error during recovery detection",
			whType:        warehouseutils.MSSQL,
			destinationID: "1",
			repoErr:       fmt.Errorf("repo error"),
			wantErr:       fmt.Errorf("detection: repo interrupted destinations: repo error"),
		},
		{
			name:          "error during recovery",
			whType:        warehouseutils.MSSQL,
			destinationID: "2",
			repoErr:       nil,
			destErr:       fmt.Errorf("destination error"),

			recoveryCalled: true,
			wantErr:        fmt.Errorf("crash recover: destination error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			repo := &mockRepo{
				m: map[string][]string{
					warehouseutils.MSSQL:     {"1", "2"},
					warehouseutils.SNOWFLAKE: {"6", "8"},
				},
				err: tc.repoErr,
			}

			d := &mockDestination{
				recoveryErr: tc.destErr,
			}

			recovery := service.NewRecovery(tc.whType, repo)

			for i := 0; i < 2; i++ {
				err := recovery.Recover(context.Background(), d, model.Warehouse{
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

			if tc.recoveryCalled {
				require.Equal(t, 1, d.recovered)
			} else {
				require.Equal(t, 0, d.recovered)
			}
		})
	}
}

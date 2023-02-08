package warehouse_test

import (
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/stretchr/testify/require"
)

func TestErrorHandler_MatchErrorMappings(t *testing.T) {
	testCases := []struct {
		name        string
		destType    string
		err         error
		expectedTag warehouse.Tag
	}{}

	for _, tc := range testCases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			m, err := manager.New(tc.destType)
			require.NoError(t, err)

			er := &warehouse.ErrorHandler{Manager: m}
			tag := er.MatchErrorMappings(tc.err)
			require.Equal(t, tag, tc.expectedTag)
		})
	}
}

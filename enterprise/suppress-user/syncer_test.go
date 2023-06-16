package suppression

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
)

func TestSyncer(t *testing.T) {
	t.Run("panic", func(t *testing.T) {
		require.Panics(t, func() {
			MustNewSyncer(
				"", &identity.NOOP{}, nil,
				WithHttpClient(&http.Client{}),
				WithPageSize(1),
				WithLogger(logger.NOP))
		})
	})
	t.Run("options", func(t *testing.T) {
		httpClient := &http.Client{}
		pageSize := 1
		log := logger.NOP
		s := MustNewSyncer(
			"", &identity.Workspace{}, nil,
			WithHttpClient(httpClient),
			WithPageSize(pageSize),
			WithLogger(log))

		require.Equal(t, httpClient, s.client)
		require.Equal(t, pageSize, s.pageSize)
		require.Equal(t, log, s.log)
	})
}

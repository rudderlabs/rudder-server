package batchrouter

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

func TestNewAsyncDestinations(t *testing.T) {
	const retryInterval = time.Minute
	now := time.Now()
	manager := &fakeAsyncManager{}
	invalid := func(age time.Duration) *asynccommon.InvalidManager {
		return &asynccommon.InvalidManager{Error: errors.New("invalid"), FailedAt: now.Add(-age)}
	}
	for _, tc := range []struct {
		name     string
		previous asynccommon.AsyncDestinationManager // nil for a destination that wasn't in the config
		revision string
		keeps    bool
	}{
		{name: "a destination that wasn't in the config gets a manager", revision: "rev-1"},
		{name: "the manager is kept while the revision is the same", previous: manager, revision: "rev-1", keeps: true},
		{name: "a revision gets its own manager", previous: manager, revision: "rev-2"},
		{name: "an invalid manager is kept until its retry is due", previous: invalid(retryInterval / 2), revision: "rev-1", keeps: true},
		{name: "an invalid manager is replaced when its retry is due", previous: invalid(retryInterval), revision: "rev-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			created := &fakeAsyncManager{}
			brt := defaultHandle("ELOQUA")
			brt.now = func() time.Time { return now }
			brt.invalidManagerRetryInterval = config.SingleValueLoader(retryInterval)
			brt.asyncManagerFactory = func(*config.Config, logger.Logger, stats.Stats, *backendconfig.DestinationT, backendconfig.BackendConfig, oauthv2.SecretVersionProvider) (asynccommon.AsyncDestinationManager, error) {
				return created, nil
			}
			if tc.previous != nil {
				brt.asyncDestinations = map[string]*asyncDestination{"destination-1": {
					DestinationWithSources: routerutils.DestinationWithSources{Destination: backendconfig.DestinationT{ID: "destination-1", RevisionID: "rev-1"}},
					manager:                tc.previous,
				}}
			}

			destinations := brt.newAsyncDestinations(
				map[string]*routerutils.DestinationWithSources{"destination-1": {Destination: backendconfig.DestinationT{ID: "destination-1", RevisionID: tc.revision}}},
				map[string]time.Duration{"destination-1": time.Hour},
			)

			require.Len(t, destinations, 1)
			dest := destinations["destination-1"]
			require.Equal(t, tc.revision, dest.Destination.RevisionID)
			require.Equal(t, time.Hour, dest.uploadInterval)
			if tc.keeps {
				require.Same(t, tc.previous, dest.manager)
			} else {
				require.Same(t, created, dest.manager)
			}
		})
	}
}

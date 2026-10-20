package v2_test

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mockoauthv2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	rtTf "github.com/rudderlabs/rudder-server/router/transformer"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	cntx "github.com/rudderlabs/rudder-server/services/oauth/v2/context"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
	httpClient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// A client built for the delete flow builds a handler for the delete flow: the inactive sample it reports
// carries flowType="delete".
func TestClientBuildsHandlerWithItsFlow(t *testing.T) {
	ctrl := gomock.NewController(t)
	statsStore, err := memstats.New()
	require.NoError(t, err)

	const (
		accountID     = "delete-account-id"
		destinationID = "delete-destination-id"
		workspaceID   = "delete-workspace-id"
		destType      = "test-definition-name"
	)

	cache := v2.NewOauthTokenCache()
	cache.Store(accountID, v2.OAuthToken{
		Secret:         []byte(`{"access_token":"cached-token"}`),
		ExpirationDate: time.Now().Add(24 * time.Hour).UTC().Format(misc.RFC3339Milli),
	})

	const originalResponse = `{"output":[{"authErrorCategory":"AUTH_STATUS_INACTIVE"}]}`
	mockRoundTrip := mockoauthv2.NewMockRoundTripper(ctrl)
	mockRoundTrip.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewReader([]byte(originalResponse))),
	}, nil)

	// no OAuthHandler is injected: the client builds one, which must carry the client's flow
	optionalArgs := &httpClient.HttpClientOptionalArgs{
		Transport:           mockRoundTrip,
		Augmenter:           extensions.HeaderAugmenter,
		Locker:              kitsync.NewPartitionRWLocker(),
		OAuthBreakerOptions: &v2.OAuthBreakerOptions{},
	}

	// the handler the client builds captures stats.Default at construction; swap a store in for
	// exactly that window and restore the global before the request runs
	client := func() *http.Client {
		previousStats := stats.Default
		stats.Default = statsStore
		defer func() { stats.Default = previousStats }()

		return httpClient.NewOAuthHttpClient(
			&http.Client{},
			common.RudderFlowDelete,
			&cache,
			backendconfig.DefaultBackendConfig,
			rtTf.GetAuthErrorCategoryFromTransformResponse,
			optionalArgs,
		)
	}()

	destination := &backendconfig.DestinationT{
		ID:            destinationID,
		WorkspaceID:   workspaceID,
		Config:        map[string]any{"rudderDeleteAccountId": accountID},
		DeleteAccount: &backendconfig.Account{ID: accountID},
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: destType,
			Config: map[string]any{
				"auth": map[string]any{
					"type":         "OAuth",
					"rudderScopes": []any{"delete"},
				},
			},
		},
	}
	req, err := http.NewRequest(http.MethodPost, "http://example.com", bytes.NewReader([]byte(`{}`)))
	require.NoError(t, err)
	req = req.WithContext(cntx.CtxWithDestination(req.Context(), destination))

	res, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, res)
	defer func() { _ = res.Body.Close() }()

	require.Equal(t, http.StatusOK, res.StatusCode)
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	var response v2.TransportResponse
	require.NoError(t, jsonrs.Unmarshal(body, &response))
	require.Equal(t, originalResponse, response.OriginalResponse)
	require.Equal(t, http.StatusBadRequest, response.InterceptorResponse.StatusCode)

	metrics := statsStore.GetByName("oauth_action_request")
	require.Len(t, metrics, 1)
	require.Equal(t, stats.Tags{
		"id":              accountID,
		"destinationId":   destinationID,
		"workspaceId":     workspaceID,
		"rudderCategory":  "destination",
		"isCallToCpApi":   "false",
		"authErrCategory": common.CategoryAuthStatusInactive,
		"destType":        destType,
		"flowType":        "delete",
		"action":          "auth_status_inactive",
		"oauthVersion":    "v2",
		"errorMessage":    common.CategoryAuthStatusInactive,
	}, metrics[0].Tags)
	require.EqualValues(t, 1, metrics[0].Value)

	// the token fetch that served the cached token carries the same flow, and no destinationId
	timers := statsStore.GetByName("oauth_action_total_latency")
	require.Len(t, timers, 1)
	require.Equal(t, "delete", timers[0].Tags["flowType"])
	require.Equal(t, "fetch_token", timers[0].Tags["action"])
	require.NotContains(t, timers[0].Tags, "destinationId")
	require.Empty(t, statsStore.GetByName("oauth_action_request_sent"))
	require.Empty(t, statsStore.GetByName("oauth_action_proactive_token_refresh"))
}

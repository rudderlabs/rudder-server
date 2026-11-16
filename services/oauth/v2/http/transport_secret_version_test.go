package v2_test

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mockoauthv2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	cntx "github.com/rudderlabs/rudder-server/services/oauth/v2/context"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
	httpClient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
)

func TestTransportPassesTheAccountSecretVersion(t *testing.T) {
	for _, flow := range []common.RudderFlow{common.RudderFlowDelivery, common.RudderFlowDelete} {
		t.Run(string(flow), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			handler := mockoauthv2.NewMockOAuthHandler(ctrl)
			handler.EXPECT().FetchToken(gomock.Any()).
				Do(func(params *v2.OAuthTokenParams) {
					require.Equal(t, "acct-"+string(flow), params.AccountID)
					require.Equal(t, map[common.RudderFlow]int{common.RudderFlowDelivery: 8, common.RudderFlowDelete: 3}[flow], params.SecretVersion)
				}).
				// Return, not DoAndReturn: the mock asserts json.RawMessage and a []byte from DoAndReturn would read as nil
				Return([]byte(`{"access_token":"t"}`), nil)
			roundTripper := mockoauthv2.NewMockRoundTripper(ctrl)
			roundTripper.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{}`))),
			}, nil).Times(1)
			cache := v2.NewOauthTokenCache()
			transport := httpClient.NewOAuthTransport(&httpClient.TransportArgs{
				FlowType:             flow,
				TokenCache:           &cache,
				GetAuthErrorCategory: func([]byte) (string, error) { return "", nil },
				Augmenter:            extensions.HeaderAugmenter,
				OAuthHandler:         handler,
				OriginalTransport:    roundTripper,
			})

			req, err := http.NewRequest(http.MethodPost, "http://example.com", bytes.NewReader([]byte(`{}`)))
			require.NoError(t, err)
			req = req.WithContext(cntx.CtxWithDestination(req.Context(), &backendconfig.DestinationT{
				ID:              "dest-1",
				DeliveryAccount: &backendconfig.Account{ID: "acct-delivery", SecretVersion: 8},
				DeleteAccount:   &backendconfig.Account{ID: "acct-delete", SecretVersion: 3},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Config: map[string]any{"auth": map[string]any{"type": "OAuth", "rudderScopes": []any{"delivery", "delete"}}},
				},
			}))
			res, err := transport.RoundTrip(req)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, res.StatusCode, "the token must have reached the augmenter and the request the destination")
			require.NoError(t, res.Body.Close())
		})
	}
}

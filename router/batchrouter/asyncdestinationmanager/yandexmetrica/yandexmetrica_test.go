package yandexmetrica_test

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mockoauthv2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/yandexmetrica"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/yandexmetrica/augmenter"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	oauthv2common "github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	httpClient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
)

var (
	currentDir, _ = os.Getwd()
	destination   = &backendconfig.DestinationT{
		ID:   "1",
		Name: "YANDEX_METRICA_OFFLINE_EVENTS",
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: "YANDEX_METRICA_OFFLINE_EVENTS",
			Config: map[string]any{
				"auth": map[string]any{
					"type": "OAuth",
				},
			},
		},
		Config: map[string]any{
			"rudderAccountId": "1234",
			"goalId":          "1234",
		},
		DeliveryAccount: &backendconfig.Account{
			ID: "1234",
		},
		Enabled:     true,
		WorkspaceID: "1",
	}
)

func TestYandexMetrica(t *testing.T) {
	t.Run("NewManager function test", func(t *testing.T) {
		t.Run("should return yandexmetrica manager", func(t *testing.T) {
			yandexmetrica, err := yandexmetrica.NewManager(config.New(), logger.NOP, stats.NOP, destination, backendconfig.DefaultBackendConfig, nil)
			require.NoError(t, err)
			require.NotNil(t, yandexmetrica)
		})
	})
	t.Run("Upload function test", func(t *testing.T) {
		t.Run("Testing a successful scenario", func(t *testing.T) {
			cache := oauthv2.NewOauthTokenCache()
			ctrl := gomock.NewController(t)
			mockRoundTrip := mockoauthv2.NewMockRoundTripper(ctrl)
			mockRoundTrip.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"uploading":{"id":719450782,"source_quantity":11,"line_quantity":11,"client_id_type":"USER_ID","status":"UPLOADED"}}`))),
			}, nil)

			mockCpConnector := mockoauthv2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"dummyID","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"dummyWorkspaceID","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"dummyUserID","metadata":{"userId":"dummyUserID","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"accessToken":"correctAccessToken","refreshToken":"dummyRefreshToken"}}`)
			mockAuthIdentityProvider := mockoauthv2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)

			// Invoke code under test
			oauthHandler := oauthv2.NewOAuthHandler(mockAuthIdentityProvider,
				oauthv2.WithCache(oauthv2.NewOauthTokenCache()),
				oauthv2.WithLocker(kitsync.NewPartitionRWLocker()),
				oauthv2.WithStats(stats.NOP),
				oauthv2.WithLogger(logger.NOP),
				oauthv2.WithCpClient(mockCpConnector),
			)
			optionalArgs := httpClient.HttpClientOptionalArgs{
				Transport:    mockRoundTrip,
				Augmenter:    augmenter.YandexReqAugmenter,
				OAuthHandler: oauthHandler,
			}
			httpClient := httpClient.NewOAuthHttpClient(&http.Client{}, oauthv2common.RudderFlowDelivery, &cache, backendconfig.DefaultBackendConfig, augmenter.GetAuthErrorCategoryForYandex, &optionalArgs)
			yandexmetrica, _ := yandexmetrica.NewManager(config.New(), logger.NOP, stats.NOP, destination, backendconfig.DefaultBackendConfig, nil)
			yandexmetrica.Client = httpClient
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     destination,
				Manager:         yandexmetrica,
			}
			res := yandexmetrica.Upload(t.Context(), &asyncDestination)
			require.Equal(t, "{\"uploading\":{\"id\":719450782,\"source_quantity\":11,\"line_quantity\":11,\"client_id_type\":\"USER_ID\",\"status\":\"UPLOADED\"}}", res.SuccessResponse)
			require.Equal(t, []int64{1, 2, 3, 4}, res.SucceededJobIDs)
		})
		t.Run("Testing a failure scenario when the accessToken is invalid", func(t *testing.T) {
			cache := oauthv2.NewOauthTokenCache()
			ctrl := gomock.NewController(t)
			mockRoundTrip := mockoauthv2.NewMockRoundTripper(ctrl)
			mockRoundTrip.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
				StatusCode: 403,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"errors":[{"error_type":"invalid_token","message":"Invalid oauth_token"}],"code":403,"message":"Invalid oauth_token"}`))),
			}, nil)

			mockCpConnector := mockoauthv2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"dummyID","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"dummyWorkspaceID","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"dummyUserID","metadata":{"userId":"dummyUserID","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"accessToken":"expiredAccessToken","refreshToken":"dummyRefreshToken"}}`)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"dummyID","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"dummyWorkspaceID","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"dummyUserID","metadata":{"userId":"dummyUserID","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"accessToken":"newAccessToken","refreshToken":"dummyRefreshToken"}}`)
			mockAuthIdentityProvider := mockoauthv2.NewMockAuthIdentityProvider(ctrl)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
			mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
			// Invoke code under test
			oauthHandler := oauthv2.NewOAuthHandler(mockAuthIdentityProvider,
				oauthv2.WithCache(oauthv2.NewOauthTokenCache()),
				oauthv2.WithLocker(kitsync.NewPartitionRWLocker()),
				oauthv2.WithStats(stats.NOP),
				oauthv2.WithLogger(logger.NOP),
				oauthv2.WithCpClient(mockCpConnector),
			)
			optionalArgs := httpClient.HttpClientOptionalArgs{
				Transport:    mockRoundTrip,
				Augmenter:    augmenter.YandexReqAugmenter,
				OAuthHandler: oauthHandler,
			}
			httpClient := httpClient.NewOAuthHttpClient(&http.Client{}, oauthv2common.RudderFlowDelivery, &cache, backendconfig.DefaultBackendConfig, augmenter.GetAuthErrorCategoryForYandex, &optionalArgs)
			yandexmetrica, _ := yandexmetrica.NewManager(config.New(), logger.NOP, stats.NOP, destination, backendconfig.DefaultBackendConfig, nil)
			yandexmetrica.Client = httpClient
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     destination,
				Manager:         yandexmetrica,
			}
			res := yandexmetrica.Upload(t.Context(), &asyncDestination)
			require.Equal(t, "got non 200 response from the destination {\"errors\":[{\"error_type\":\"invalid_token\",\"message\":\"Invalid oauth_token\"}],\"code\":403,\"message\":\"Invalid oauth_token\"}", res.AbortReason)
			require.Equal(t, []int64{1, 2, 3, 4}, res.AbortJobIDs)
		})
	})
}

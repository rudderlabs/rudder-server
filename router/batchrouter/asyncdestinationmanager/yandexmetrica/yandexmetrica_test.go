package yandexmetrica_test

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

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
			Config: map[string]interface{}{
				"auth": map[string]interface{}{
					"type": "OAuth",
				},
			},
		},
		Config: map[string]interface{}{
			"rudderAccountId": "1234",
			"goalId":          "1234",
		},
		Enabled:     true,
		WorkspaceID: "1",
	}
)

var _ = Describe("Antisymmetric", func() {
	Describe("NewManager function test", func() {
		It("should return yandexmetrica manager", func() {
			yandexmetrica, err := yandexmetrica.NewManager(logger.NOP, stats.NOP, destination, backendconfig.DefaultBackendConfig)
			Expect(err).To(BeNil())
			Expect(yandexmetrica).NotTo(BeNil())
		})
	})
	Describe("Upload function test", func() {
		It("Testing a successful scenario", func() {
			cache := oauthv2.NewCache()
			ctrl := gomock.NewController(GinkgoT())
			mockRoundTrip := mockoauthv2.NewMockRoundTripper(ctrl)
			mockRoundTrip.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"uploading":{"id":719450782,"source_quantity":11,"line_quantity":11,"client_id_type":"USER_ID","status":"UPLOADED"}}`))),
			}, nil)

			mockCpConnector := mockoauthv2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"dummyID","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"dummyWorkspaceID","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"dummyUserID","metadata":{"userId":"dummyUserID","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"accessToken":"correctAccessToken","refreshToken":"dummyRefreshToken"}}`)
			mockTokenProvider := mockoauthv2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)

			// Invoke code under test
			oauthHandler := oauthv2.NewOAuthHandler(mockTokenProvider,
				oauthv2.WithCache(oauthv2.NewCache()),
				oauthv2.WithLocker(kitsync.NewPartitionRWLocker()),
				oauthv2.WithStats(stats.NOP),
				oauthv2.WithLogger(logger.NOP),
				oauthv2.WithCpConnector(mockCpConnector),
			)
			optionalArgs := httpClient.HttpClientOptionalArgs{
				Transport:    mockRoundTrip,
				Augmenter:    augmenter.YandexReqAugmenter,
				OAuthHandler: oauthHandler,
			}
			httpClient := httpClient.NewOAuthHttpClient(&http.Client{}, oauthv2common.RudderFlowDelivery, &cache, backendconfig.DefaultBackendConfig, augmenter.GetAuthErrorCategoryForYandex, &optionalArgs)
			yandexmetrica, _ := yandexmetrica.NewManager(logger.NOP, stats.NOP, destination, backendconfig.DefaultBackendConfig)
			yandexmetrica.Client = httpClient
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     destination,
				Manager:         yandexmetrica,
			}
			res := yandexmetrica.Upload(&asyncDestination)
			Expect(res.SuccessResponse).To(Equal("{\"uploading\":{\"id\":719450782,\"source_quantity\":11,\"line_quantity\":11,\"client_id_type\":\"USER_ID\",\"status\":\"UPLOADED\"}}"))
			Expect(res.SucceededJobIDs).To(Equal([]int64{1, 2, 3, 4}))
		})
		It("Testing a failure scenario when the accessToken is invalid", func() {
			cache := oauthv2.NewCache()
			ctrl := gomock.NewController(GinkgoT())
			mockRoundTrip := mockoauthv2.NewMockRoundTripper(ctrl)
			mockRoundTrip.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
				StatusCode: 403,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"errors":[{"error_type":"invalid_token","message":"Invalid oauth_token"}],"code":403,"message":"Invalid oauth_token"}`))),
			}, nil)

			mockCpConnector := mockoauthv2.NewMockConnector(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"dummyID","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"dummyWorkspaceID","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"dummyUserID","metadata":{"userId":"dummyUserID","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"accessToken":"expiredAccessToken","refreshToken":"dummyRefreshToken"}}`)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"dummyID","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"dummyWorkspaceID","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"dummyUserID","metadata":{"userId":"dummyUserID","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"accessToken":"newAccessToken","refreshToken":"dummyRefreshToken"}}`)
			mockTokenProvider := mockoauthv2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			// Invoke code under test
			oauthHandler := oauthv2.NewOAuthHandler(mockTokenProvider,
				oauthv2.WithCache(oauthv2.NewCache()),
				oauthv2.WithLocker(kitsync.NewPartitionRWLocker()),
				oauthv2.WithStats(stats.NOP),
				oauthv2.WithLogger(logger.NOP),
				oauthv2.WithCpConnector(mockCpConnector),
			)
			optionalArgs := httpClient.HttpClientOptionalArgs{
				Transport:    mockRoundTrip,
				Augmenter:    augmenter.YandexReqAugmenter,
				OAuthHandler: oauthHandler,
			}
			httpClient := httpClient.NewOAuthHttpClient(&http.Client{}, oauthv2common.RudderFlowDelivery, &cache, backendconfig.DefaultBackendConfig, augmenter.GetAuthErrorCategoryForYandex, &optionalArgs)
			yandexmetrica, _ := yandexmetrica.NewManager(logger.NOP, stats.NOP, destination, backendconfig.DefaultBackendConfig)
			yandexmetrica.Client = httpClient
			asyncDestination := common.AsyncDestinationStruct{
				ImportingJobIDs: []int64{1, 2, 3, 4},
				FailedJobIDs:    []int64{},
				FileName:        filepath.Join(currentDir, "testdata/uploadData.txt"),
				Destination:     destination,
				Manager:         yandexmetrica,
			}
			res := yandexmetrica.Upload(&asyncDestination)
			Expect(res.AbortReason).To(Equal("got non 200 response from the destination {\"errors\":[{\"error_type\":\"invalid_token\",\"message\":\"Invalid oauth_token\"}],\"code\":403,\"message\":\"Invalid oauth_token\"}"))
			Expect(res.AbortJobIDs).To(Equal([]int64{1, 2, 3, 4}))
		})
	})
})

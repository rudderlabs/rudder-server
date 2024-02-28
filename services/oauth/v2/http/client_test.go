package v2_test

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	rudderSync "github.com/rudderlabs/rudder-go-kit/sync"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_oauthV2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	oauth "github.com/rudderlabs/rudder-server/services/oauth/v2"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	extensions "github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
	httpClient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
)

var _ = Describe("Http/Client", func() {
	Describe("OAuthHttpClient", func() {
		It("should return an http client", func() {
			cache := oauth.NewCache()
			optionalArgs := httpClient.HttpClientOptionalArgs{
				Augmenter: extensions.RouterBodyAugmenter,
			}
			httpClient := httpClient.OAuthHttpClient(&http.Client{}, oauth.RudderFlow_Delivery, &cache, backendconfig.DefaultBackendConfig, oauth.GetAuthErrorCategoryFromTransformResponse, &optionalArgs)
			Expect(httpClient).ToNot(BeNil())
		})
	})
	Describe("OAuthHttpClient uses", func() {
		It("Use OAuthHttpClient to transform event for a non oauth destination", func() {
			// mockRoundTrip := mockRoundTrip{}
			cache := oauth.NewCache()
			ctrl := gomock.NewController(GinkgoT())
			mockRoundTrip := mock_oauthV2.NewMockRoundTripper(ctrl)
			mockRoundTrip.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"version":"1","type":"REST","method":"POST","endpoint":"https://api.clevertap.com/1/upload","headers":{"X-CleverTap-Account-Id":"476550467","X-CleverTap-Passcode":"sample_passcode","Content-Type":"application/json"},"params":{},"body":{"JSON":{"d":[{"type":"profile","profileData":{"Email":"jamesDoe@gmail.com","Name":"James Doe","Phone":"92374162212","Gender":"M","address":"{\"city\":\"kolkata\",\"country\":\"India\",\"postalCode\":789223,\"state\":\"WB\",\"street\":\"\"}"},"identity":"anon_id"}]},"JSON_ARRAY":{},"XML":{},"FORM":{}},"files":{},"userId":""}`))),
			}, nil)
			optionalArgs := httpClient.HttpClientOptionalArgs{
				Transport: mockRoundTrip,
				Augmenter: extensions.RouterBodyAugmenter,
			}
			httpClient := httpClient.OAuthHttpClient(&http.Client{}, oauth.RudderFlow_Delivery, &cache, backendconfig.DefaultBackendConfig, oauth.GetAuthErrorCategoryFromTransformResponse, &optionalArgs)
			req, _ := http.NewRequest("POST", "url", bytes.NewBuffer([]byte(`{"input":[{"message":{"anonymousId":"anon_id","type":"identify","traits":{"email":"jamesDoe@gmail.com","name":"James Doe","phone":"92374162212","gender":"M","address":{"city":"kolkata","country":"India","postalCode":789223,"state":"WB","street":""}}},"metadata":{"jobId":1},"destination":{"config":{},"name":"CleverTap","destinationDefinition":{"config":{},"category":null}}}],"destType":"clevertap"}`)))
			destination := &oauth.DestinationInfo{
				DestDefName:   "CLEVERTAP",
				DestDefConfig: map[string]interface{}{},
				DestinationId: "25beoSzcLFmimO8FgiVqTNwBG12",
				DestConfig:    map[string]interface{}{},
			}
			req = req.WithContext(context.WithValue(req.Context(), oauth.DestKey, destination))
			res, err := httpClient.Do(req)
			Expect(res.StatusCode).To(Equal(200))
			Expect(err).To(BeNil())
			respData, err := io.ReadAll(res.Body)
			Expect(err).To(BeNil())
			Expect(respData).To(Equal([]byte(`{"version":"1","type":"REST","method":"POST","endpoint":"https://api.clevertap.com/1/upload","headers":{"X-CleverTap-Account-Id":"476550467","X-CleverTap-Passcode":"sample_passcode","Content-Type":"application/json"},"params":{},"body":{"JSON":{"d":[{"type":"profile","profileData":{"Email":"jamesDoe@gmail.com","Name":"James Doe","Phone":"92374162212","Gender":"M","address":"{\"city\":\"kolkata\",\"country\":\"India\",\"postalCode\":789223,\"state\":\"WB\",\"street\":\"\"}"},"identity":"anon_id"}]},"JSON_ARRAY":{},"XML":{},"FORM":{}},"files":{},"userId":""}`)))
		})
		It("Use OAuthHttpClient to transform event for a oauth destination with success in transforming", func() {
			cache := oauth.NewCache()
			ctrl := gomock.NewController(GinkgoT())
			mockRoundTrip := mock_oauthV2.NewMockRoundTripper(ctrl)
			mockRoundTrip.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"output":[{"version":"1","type":"REST","method":"POST","endpoint":"https://googleads.googleapis.com/v15/customers/7693729833/offlineUserDataJobs","headers":{"Authorization":"Bearer dummy-access","Content-Type":"application/json","developer-token":"dummy-dev-token"},"params":{"listId":"list111","customerId":"7693729833","consent":{}},"body":{"JSON":{"enablePartialFailure":true,"operations":[{"create":{"userIdentifiers":[{"hashedEmail":"d3142c8f9c9129484daf28df80cc5c955791efed5e69afabb603bc8cb9ffd419"},{"hashedPhoneNumber":"8846dcb6ab2d73a0e67dbd569fa17cec2d9d391e5b05d1dd42919bc21ae82c45"},{"addressInfo":{"hashedFirstName":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","hashedLastName":"dcf000c2386fb76d22cefc0d118a8511bb75999019cd373df52044bccd1bd251","countryCode":"US","postalCode":"1245"}}]}}]},"JSON_ARRAY":{},"XML":{},"FORM":{}},"files":{},"userId":""}]}`))),
			}, nil)

			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"newaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)

			// Invoke code under test
			oauthHandler := &oauth.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				CpConn:        mockCpConnector,
				TokenProvider: mockTokenProvider,
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
			}

			optionalArgs := httpClient.HttpClientOptionalArgs{
				Transport:    mockRoundTrip,
				Augmenter:    extensions.RouterBodyAugmenter,
				OAuthHandler: oauthHandler,
			}
			httpClient := httpClient.OAuthHttpClient(&http.Client{}, oauth.RudderFlow_Delivery, &cache, backendconfig.DefaultBackendConfig, oauth.GetAuthErrorCategoryFromTransformResponse, &optionalArgs)

			req, _ := http.NewRequest("POST", "url", bytes.NewBuffer([]byte(`{"input":[{"message":{"userId":"user 1","event":"event1","type":"audiencelist","properties":{"listData":{"add":[{"email":"test@abc.com","phone":"@09876543210","firstName":"test","lastName":"rudderlabs","country":"US","postalCode":"1245"}]},"enablePartialFailure":true},"context":{"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z"},"metadata":{"secret":{"access_token":"dummy-access","refresh_token":"dummy-refresh","developer_token":"dummy-dev-token"}},"destination":{"secretConfig":{},"config":{},"name":"GARL","destinationDefinition":{"config":{"auth":{"role":"google_adwords_remarketing_lists_v1","type":"OAuth","provider":"Google","rudderScopes":["delivery"]}},"responseRules":{},"name":"GOOGLE_ADWORDS_REMARKETING_LISTS","displayName":"Google Ads Remarketing Lists (Customer Match)","category":null},"permissions":{"isLocked":false}}}],"destType":"google_adwords_remarketing_lists"}`)))
			destination := &oauth.DestinationInfo{
				DestDefName: "GOOGLE_ADWORDS_REMARKETING_LISTS",
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
				DestinationId: "25beoSzcLFmimO8FgiVqTNwBG12",
				DestConfig: map[string]interface{}{
					"rudderAccountId": "7693729833",
				},
				WorkspaceID: "1234",
			}
			req = req.WithContext(context.WithValue(req.Context(), oauth.DestKey, destination))
			res, err := httpClient.Do(req)
			Expect(res.StatusCode).To(Equal(200))
			Expect(err).To(BeNil())
			respData, err := io.ReadAll(res.Body)
			Expect(err).To(BeNil())
			Expect(respData).To(Equal([]byte(`{"output":[{"version":"1","type":"REST","method":"POST","endpoint":"https://googleads.googleapis.com/v15/customers/7693729833/offlineUserDataJobs","headers":{"Authorization":"Bearer dummy-access","Content-Type":"application/json","developer-token":"dummy-dev-token"},"params":{"listId":"list111","customerId":"7693729833","consent":{}},"body":{"JSON":{"enablePartialFailure":true,"operations":[{"create":{"userIdentifiers":[{"hashedEmail":"d3142c8f9c9129484daf28df80cc5c955791efed5e69afabb603bc8cb9ffd419"},{"hashedPhoneNumber":"8846dcb6ab2d73a0e67dbd569fa17cec2d9d391e5b05d1dd42919bc21ae82c45"},{"addressInfo":{"hashedFirstName":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","hashedLastName":"dcf000c2386fb76d22cefc0d118a8511bb75999019cd373df52044bccd1bd251","countryCode":"US","postalCode":"1245"}}]}}]},"JSON_ARRAY":{},"XML":{},"FORM":{}},"files":{},"userId":""}],"interceptorResponse":{"statusCode":0}}`)))
		})
		It("Use OAuthHttpClient to transform event for a oauth destination with returned oauthStatus as REFRESH_TOKEN", func() {
			cache := oauth.NewCache()
			ctrl := gomock.NewController(GinkgoT())
			mockRoundTrip := mock_oauthV2.NewMockRoundTripper(ctrl)
			mockRoundTrip.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"output":[{"authErrorCategory":"REFRESH_TOKEN"}]}`))),
			}, nil)

			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"storedaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"newaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`)
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			mockTokenProvider.EXPECT().Identity().Return(nil)

			// Invoke code under test
			oauthHandler := &oauth.OAuthHandler{
				CacheMutex:    rudderSync.NewPartitionRWLocker(),
				Cache:         v2.NewCache(),
				CpConn:        mockCpConnector,
				TokenProvider: mockTokenProvider,
				Logger:        logger.NewLogger().Child("MockOAuthHandler"),
			}
			optionalArgs := httpClient.HttpClientOptionalArgs{
				Transport:    mockRoundTrip,
				Augmenter:    extensions.RouterBodyAugmenter,
				OAuthHandler: oauthHandler,
			}
			httpClient := httpClient.OAuthHttpClient(&http.Client{}, oauth.RudderFlow_Delivery, &cache, backendconfig.DefaultBackendConfig, oauth.GetAuthErrorCategoryFromTransformResponse, &optionalArgs)

			req, _ := http.NewRequest("POST", "url", bytes.NewBuffer([]byte(`{"input":[{"message":{"userId":"user 1","event":"event1","type":"audiencelist","properties":{"listData":{"add":[{"email":"test@abc.com","phone":"@09876543210","firstName":"test","lastName":"rudderlabs","country":"US","postalCode":"1245"}]},"enablePartialFailure":true},"context":{"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z"},"metadata":{"secret":{"access_token":"dummy-access","refresh_token":"dummy-refresh","developer_token":"dummy-dev-token"}},"destination":{"secretConfig":{},"config":{},"name":"GARL","destinationDefinition":{"config":{"auth":{"role":"google_adwords_remarketing_lists_v1","type":"OAuth","provider":"Google","rudderScopes":["delivery"]}},"responseRules":{},"name":"GOOGLE_ADWORDS_REMARKETING_LISTS","displayName":"Google Ads Remarketing Lists (Customer Match)","category":null},"permissions":{"isLocked":false}}}],"destType":"google_adwords_remarketing_lists"}`)))
			destination := &oauth.DestinationInfo{
				DestDefName: "GOOGLE_ADWORDS_REMARKETING_LISTS",
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
				DestinationId: "25beoSzcLFmimO8FgiVqTNwBG12",
				DestConfig: map[string]interface{}{
					"rudderAccountId": "7693729833",
				},
			}
			req = req.WithContext(context.WithValue(req.Context(), oauth.DestKey, destination))
			res, err := httpClient.Do(req)
			Expect(res.StatusCode).To(Equal(http.StatusInternalServerError))
			Expect(err).To(BeNil())
			respData, err := io.ReadAll(res.Body)
			Expect(err).To(BeNil())
			Expect(respData).To(Equal([]byte(`{"output":[{"authErrorCategory":"REFRESH_TOKEN"}],"interceptorResponse":{"statusCode":500}}`)))
		})

		It("Use OAuthHttpClient to transform event for a oauth destination with returned oauthStatus as AUTH_STATUS_INACTIVE", func() {
			cache := oauth.NewCache()
			ctrl := gomock.NewController(GinkgoT())
			mockRoundTrip := mock_oauthV2.NewMockRoundTripper(ctrl)
			mockRoundTrip.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte(`{"output":[{"authErrorCategory":"AUTH_STATUS_INACTIVE"}]}`))),
			}, nil)

			mockCpConnector := mock_oauthV2.NewMockControlPlaneConnectorI(ctrl)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, `{"options":{},"id":"2BFzzzID8kITtU7AxxWtrn9KQQf","createdAt":"2022-06-29T15:34:47.758Z","updatedAt":"2024-02-12T12:18:35.213Z","workspaceId":"1oVajb9QqG50undaAcokNlYyJQa","name":"dummy user","role":"google_adwords_enhanced_conversions_v1","userId":"1oVadeaoGXN2pataEEoeIaXS3bO","metadata":{"userId":"115538421777182389816","displayName":"dummy user","email":"dummy@testmail.com"},"secretVersion":50,"rudderCategory":"destination","secret":{"access_token":"storedaccesstoken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}}`)
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusOK, "")
			mockTokenProvider := mock_oauthV2.NewMockTokenProvider(ctrl)
			mockTokenProvider.EXPECT().Identity().Return(nil)
			mockTokenProvider.EXPECT().Identity().Return(nil)

			// Invoke code under test
			oauthHandler := &oauth.OAuthHandler{
				CacheMutex:                rudderSync.NewPartitionRWLocker(),
				Cache:                     v2.NewCache(),
				CpConn:                    mockCpConnector,
				TokenProvider:             mockTokenProvider,
				Logger:                    logger.NewLogger().Child("MockOAuthHandler"),
				AuthStatusUpdateActiveMap: map[string]bool{},
			}
			optionalArgs := httpClient.HttpClientOptionalArgs{
				Transport:    mockRoundTrip,
				Augmenter:    extensions.RouterBodyAugmenter,
				OAuthHandler: oauthHandler,
			}
			httpClient := httpClient.OAuthHttpClient(&http.Client{}, oauth.RudderFlow_Delivery, &cache, backendconfig.DefaultBackendConfig, oauth.GetAuthErrorCategoryFromTransformResponse, &optionalArgs)

			req, _ := http.NewRequest("POST", "url", bytes.NewBuffer([]byte(`{"input":[{"message":{"userId":"user 1","event":"event1","type":"audiencelist","properties":{"listData":{"add":[{"email":"test@abc.com","phone":"@09876543210","firstName":"test","lastName":"rudderlabs","country":"US","postalCode":"1245"}]},"enablePartialFailure":true},"context":{"ip":"14.5.67.21","library":{"name":"http"}},"timestamp":"2020-02-02T00:23:09.544Z"},"metadata":{"secret":{"access_token":"dummy-access","refresh_token":"dummy-refresh","developer_token":"dummy-dev-token"}},"destination":{"secretConfig":{},"config":{},"name":"GARL","destinationDefinition":{"config":{"auth":{"role":"google_adwords_remarketing_lists_v1","type":"OAuth","provider":"Google","rudderScopes":["delivery"]}},"responseRules":{},"name":"GOOGLE_ADWORDS_REMARKETING_LISTS","displayName":"Google Ads Remarketing Lists (Customer Match)","category":null},"permissions":{"isLocked":false}}}],"destType":"google_adwords_remarketing_lists"}`)))
			destination := &oauth.DestinationInfo{
				DestDefName: "GOOGLE_ADWORDS_REMARKETING_LISTS",
				DestDefConfig: map[string]interface{}{
					"auth": map[string]interface{}{
						"type": "OAuth",
					},
				},
				DestinationId: "25beoSzcLFmimO8FgiVqTNwBG12",
				DestConfig: map[string]interface{}{
					"rudderAccountId": "7693729833",
				},
			}
			req = req.WithContext(context.WithValue(req.Context(), oauth.DestKey, destination))
			res, err := httpClient.Do(req)
			Expect(res.StatusCode).To(Equal(http.StatusOK))
			Expect(err).To(BeNil())
			respData, err := io.ReadAll(res.Body)
			Expect(err).To(BeNil())
			Expect(respData).To(Equal([]byte(`{"output":[{"authErrorCategory":"AUTH_STATUS_INACTIVE"}],"interceptorResponse":{"statusCode":400}}`)))
		})
	})
})

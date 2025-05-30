package router

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/netutil"
	mocksSysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/processor/integrations"
)

func TestSendPostWithGzipData(t *testing.T) {
	t.Run("should send Gzip data when payload is valid", func(r *testing.T) {
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Content-Encoding") != "gzip" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			body, err := gzip.NewReader(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			defer body.Close()
			buf := new(bytes.Buffer)
			_, _ = buf.ReadFrom(body)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(buf.Bytes())
		}))
		defer testServer.Close()

		network := &netHandle{
			logger:               logger.NewLogger().Child("network"),
			httpClient:           http.DefaultClient,
			blockPrivateIPsCIDRs: netutil.DefaultPrivateCidrRanges,
		}
		eventData := `[{"event":"Signed Up"}]`
		var structData integrations.PostParametersT
		structData.RequestMethod = "POST"
		structData.Type = "REST"
		structData.URL = testServer.URL
		structData.UserID = "anon_id"
		structData.Body = map[string]interface{}{
			"GZIP": map[string]interface{}{
				"payload": eventData,
			},
		}

		resp := network.SendPost(context.Background(), structData)
		require.Equal(r, resp.StatusCode, http.StatusOK)
		require.Equal(r, string(resp.ResponseBody), eventData)
	})

	t.Run("should fail to send Gzip data when payload is missing", func(r *testing.T) {
		network := &netHandle{
			logger:                logger.NewLogger().Child("network"),
			httpClient:            http.DefaultClient,
			blockPrivateIPsDryRun: false,
			blockPrivateIPs:       false,
			blockPrivateIPsCIDRs:  netutil.DefaultPrivateCidrRanges,
		}
		eventData := `[{"event":"Signed Up"}]`
		var structData integrations.PostParametersT
		structData.RequestMethod = "POST"
		structData.Type = "REST"
		structData.UserID = "anon_id"
		structData.Body = map[string]interface{}{
			"GZIP": map[string]interface{}{
				"abc": eventData,
			},
		}

		resp := network.SendPost(context.Background(), structData)
		require.Equal(r, resp.StatusCode, http.StatusBadRequest)
		require.Equal(r, resp.ResponseBody, []byte("400 Unable to parse json list. Unexpected transformer response"))
	})

	t.Run("should send error with invalid body", func(r *testing.T) {
		network := &netHandle{
			blockPrivateIPsCIDRs: netutil.DefaultPrivateCidrRanges,
		}
		network.logger = logger.NewLogger().Child("network")
		network.httpClient = http.DefaultClient
		var structData integrations.PostParametersT
		structData.RequestMethod = "POST"
		structData.Type = "REST"
		structData.UserID = "anon_id"
		structData.Body = map[string]interface{}{
			"key": "value",
		}

		resp := network.SendPost(context.Background(), structData)
		require.Equal(r, resp.StatusCode, http.StatusInternalServerError)
		require.Equal(r, resp.ResponseBody, []byte("500 Invalid Router Payload: body value must be a map"))
	})

	t.Run("should error with invalid body format", func(r *testing.T) {
		network := &netHandle{
			blockPrivateIPsCIDRs: netutil.DefaultPrivateCidrRanges,
		}
		network.logger = logger.NewLogger().Child("network")
		network.httpClient = http.DefaultClient
		var structData integrations.PostParametersT
		structData.RequestMethod = "POST"
		structData.Type = "REST"
		structData.UserID = "anon_id"
		structData.Body = map[string]interface{}{
			"INVALID": map[string]interface{}{
				"key": "value",
			},
		}
		resp := network.SendPost(context.Background(), structData)
		require.Equal(r, resp.StatusCode, http.StatusInternalServerError)
		require.Equal(r, resp.ResponseBody, []byte("500 Invalid Router Payload: body format must be a map found format INVALID"))
	})

	t.Run("should not error with valid body", func(r *testing.T) {
		network := &netHandle{
			blockPrivateIPsCIDRs: netutil.DefaultPrivateCidrRanges,
		}
		network.logger = logger.NewLogger().Child("network")
		httpClient := mocksSysUtils.NewMockHTTPClientI(gomock.NewController(t))
		network.httpClient = httpClient
		var structData integrations.PostParametersT
		structData.RequestMethod = "POST"
		structData.Type = "REST"
		structData.UserID = "anon_id"
		structData.Body = map[string]interface{}{
			"JSON": map[string]interface{}{
				"key": "value",
			},
		}
		httpClient.EXPECT().Do(gomock.Any()).Times(1).Return(&http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader([]byte(""))),
		}, nil)

		resp := network.SendPost(context.Background(), structData)
		require.Equal(r, resp.StatusCode, http.StatusOK)
		require.Equal(r, resp.ResponseBody, []byte(""))
	})

	t.Run("should fail when gzip compression fails", func(r *testing.T) {
		network := &netHandle{
			logger:               logger.NewLogger().Child("network"),
			httpClient:           http.DefaultClient,
			blockPrivateIPsCIDRs: netutil.DefaultPrivateCidrRanges,
		}
		var structData integrations.PostParametersT
		structData.RequestMethod = "POST"
		structData.Type = "REST"
		structData.URL = "https://example.com"
		structData.Body = map[string]interface{}{
			"GZIP": map[string]interface{}{
				"payload": make(chan int), // Invalid type that can't be compressed
			},
		}

		resp := network.SendPost(context.Background(), structData)
		require.Equal(r, resp.StatusCode, http.StatusBadRequest)
		require.Contains(r, string(resp.ResponseBody), "Unable to parse json list")
	})

	t.Run("should handle empty gzip payload", func(r *testing.T) {
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Content-Encoding") != "gzip" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			body, err := gzip.NewReader(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			defer body.Close()
			buf := new(bytes.Buffer)
			_, _ = buf.ReadFrom(body)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(buf.Bytes())
		}))
		defer testServer.Close()

		network := &netHandle{
			logger:               logger.NewLogger().Child("network"),
			httpClient:           http.DefaultClient,
			blockPrivateIPsCIDRs: netutil.DefaultPrivateCidrRanges,
		}
		var structData integrations.PostParametersT
		structData.RequestMethod = "POST"
		structData.Type = "REST"
		structData.URL = testServer.URL
		structData.Body = map[string]interface{}{
			"GZIP": map[string]interface{}{
				"payload": "",
			},
		}

		resp := network.SendPost(context.Background(), structData)
		require.Equal(r, resp.StatusCode, http.StatusOK)
		require.Equal(r, string(resp.ResponseBody), "")
	})

	t.Run("should handle gzip with custom headers", func(r *testing.T) {
		testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Content-Encoding") != "gzip" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if r.Header.Get("X-Custom-Header") != "test" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			body, err := gzip.NewReader(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			defer body.Close()
			buf := new(bytes.Buffer)
			_, _ = buf.ReadFrom(body)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(buf.Bytes())
		}))
		defer testServer.Close()

		network := &netHandle{
			logger:               logger.NewLogger().Child("network"),
			httpClient:           http.DefaultClient,
			blockPrivateIPsCIDRs: netutil.DefaultPrivateCidrRanges,
		}
		eventData := `[{"event":"Signed Up"}]`
		var structData integrations.PostParametersT
		structData.RequestMethod = "POST"
		structData.Type = "REST"
		structData.URL = testServer.URL
		structData.UserID = "anon_id"
		structData.Headers = map[string]interface{}{
			"X-Custom-Header": "test",
		}
		structData.Body = map[string]interface{}{
			"GZIP": map[string]interface{}{
				"payload": eventData,
			},
		}

		resp := network.SendPost(context.Background(), structData)
		require.Equal(r, resp.StatusCode, http.StatusOK)
		require.Equal(r, string(resp.ResponseBody), eventData)
	})
}

func TestSendPost(t *testing.T) {
	network := &netHandle{
		logger:               logger.NewLogger().Child("network"),
		blockPrivateIPsCIDRs: netutil.DefaultPrivateCidrRanges,
	}

	t.Run("should successfully send the request to google analytics", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		mockHTTPClient := mocksSysUtils.NewMockHTTPClientI(mockCtrl)
		network.httpClient = mockHTTPClient

		var structData integrations.PostParametersT
		structData.Type = "REST"
		structData.RequestMethod = "POST"
		structData.URL = "https://www.google-analytics.com/collect"
		structData.UserID = "anon_id"
		structData.Headers = map[string]interface{}{}
		structData.QueryParams = map[string]interface{}{
			"aiid": "com.rudderlabs.android.sdk",
			"an":   "RudderAndroidClient",
			"av":   "1.0",
			"cid":  "anon_id",
			"ds":   "android-sdk",
			"ea":   "Demo Track",
			"ec":   "Demo Category",
			"el":   "Demo Label",
			"ni":   0,
			"qt":   "5.9190508594e+10",
			"t":    "event",
			"tid":  "UA-185645846-1",
			"uip":  "[::1]",
			"ul":   "en-US",
			"v":    1,
		}
		structData.Body = map[string]interface{}{
			"FORM": map[string]interface{}{},
			"JSON": map[string]interface{}{},
			"XML":  map[string]interface{}{},
		}
		structData.Files = map[string]interface{}{}

		// Response JSON
		jsonResponse := `[{
			"full_name": "mock-repo"
		}]`
		// New reader with that JSON
		r := io.NopCloser(bytes.NewReader([]byte(jsonResponse)))

		mockHTTPClient.EXPECT().Do(gomock.Any()).Times(1).Do(func(req *http.Request) {
			// asserting http request
			require.Equal(t, "POST", req.Method)
			require.Equal(t, "www.google-analytics.com", req.URL.Host)
			require.Equal(t, "aiid=com.rudderlabs.android.sdk&an=RudderAndroidClient&av=1.0&cid=anon_id&ds=android-sdk&ea=Demo+Track&ec=Demo+Category&el=Demo+Label&ni=0&qt=5.9190508594e%2B10&t=event&tid=UA-185645846-1&uip=%5B%3A%3A1%5D&ul=en-US&v=1", req.URL.RawQuery)
			require.Equal(t, "RudderLabs", req.Header.Get("User-Agent"))
		}).Return(&http.Response{
			StatusCode: 200,
			Body:       r,
		}, nil)

		resp := network.SendPost(context.Background(), structData)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, jsonResponse, string(resp.ResponseBody))
	})

	t.Run("should respect ctx cancelation", func(t *testing.T) {
		network.httpClient = &http.Client{}

		structData := integrations.PostParametersT{
			Type:          "REST",
			RequestMethod: "POST",
			URL:           "https://www.google-analytics.com/collect",
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		resp := network.SendPost(ctx, structData)
		require.Equal(t, http.StatusGatewayTimeout, resp.StatusCode)
		require.Contains(t, string(resp.ResponseBody), "504 Unable to make \"POST\" request for URL")
		require.Contains(t, string(resp.ResponseBody), "context canceled")
	})

	t.Run("should handle request construction failure", func(t *testing.T) {
		network.httpClient = &http.Client{}

		structData := integrations.PostParametersT{
			Type:          "REST",
			RequestMethod: "POST",
			URL:           "http://[::1]:namedport", // Invalid URL
		}

		resp := network.SendPost(context.Background(), structData)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		require.Contains(t, string(resp.ResponseBody), "400 Unable to construct")
	})

	t.Run("should handle private IP in block mode", func(t *testing.T) {
		// Save original config value and restore after test
		originalValue := config.GetBool("Router.blockPrivateIPs", false)
		config.Set("Router.blockPrivateIPs", true)
		defer config.Set("Router.blockPrivateIPs", originalValue)

		network := &netHandle{
			logger:               logger.NewLogger().Child("network"),
			blockPrivateIPsCIDRs: netutil.DefaultPrivateCidrRanges,
			destType:             "TEST", // Set a destType for the test
		}

		// Properly setup the network handler with private IP blocking
		err := network.Setup(config.Default, 30*time.Second)
		require.NoError(t, err)

		structData := integrations.PostParametersT{
			Type:          "REST",
			RequestMethod: "POST",
			URL:           "https://10.0.0.1",
		}

		resp := network.SendPost(context.Background(), structData)
		require.Equal(t, http.StatusForbidden, resp.StatusCode)
		require.Contains(t, string(resp.ResponseBody), "access to private IP")
	})

	t.Run("should handle egress disabled", func(t *testing.T) {
		network := &netHandle{
			logger:               logger.NewLogger().Child("network"),
			disableEgress:        true,
			blockPrivateIPsCIDRs: netutil.DefaultPrivateCidrRanges,
		}

		structData := integrations.PostParametersT{
			Type:          "REST",
			RequestMethod: "POST",
			URL:           "https://example.com",
		}

		resp := network.SendPost(context.Background(), structData)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "200: outgoing disabled", string(resp.ResponseBody))
	})

	t.Run("should handle JSON body", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		mockHTTPClient := mocksSysUtils.NewMockHTTPClientI(mockCtrl)
		network := &netHandle{
			logger:               logger.NewLogger().Child("network"),
			blockPrivateIPsCIDRs: netutil.DefaultPrivateCidrRanges,
			httpClient:           mockHTTPClient,
		}

		structData := integrations.PostParametersT{
			Type:          "REST",
			RequestMethod: "POST",
			URL:           "https://example.com",
			Body: map[string]interface{}{
				"JSON": map[string]interface{}{
					"key": "value",
				},
			},
		}

		mockHTTPClient.EXPECT().Do(gomock.Any()).Times(1).Do(func(req *http.Request) {
			require.Equal(t, "application/json", req.Header.Get("Content-Type"))
			require.Equal(t, "RudderLabs", req.Header.Get("User-Agent"))
			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.Equal(t, `{"key":"value"}`, string(body))
		}).Return(&http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader([]byte("OK"))),
		}, nil)

		resp := network.SendPost(context.Background(), structData)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "OK", string(resp.ResponseBody))
	})

	t.Run("should handle FORM body", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		mockHTTPClient := mocksSysUtils.NewMockHTTPClientI(mockCtrl)
		network := &netHandle{
			logger:               logger.NewLogger().Child("network"),
			blockPrivateIPsCIDRs: netutil.DefaultPrivateCidrRanges,
			httpClient:           mockHTTPClient,
		}

		structData := integrations.PostParametersT{
			Type:          "REST",
			RequestMethod: "POST",
			URL:           "https://example.com",
			Body: map[string]interface{}{
				"FORM": map[string]interface{}{
					"key": "value",
				},
			},
		}

		mockHTTPClient.EXPECT().Do(gomock.Any()).Times(1).Do(func(req *http.Request) {
			require.Equal(t, "application/x-www-form-urlencoded", req.Header.Get("Content-Type"))
			require.Equal(t, "RudderLabs", req.Header.Get("User-Agent"))
			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.Equal(t, "key=value", string(body))
		}).Return(&http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader([]byte("OK"))),
		}, nil)

		resp := network.SendPost(context.Background(), structData)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "OK", string(resp.ResponseBody))
	})
}

func TestResponseContentType(t *testing.T) {
	network := &netHandle{
		logger:               logger.NewLogger().Child("network"),
		blockPrivateIPsCIDRs: netutil.DefaultPrivateCidrRanges,
	}

	tests := []struct {
		name        string
		contentType string
		altered     bool
	}{
		{"text/html", "text/html", false},
		{"text/xml", "text/xml", false},
		{"text/plain;charset=UTF-8", "text/plain;charset=UTF-8", false},
		{"application/json", "application/json", false},
		{"application/problem+json; charset=utf-8", "application/problem+json; charset=utf-8", false},
		{"application/vnd.collection+json", "application/vnd.collection+json", false},
		{"application/xml", "application/xml", false},
		{"application/atom+xml; charset=utf-8", "application/atom+xml; charset=utf-8", false},
		{"application/soap+xml", "application/soap+xml", false},
		{"application/jwt", "application/jwt", true},
		{"image/jpeg", "image/jpeg", true},
		{"video/mpeg", "video/mpeg", true},
		{"invalidcontenttype", "invalidcontenttype", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCtrl := gomock.NewController(t)
			defer mockCtrl.Finish()
			mockHTTPClient := mocksSysUtils.NewMockHTTPClientI(mockCtrl)
			network.httpClient = mockHTTPClient

			const mockResponseBody = `[{"full_name": "mock-repo"}]`
			mockResponse := http.Response{
				StatusCode: 200,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader([]byte(mockResponseBody))),
			}
			mockResponse.Header.Set("Content-Type", tt.contentType)

			mockHTTPClient.EXPECT().Do(gomock.Any()).Return(&mockResponse, nil)

			requestParams := integrations.PostParametersT{
				Type: "REST",
				URL:  "https://www.google-analytics.com/collect",
				Body: map[string]interface{}{
					"FORM": map[string]interface{}{},
					"JSON": map[string]interface{}{},
					"XML":  map[string]interface{}{},
					"GZIP": map[string]interface{}{},
				},
			}

			resp := network.SendPost(context.Background(), requestParams)
			if tt.altered {
				require.Equal(t, []byte("redacted due to unsupported content-type"), resp.ResponseBody)
			} else {
				require.Equal(t, []byte(mockResponseBody), resp.ResponseBody)
			}
		})
	}
}

package router

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	mocksSysUtils "github.com/rudderlabs/rudder-server/mocks/utils/sysUtils"
	"github.com/rudderlabs/rudder-server/processor/integrations"
)

func TestParseCIDRToRange(t *testing.T) {
	tests := []struct {
		name    string
		cidr    string
		want    IPRange
		wantErr bool
	}{
		{
			name: "valid IPv4 CIDR",
			cidr: "10.0.0.0/8",
			want: IPRange{
				start: net.ParseIP("10.0.0.0").To4(),
				end:   net.ParseIP("10.255.255.255").To4(),
			},
			wantErr: false,
		},
		{
			name: "valid IPv6 CIDR",
			cidr: "fc00::/7",
			want: IPRange{
				start: net.ParseIP("fc00::"),
				end:   net.ParseIP("fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
			},
			wantErr: false,
		},
		{
			name:    "invalid CIDR",
			cidr:    "invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCIDRToRange(tt.cidr)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Compare IP addresses using String() to ensure proper comparison
			require.Equal(t, tt.want.start.String(), got.start.String(), "start IP mismatch")
			require.Equal(t, tt.want.end.String(), got.end.String(), "end IP mismatch")
		})
	}
}

func TestLoadPrivateIPRanges(t *testing.T) {
	// Create a test config
	cfg := config.New()

	tests := []struct {
		name           string
		configValue    string
		expectedRanges int
	}{
		{
			name:           "default ranges",
			configValue:    "",
			expectedRanges: 7, // Number of ranges in defaultPrivateIPRanges
		},
		{
			name:           "custom ranges",
			configValue:    "10.0.0.0/8,192.168.0.0/16",
			expectedRanges: 2,
		},
		{
			name:           "invalid range ignored",
			configValue:    "10.0.0.0/8,invalid,192.168.0.0/16",
			expectedRanges: 2,
		},
		{
			name:           "empty ranges ignored",
			configValue:    "10.0.0.0/8,,192.168.0.0/16",
			expectedRanges: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set the config value
			if tt.configValue != "" {
				cfg.Set("Router.privateIPRanges", tt.configValue)
			} else {
				cfg.Set("Router.privateIPRanges", defaultPrivateIPRanges)
			}

			ranges := loadPrivateIPRanges(cfg)
			require.Len(t, ranges, tt.expectedRanges)

			// Verify the ranges are valid
			for _, r := range ranges {
				require.NotNil(t, r.start, "start IP should not be nil")
				require.NotNil(t, r.end, "end IP should not be nil")
			}
		})
	}
}

func TestValidateURLAndHandlePrivateIP(t *testing.T) {
	// Create a test config
	cfg := config.New()

	tests := []struct {
		name        string
		url         string
		dryRunMode  bool
		blockMode   bool
		wantBlocked bool
		wantErr     bool
		errMsg      string
	}{
		{
			name:        "public URL in normal mode",
			url:         "https://8.8.8.8",
			dryRunMode:  false,
			blockMode:   false,
			wantBlocked: false,
			wantErr:     false,
		},
		{
			name:        "private URL in normal mode",
			url:         "https://10.0.0.1",
			dryRunMode:  false,
			blockMode:   false,
			wantBlocked: false,
			wantErr:     false,
		},
		{
			name:        "private URL in dry run mode",
			url:         "https://10.0.0.1",
			dryRunMode:  true,
			blockMode:   false,
			wantBlocked: false,
			wantErr:     false,
		},
		{
			name:        "private URL in block mode",
			url:         "https://10.0.0.1",
			dryRunMode:  false,
			blockMode:   true,
			wantBlocked: true,
			wantErr:     true,
			errMsg:      "access to private IPs is blocked",
		},
		{
			name:        "public URL in block mode",
			url:         "https://8.8.8.8",
			dryRunMode:  false,
			blockMode:   true,
			wantBlocked: false,
			wantErr:     false,
		},
		{
			name:        "invalid URL",
			url:         "not-a-valid-url",
			dryRunMode:  false,
			blockMode:   false,
			wantBlocked: false,
			wantErr:     false,
		},
		{
			name:        "invalid URL in block mode",
			url:         "not-a-valid-url",
			dryRunMode:  false,
			blockMode:   true,
			wantBlocked: false,
			wantErr:     false,
		},
		{
			name:        "private URL with hostname",
			url:         "https://internal.example.com",
			dryRunMode:  false,
			blockMode:   true,
			wantBlocked: false,
			wantErr:     false,
		},
		{
			name:        "private URL with IPv6",
			url:         "https://[fc00::1]",
			dryRunMode:  false,
			blockMode:   true,
			wantBlocked: true,
			wantErr:     true,
			errMsg:      "access to private IPs is blocked",
		},
		{
			name:        "private URL with port",
			url:         "https://10.0.0.1:8080",
			dryRunMode:  false,
			blockMode:   true,
			wantBlocked: true,
			wantErr:     true,
			errMsg:      "access to private IPs is blocked",
		},
		{
			name:        "private URL with path",
			url:         "https://10.0.0.1/api/v1",
			dryRunMode:  false,
			blockMode:   true,
			wantBlocked: true,
			wantErr:     true,
			errMsg:      "access to private IPs is blocked",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			network := &netHandle{
				logger:          logger.NewLogger().Child("network"),
				dryRunMode:      tt.dryRunMode,
				blockPrivateIPs: tt.blockMode,
				privateIPRanges: loadPrivateIPRanges(cfg),
			}

			isBlocked, err := network.validateURLAndHandlePrivateIP(tt.url)
			if tt.wantErr {
				require.Error(t, err)
				if tt.errMsg != "" {
					require.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantBlocked, isBlocked)
		})
	}
}

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
			logger:          logger.NewLogger().Child("network"),
			httpClient:      http.DefaultClient,
			privateIPRanges: loadPrivateIPRanges(config.Default),
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
			logger:          logger.NewLogger().Child("network"),
			httpClient:      http.DefaultClient,
			privateIPRanges: loadPrivateIPRanges(config.Default),
			dryRunMode:      false,
			blockPrivateIPs: false,
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
		network := &netHandle{}
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
		network := &netHandle{}
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
		network := &netHandle{}
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
			logger:          logger.NewLogger().Child("network"),
			httpClient:      http.DefaultClient,
			privateIPRanges: loadPrivateIPRanges(config.Default),
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
			logger:          logger.NewLogger().Child("network"),
			httpClient:      http.DefaultClient,
			privateIPRanges: loadPrivateIPRanges(config.Default),
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
			logger:          logger.NewLogger().Child("network"),
			httpClient:      http.DefaultClient,
			privateIPRanges: loadPrivateIPRanges(config.Default),
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
		logger:          logger.NewLogger().Child("network"),
		privateIPRanges: loadPrivateIPRanges(config.Default),
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
		network := &netHandle{
			logger:          logger.NewLogger().Child("network"),
			privateIPRanges: loadPrivateIPRanges(config.Default),
			blockPrivateIPs: true,
			httpClient:      &http.Client{},
		}

		structData := integrations.PostParametersT{
			Type:          "REST",
			RequestMethod: "POST",
			URL:           "https://10.0.0.1",
		}

		resp := network.SendPost(context.Background(), structData)
		require.Equal(t, http.StatusForbidden, resp.StatusCode)
		require.Contains(t, string(resp.ResponseBody), "403: access to private IPs is blocked")
	})

	t.Run("should handle egress disabled", func(t *testing.T) {
		network := &netHandle{
			logger:        logger.NewLogger().Child("network"),
			disableEgress: true,
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
		network.httpClient = mockHTTPClient

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
		network.httpClient = mockHTTPClient

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
		logger:          logger.NewLogger().Child("network"),
		privateIPRanges: loadPrivateIPRanges(config.Default),
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

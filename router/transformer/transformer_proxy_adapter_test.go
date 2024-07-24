package transformer

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/services/transformer"
)

func TestV0Adapter(t *testing.T) {
	v0Adapter := NewTransformerProxyAdapter(transformer.V0, logger.NOP)

	t.Run("should return the right url", func(t *testing.T) {
		testDestType := "testDestType"
		testDestTypeLower := "testdesttype"
		url, err := v0Adapter.getProxyURL(testDestType)
		require.Nil(t, err)
		require.True(t, strings.HasSuffix(url, fmt.Sprintf("/%s/destinations/%s/proxy", transformer.V0, testDestTypeLower)))
	})

	t.Run("should return the right payload", func(t *testing.T) {
		proxyReqParms := &ProxyRequestParams{
			ResponseData: ProxyRequestPayload{
				PostParametersT: integrations.PostParametersT{
					Type: "a",
					URL:  "a.com",
					Body: map[string]interface{}{
						"jobId": 1,
					},
				},
				Metadata: []ProxyRequestMetadata{
					{
						JobID:     1,
						DontBatch: true,
					},
					{
						JobID: 2,
					},
				},
				DestinationConfig: map[string]interface{}{
					"key_1": "val_1",
					"key_2": "val_2",
				},
			},
			DestName: "testDestType",
		}
		expectedPayload := `{"type":"a","endpoint":"a.com","method":"","userId":"","headers":null,"params":null,"body":{"jobId":1},"files":null,"metadata":{"jobId":1,"attemptNum":0,"userId":"","sourceId":"","destinationId":"","workspaceId":"","secret":null,"dontBatch":true},"destinationConfig":{"key_1":"val_1","key_2":"val_2"}}`

		payload, err := v0Adapter.getPayload(proxyReqParms)
		require.Nil(t, err)
		require.Equal(t, expectedPayload, string(payload))
	})

	t.Run("should return the right response", func(t *testing.T) {
		metadata := []ProxyRequestMetadata{
			{
				JobID:     11,
				DontBatch: true,
			},
			{
				JobID:     21,
				DontBatch: false,
			},
		}
		resp := ProxyResponseV0{
			Message:             "test",
			DestinationResponse: `{}`,
			AuthErrorCategory:   "oauth123",
		}
		r, err := json.Marshal(resp)
		require.Nil(t, err)

		response, err := v0Adapter.getResponse(r, 200, metadata)
		require.Nil(t, err)
		require.Equal(t, 2, len(response.routerJobResponseCodes))
		require.Equal(t, 2, len(response.routerJobResponseBodys))
		require.Equal(t, 2, len(response.routerJobDontBatchDirectives))

		require.Equal(t, 200, response.routerJobResponseCodes[11])
		require.Equal(t, 200, response.routerJobResponseCodes[21])

		require.Equal(t, string(r), response.routerJobResponseBodys[11])
		require.Equal(t, string(r), response.routerJobResponseBodys[21])

		require.Equal(t, true, response.routerJobDontBatchDirectives[11])
		require.Equal(t, false, response.routerJobDontBatchDirectives[21])

		require.Equal(t, "oauth123", response.authErrorCategory)
	})

	t.Run("should return the unmarshal error", func(t *testing.T) {
		metadata := []ProxyRequestMetadata{
			{
				JobID:     11,
				DontBatch: true,
			},
			{
				JobID:     21,
				DontBatch: false,
			},
		}

		response, err := v0Adapter.getResponse([]byte(`abc`), 200, metadata)
		require.NotNil(t, err)

		require.Equal(t, 0, len(response.routerJobResponseCodes))
		require.Equal(t, 0, len(response.routerJobResponseBodys))
		require.Equal(t, 2, len(response.routerJobDontBatchDirectives))

		require.NotNil(t, response.routerJobResponseCodes)
		require.NotNil(t, response.routerJobResponseBodys)
		require.NotNil(t, response.routerJobDontBatchDirectives)

		require.Equal(t, true, response.routerJobDontBatchDirectives[11])
		require.Equal(t, false, response.routerJobDontBatchDirectives[21])

		require.Equal(t, "", response.authErrorCategory)
	})
}

func TestV1Adapter(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockLogger := mock_logger.NewMockLogger(ctrl)

	v1Adapter := NewTransformerProxyAdapter(transformer.V1, mockLogger)

	t.Run("should return the right url", func(t *testing.T) {
		testDestType := "testDestType"
		testDestTypeLower := "testdesttype"
		url, err := v1Adapter.getProxyURL(testDestType)
		require.Nil(t, err)
		require.True(t, strings.HasSuffix(url, fmt.Sprintf("/%s/destinations/%s/proxy", transformer.V1, testDestTypeLower)))
	})

	t.Run("should return the right payload", func(t *testing.T) {
		proxyReqParms := &ProxyRequestParams{
			ResponseData: ProxyRequestPayload{
				PostParametersT: integrations.PostParametersT{
					Type: "a",
					URL:  "a.com",
					Body: map[string]interface{}{
						"jobId": 1,
					},
				},
				Metadata: []ProxyRequestMetadata{
					{
						JobID:     1,
						DontBatch: true,
					},
					{
						JobID:     2,
						DontBatch: false,
					},
				},
				DestinationConfig: map[string]interface{}{
					"key_1": "val_1",
					"key_2": "val_2",
				},
			},
			DestName: "testDestType",
		}
		expectedPayload := `{"type":"a","endpoint":"a.com","method":"","userId":"","headers":null,"params":null,"body":{"jobId":1},"files":null,"metadata":[{"jobId":1,"attemptNum":0,"userId":"","sourceId":"","destinationId":"","workspaceId":"","secret":null,"dontBatch":true},{"jobId":2,"attemptNum":0,"userId":"","sourceId":"","destinationId":"","workspaceId":"","secret":null,"dontBatch":false}],"destinationConfig":{"key_1":"val_1","key_2":"val_2"}}`

		payload, err := v1Adapter.getPayload(proxyReqParms)
		require.Nil(t, err)
		require.Equal(t, expectedPayload, string(payload))
	})

	t.Run("should return the right response", func(t *testing.T) {
		metadata := []ProxyRequestMetadata{
			{
				JobID:     11,
				DontBatch: true,
			},
			{
				JobID:     21,
				DontBatch: false,
			},
		}
		resp := ProxyResponseV1{
			Message: "test",
			Response: []TPDestResponse{
				{
					StatusCode: 200,
					Metadata: ProxyRequestMetadata{
						JobID:     11,
						DontBatch: false,
					},
					Error: "",
				},
				{
					StatusCode: 401,
					Metadata: ProxyRequestMetadata{
						JobID:     21,
						DontBatch: true,
					},
					Error: "test_error",
				},
			},
			AuthErrorCategory: "oauth123",
		}
		r, err := json.Marshal(resp)
		require.Nil(t, err)

		response, err := v1Adapter.getResponse(r, 200, metadata)
		require.Nil(t, err)
		require.Equal(t, 2, len(response.routerJobResponseCodes))
		require.Equal(t, 2, len(response.routerJobResponseBodys))
		require.Equal(t, 2, len(response.routerJobDontBatchDirectives))

		require.Equal(t, 200, response.routerJobResponseCodes[11])
		require.Equal(t, 401, response.routerJobResponseCodes[21])

		require.Equal(t, "", response.routerJobResponseBodys[11])
		require.Equal(t, "test_error", response.routerJobResponseBodys[21])

		require.Equal(t, false, response.routerJobDontBatchDirectives[11])
		require.Equal(t, true, response.routerJobDontBatchDirectives[21])

		require.Equal(t, "oauth123", response.authErrorCategory)
	})

	t.Run("should produce warning log when in and out jobIDs mismatch", func(t *testing.T) {
		metadata := []ProxyRequestMetadata{
			{
				JobID:     11,
				DontBatch: true,
			},
			{
				JobID:     21,
				DontBatch: false,
			},
		}
		resp := ProxyResponseV1{
			Message: "test",
			Response: []TPDestResponse{
				{
					StatusCode: 200,
					Metadata: ProxyRequestMetadata{
						JobID:     11,
						DontBatch: false,
					},
					Error: "",
				},
				{
					StatusCode: 401,
					Metadata: ProxyRequestMetadata{
						JobID:     21,
						DontBatch: true,
					},
					Error: "test_error",
				},
				{
					StatusCode: 501,
					Metadata: ProxyRequestMetadata{
						JobID:     31,
						DontBatch: true,
					},
					Error: "test_error2",
				},
			},
			AuthErrorCategory: "oauth123",
		}
		r, err := json.Marshal(resp)
		require.Nil(t, err)

		mockLogger.EXPECT().Warnf(gomock.Any(), gomock.Any()).Times(1)

		response, err := v1Adapter.getResponse(r, 200, metadata)
		require.Nil(t, err)
		require.Equal(t, 3, len(response.routerJobResponseCodes))
		require.Equal(t, 3, len(response.routerJobResponseBodys))
		require.Equal(t, 3, len(response.routerJobDontBatchDirectives))

		require.Equal(t, 200, response.routerJobResponseCodes[11])
		require.Equal(t, 401, response.routerJobResponseCodes[21])
		require.Equal(t, 501, response.routerJobResponseCodes[31])

		require.Equal(t, "", response.routerJobResponseBodys[11])
		require.Equal(t, "test_error", response.routerJobResponseBodys[21])
		require.Equal(t, "test_error2", response.routerJobResponseBodys[31])

		require.Equal(t, false, response.routerJobDontBatchDirectives[11])
		require.Equal(t, true, response.routerJobDontBatchDirectives[21])
		require.Equal(t, true, response.routerJobDontBatchDirectives[31])

		require.Equal(t, "oauth123", response.authErrorCategory)
	})

	t.Run("should return the unmarshal error", func(t *testing.T) {
		metadata := []ProxyRequestMetadata{
			{
				JobID:     11,
				DontBatch: true,
			},
			{
				JobID:     21,
				DontBatch: false,
			},
		}

		response, err := v1Adapter.getResponse([]byte(`abc`), 200, metadata)
		require.NotNil(t, err)

		require.Equal(t, 0, len(response.routerJobResponseCodes))
		require.Equal(t, 0, len(response.routerJobResponseBodys))
		require.Equal(t, 2, len(response.routerJobDontBatchDirectives))

		require.NotNil(t, response.routerJobResponseCodes)
		require.NotNil(t, response.routerJobResponseBodys)
		require.NotNil(t, response.routerJobDontBatchDirectives)

		require.Equal(t, true, response.routerJobDontBatchDirectives[11])
		require.Equal(t, false, response.routerJobDontBatchDirectives[21])

		require.Equal(t, "", response.authErrorCategory)
	})
}

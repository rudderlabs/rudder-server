package webhook

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/services/transformer"
)

func TestV1Adapter(t *testing.T) {
	t.Run("should return the right url", func(t *testing.T) {
		v1Adapter := newSourceTransformAdapter(transformer.V1)
		testSrcType := "testSrcType"
		testSrcTypeLower := "testsrctype"

		url, err := v1Adapter.getTransformerURL(testSrcType)
		require.Nil(t, err)
		require.True(t, strings.HasSuffix(url, fmt.Sprintf("/%s/sources/%s", transformer.V1, testSrcTypeLower)))
	})

	t.Run("should return the right adapter version", func(t *testing.T) {
		v1Adapter := newSourceTransformAdapter(transformer.V1)
		adapterVersion := v1Adapter.getAdapterVersion()
		require.Equal(t, adapterVersion, transformer.V1)
	})

	t.Run("should return the body in v1 format", func(t *testing.T) {
		testSrcId := "testSrcId"
		testBody := []byte(`{"a": "testBody"}`)

		mockSrc := backendconfig.SourceT{
			ID:           testSrcId,
			Destinations: []backendconfig.DestinationT{{ID: "testDestId"}},
		}

		v1Adapter := newSourceTransformAdapter(transformer.V1)

		retBody, err := v1Adapter.getTransformerEvent(&gwtypes.AuthRequestContext{Source: mockSrc}, testBody)
		require.Nil(t, err)

		v1TransformerEvent := V1TransformerEvent{
			EventRequest: testBody,
			Source:       backendconfig.SourceT{ID: mockSrc.ID},
		}
		expectedBody, err := json.Marshal(v1TransformerEvent)
		require.Nil(t, err)
		require.Equal(t, expectedBody, retBody)
	})
}

func TestV2Adapter(t *testing.T) {
	t.Run("should return the right url", func(t *testing.T) {
		v2Adapter := newSourceTransformAdapter(transformer.V2)
		testSrcType := "testSrcType"
		testSrcTypeLower := "testsrctype"

		url, err := v2Adapter.getTransformerURL(testSrcType)
		require.Nil(t, err)
		require.True(t, strings.HasSuffix(url, fmt.Sprintf("/%s/sources/%s", transformer.V2, testSrcTypeLower)))
	})

	t.Run("should return the right adapter version", func(t *testing.T) {
		v1Adapter := newSourceTransformAdapter(transformer.V2)
		adapterVersion := v1Adapter.getAdapterVersion()
		require.Equal(t, adapterVersion, transformer.V2)
	})

	t.Run("should return the body in v2 format", func(t *testing.T) {
		testSrcId := "testSrcId"
		testBody := []byte(`{"a": "testBody"}`)

		mockSrc := backendconfig.SourceT{
			ID:           testSrcId,
			Destinations: []backendconfig.DestinationT{{ID: "testDestId"}},
		}

		v2Adapter := newSourceTransformAdapter(transformer.V2)

		retBody, err := v2Adapter.getTransformerEvent(&gwtypes.AuthRequestContext{Source: mockSrc}, testBody)
		require.Nil(t, err)

		v2TransformerEvent := V2TransformerEvent{
			EventRequest: testBody,
			Source:       backendconfig.SourceT{ID: mockSrc.ID},
		}
		expectedBody, err := json.Marshal(v2TransformerEvent)
		require.Nil(t, err)
		require.Equal(t, expectedBody, retBody)
	})
}

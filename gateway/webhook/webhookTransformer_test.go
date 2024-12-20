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
			Event:  testBody,
			Source: backendconfig.SourceT{ID: mockSrc.ID},
		}
		expectedBody, err := json.Marshal(v1TransformerEvent)
		require.Nil(t, err)
		require.Equal(t, expectedBody, retBody)
	})
}

package webhook

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	gwtypes "github.com/rudderlabs/rudder-server/gateway/internal/types"
	"github.com/rudderlabs/rudder-server/jsonrs"
)

func TestV2Adapter(t *testing.T) {
	t.Run("should return the right url", func(t *testing.T) {
		testSrcType := "testSrcType"
		testSrcTypeLower := "testsrctype"

		url, err := getTransformerURL(testSrcType)
		require.Nil(t, err)
		require.True(t, strings.HasSuffix(url, fmt.Sprintf("/v2/sources/%s", testSrcTypeLower)))
	})

	t.Run("should return the body in v2 format", func(t *testing.T) {
		testSrcId := "testSrcId"
		testBody := []byte(`{"a": "testBody"}`)

		mockSrc := backendconfig.SourceT{
			ID:           testSrcId,
			Destinations: []backendconfig.DestinationT{{ID: "testDestId"}},
		}

		retBody, err := getTransformerEvent(&gwtypes.AuthRequestContext{Source: mockSrc}, testBody)
		require.Nil(t, err)

		transformerEvent := TransformerEvent{
			EventRequest: testBody,
			Source:       backendconfig.SourceT{ID: mockSrc.ID},
		}
		expectedBody, err := jsonrs.Marshal(transformerEvent)
		require.Nil(t, err)
		require.JSONEq(t, string(expectedBody), string(retBody))
	})
}

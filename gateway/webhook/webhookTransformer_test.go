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

// V0 is deprecated

// func TestV0Adapter(t *testing.T) {
// 	v0Adapter := newSourceTransformAdapter(transformer.V0)

// 	t.Run("should return the right url", func(t *testing.T) {
// 		testSrcType := "testSrcType"
// 		testSrcTypeLower := "testsrctype"
// 		url, err := v0Adapter.getTransformerURL(testSrcType)
// 		require.Nil(t, err)
// 		require.True(t, strings.HasSuffix(url, fmt.Sprintf("/%s/sources/%s", transformer.V0, testSrcTypeLower)))
// 	})

// 	t.Run("should return the body as is", func(t *testing.T) {
// 		testBody := []byte("testBody")
// 		retBody, err := v0Adapter.getTransformerEvent(nil, testBody)
// 		require.Equal(t, testBody, retBody)
// 		require.Nil(t, err)
// 	})
// }

func TestAdapter(t *testing.T) {
	t.Run("should return the right url", func(t *testing.T) {
		adapter := newSourceTransformAdapter()
		testSrcType := "testSrcType"
		testSrcTypeLower := "testsrctype"

		url, err := adapter.getTransformerURL(testSrcType)
		require.Nil(t, err)
		require.True(t, strings.HasSuffix(url, fmt.Sprintf("/%s/sources/%s", transformer.V2, testSrcTypeLower)))
	})

	t.Run("should return the body in v2 format", func(t *testing.T) {
		testSrcId := "testSrcId"
		testBody := []byte(`{"a": "testBody"}`)

		mockSrc := backendconfig.SourceT{
			ID:           testSrcId,
			Destinations: []backendconfig.DestinationT{{ID: "testDestId"}},
		}

		adapter := newSourceTransformAdapter()

		retBody, err := adapter.getTransformerEvent(&gwtypes.AuthRequestContext{Source: mockSrc}, testBody)
		require.Nil(t, err)

		v2TransformerRequest := V2TransformerRequest{
			Request: testBody,
			Source:  backendconfig.SourceT{ID: mockSrc.ID},
		}
		expectedBody, err := json.Marshal(v2TransformerRequest)
		require.Nil(t, err)
		require.Equal(t, expectedBody, retBody)
	})
}

package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResponseEqualDatetimeStrings(t *testing.T) {
	base := time.Date(2026, time.March, 27, 13, 7, 55, 0, time.UTC)

	makeResponse := func(value any) Response {
		return Response{
			Events: []TransformerResponse{
				{
					Output: map[string]any{
						"messageId":                 "msg-1",
						"rudderstackTransformedUtc": value,
					},
					StatusCode: 200,
					Metadata: Metadata{
						MessageID:     "msg-1",
						SourceID:      "src-1",
						DestinationID: "dest-1",
						WorkspaceID:   "ws-1",
					},
				},
			},
		}
	}

	t.Run("identical responses are strictly equal", func(t *testing.T) {
		left := makeResponse(base.Format(time.DateTime))
		right := makeResponse(base.Format(time.DateTime))

		result := left.EqualDetailed(&right)
		require.True(t, result.Equal)
		require.Empty(t, result.Diff)
		require.False(t, result.DatetimeForgiven, "identical responses should match strictly")
	})

	t.Run("datetime strings compare equal with forgiveness", func(t *testing.T) {
		left := makeResponse(base.Format(time.DateTime))
		right := makeResponse(base.Add(2 * time.Second).Format(time.DateTime))

		result := left.EqualDetailed(&right)
		require.True(t, result.Equal)
		require.Empty(t, result.Diff)
		require.True(t, result.DatetimeForgiven, "different datetime strings should require forgiveness")

		// Backward-compatible Equal still works
		diff, equal := left.Equal(&right)
		require.True(t, equal)
		require.Empty(t, diff)
	})

	t.Run("nested datetime strings compare equal with forgiveness", func(t *testing.T) {
		left := makeResponse(map[string]any{
			"processedAt": base.Format(time.RFC3339),
		})
		right := makeResponse(map[string]any{
			"processedAt": base.Add(37 * time.Minute).Format(time.RFC3339),
		})

		result := left.EqualDetailed(&right)
		require.True(t, result.Equal)
		require.Empty(t, result.Diff)
		require.True(t, result.DatetimeForgiven)
	})

	t.Run("non datetime strings still differ", func(t *testing.T) {
		left := makeResponse("not-a-datetime")
		right := makeResponse("still-not-a-datetime")

		result := left.EqualDetailed(&right)
		require.False(t, result.Equal)
		require.Contains(t, result.Diff, "rudderstackTransformedUtc")
		require.False(t, result.DatetimeForgiven)
	})

	t.Run("timestamp field missing on one side", func(t *testing.T) {
		left := makeResponse(base.Format(time.DateTime))
		right := Response{
			Events: []TransformerResponse{
				{
					Output: map[string]any{
						"messageId": "msg-1",
					},
					StatusCode: 200,
					Metadata: Metadata{
						MessageID:     "msg-1",
						SourceID:      "src-1",
						DestinationID: "dest-1",
						WorkspaceID:   "ws-1",
					},
				},
			},
		}

		result := left.EqualDetailed(&right)
		require.False(t, result.Equal)
		require.Contains(t, result.Diff, "rudderstackTransformedUtc")
		require.False(t, result.DatetimeForgiven)
	})
}

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

	t.Run("datetime strings compare equal", func(t *testing.T) {
		left := makeResponse(base.Format(time.DateTime))
		right := makeResponse(base.Add(2 * time.Second).Format(time.DateTime))

		diff, equal := left.Equal(&right)
		require.True(t, equal)
		require.Empty(t, diff)
	})

	t.Run("nested datetime strings compare equal", func(t *testing.T) {
		left := makeResponse(map[string]any{
			"processedAt": base.Format(time.RFC3339),
		})
		right := makeResponse(map[string]any{
			"processedAt": base.Add(37 * time.Minute).Format(time.RFC3339),
		})

		diff, equal := left.Equal(&right)
		require.True(t, equal)
		require.Empty(t, diff)
	})

	t.Run("non datetime strings still differ", func(t *testing.T) {
		left := makeResponse("not-a-datetime")
		right := makeResponse("still-not-a-datetime")

		diff, equal := left.Equal(&right)
		require.False(t, equal)
		require.Contains(t, diff, "rudderstackTransformedUtc")
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

		diff, equal := left.Equal(&right)
		require.False(t, equal)
		require.Contains(t, diff, "rudderstackTransformedUtc")
	})
}

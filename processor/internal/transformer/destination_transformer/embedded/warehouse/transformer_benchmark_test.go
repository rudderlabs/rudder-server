package warehouse_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/warehouse"
	"github.com/rudderlabs/rudder-server/processor/types"
)

/*
Benchmark_Transformer
Benchmark_Transformer/Identify_(POSTGRES)_-_10000_events
Benchmark_Transformer/Identify_(POSTGRES)_-_10000_events-12         	      12	  85358024 ns/op
*/
func Benchmark_Transformer(b *testing.B) {
	b.Run("Identify (POSTGRES) - 10000 events", func(t *testing.B) {
		b.StopTimer()
		eventPayload := `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`
		metadata := getMetadata("identify", "POSTGRES")
		destination := getDestination("POSTGRES", map[string]any{
			"allowUsersContextTraits": true,
		})

		var singularEvent types.SingularEventT
		err := jsonrs.Unmarshal([]byte(eventPayload), &singularEvent)
		require.NoError(t, err)

		batchSize := 10000
		events := lo.Times(batchSize, func(int) types.TransformerEvent {
			return types.TransformerEvent{
				Message:     singularEvent,
				Metadata:    metadata,
				Destination: destination,
			}
		})

		warehouseTransformer := warehouse.New(config.New(), logger.NOP, stats.NOP)
		ctx := context.Background()
		b.StartTimer()

		for i := 0; i < t.N; i++ {
			embeddedResponse := warehouseTransformer.Transform(ctx, events)
			require.Len(b, embeddedResponse.Events, 2*batchSize)
			require.Nil(b, embeddedResponse.FailedEvents)
		}
	})
}

/*
Benchmark_Transformer_HighCardinality models the production / multi-tenant case
that the warehouse CPU profile was taken from: column-name cardinality is high
enough that the per-batch transformColumnName cache (rebuilt per Transform call)
has a low hit rate, so the snake_case conversion actually runs per distinct
column instead of being served from cache.

Each event carries a distinct set of camelCase / numbered trait keys, so the
columns rarely repeat within a batch. This is the workload where replacing the
regexp2-backed snake_case implementation with the ASCII fast path matters; the
original Benchmark_Transformer (identical events) is cache-saturated and is
insensitive to the snake_case cost.
*/
func Benchmark_Transformer_HighCardinality(b *testing.B) {
	const (
		batchSize      = 2000
		traitsPerEvent = 40
	)
	metadata := getMetadata("identify", "POSTGRES")
	destination := getDestination("POSTGRES", map[string]any{
		"allowUsersContextTraits": true,
	})

	events := lo.Times(batchSize, func(eventIdx int) types.TransformerEvent {
		traits := make(map[string]any, traitsPerEvent)
		for j := range traitsPerEvent {
			// Distinct, realistic-looking column names per event: a mix of
			// camelCase and numbered identifiers that all exercise the
			// transformColumnName / snake_case path with low cache reuse.
			key := fmt.Sprintf("userTraitFieldName%dForEvent%dValue", j, eventIdx)
			traits[key] = "some value"
		}
		message := types.SingularEventT{
			"type":        "identify",
			"messageId":   fmt.Sprintf("messageId-%d", eventIdx),
			"anonymousId": "anonymousId",
			"userId":      "userId",
			"sentAt":      "2021-09-01T00:00:00.000Z",
			"timestamp":   "2021-09-01T00:00:00.000Z",
			"receivedAt":  "2021-09-01T00:00:00.000Z",
			"channel":     "web",
			"traits":      traits,
			"context": map[string]any{
				"traits": map[string]any{"name": "Richard Hendricks"},
				"ip":     "1.2.3.4",
			},
		}
		return types.TransformerEvent{
			Message:     message,
			Metadata:    metadata,
			Destination: destination,
		}
	})

	warehouseTransformer := warehouse.New(config.New(), logger.NOP, stats.NOP)
	ctx := context.Background()
	b.ReportAllocs()

	for b.Loop() {
		res := warehouseTransformer.Transform(ctx, events)
		require.Len(b, res.Events, 2*batchSize) // identify -> identifies + users rows
		require.Nil(b, res.FailedEvents)
	}
}

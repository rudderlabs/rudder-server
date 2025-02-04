package transformer

import (
	"context"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	ptrans "github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/types"
)

/*
Benchmark_Transformer/Identify_(POSTGRES)_-_1000_events
Benchmark_Transformer/Identify_(POSTGRES)_-_1000_events-12         	       9	 112701398 ns/op

Benchmark_Transformer/Identify_(POSTGRES)_-_1000_events
Benchmark_Transformer/Identify_(POSTGRES)_-_1000_events-12         	      28	  37071064 ns/op
*/
func Benchmark_Transformer(b *testing.B) {
	b.Run("Identify (POSTGRES) - 1000 events", func(t *testing.B) {
		b.StopTimer()
		eventPayload := `{"type":"identify","messageId":"messageId","anonymousId":"anonymousId","userId":"userId","sentAt":"2021-09-01T00:00:00.000Z","timestamp":"2021-09-01T00:00:00.000Z","receivedAt":"2021-09-01T00:00:00.000Z","originalTimestamp":"2021-09-01T00:00:00.000Z","channel":"web","request_ip":"5.6.7.8","traits":{"review_id":"86ac1cd43","product_id":"9578257311"},"userProperties":{"rating":3.0,"review_body":"OK for the price. It works but the material feels flimsy."},"context":{"traits":{"name":"Richard Hendricks","email":"rhedricks@example.com","logins":2},"ip":"1.2.3.4"}}`
		metadata := getMetadata("identify", "POSTGRES")
		destination := getDestination("POSTGRES", map[string]any{
			"allowUsersContextTraits": true,
		})

		var singularEvent types.SingularEventT
		err := json.Unmarshal([]byte(eventPayload), &singularEvent)
		require.NoError(t, err)

		batchSize := 1000
		events := lo.Times(batchSize, func(index int) ptrans.TransformerEvent {
			return ptrans.TransformerEvent{
				Message:     singularEvent,
				Metadata:    metadata,
				Destination: destination,
			}
		})

		warehouseTransformer := New(config.New(), logger.NOP, stats.NOP)
		ctx := context.Background()
		b.StartTimer()

		for i := 0; i < t.N; i++ {
			wResponse := warehouseTransformer.Transform(ctx, events, batchSize)
			require.Len(b, wResponse.Events, 2*batchSize)
			require.Nil(b, wResponse.FailedEvents)
		}
	})
}

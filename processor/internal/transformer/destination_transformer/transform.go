package destination_transformer

import (
	"context"

	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/kafka"
	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/pubsub"
	"github.com/rudderlabs/rudder-server/processor/types"
)

type transformerImpl func(ctx context.Context, clientEvents []types.TransformerEvent) types.Response

var embeddedTransformerImpls = map[string]transformerImpl{
	"GOOGLEPUBSUB": pubsub.Transform,
	"KAFKA":        kafka.Transform,
}

func (c *Client) Transform(ctx context.Context, clientEvents []types.TransformerEvent) types.Response {
	destType := clientEvents[0].Destination.DestinationDefinition.Name
	impl, ok := embeddedTransformerImpls[destType]
	if !ok {
		return c.transform(ctx, clientEvents)
	}
	if !c.conf.GetBoolVar(false, "Processor.Transformer.Embedded."+destType+".Enabled") {
		return c.transform(ctx, clientEvents)
	}
	if c.conf.GetBoolVar(true, "Processor.Transformer.Embedded."+destType+".Verify") {
		legacyTransformerResponse := c.transform(ctx, clientEvents)
		embeddedTransformerResponse := impl(ctx, clientEvents)

		c.CompareAndLog(embeddedTransformerResponse, legacyTransformerResponse)

		return legacyTransformerResponse
	}
	return impl(ctx, clientEvents)
}

package destination_transformer

import (
	"context"
	"fmt"

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
	if len(clientEvents) == 0 {
		return types.Response{}
	}

	// TODO: remove this
	destType := clientEvents[0].Destination.DestinationDefinition.Name
	impl, ok := embeddedTransformerImpls[destType]
	e := c.conf.GetBoolVar(false, "Processor.Transformer.Embedded."+destType+".Enabled")

	fmt.Println("Destination Transformer", destType, e, ok)

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

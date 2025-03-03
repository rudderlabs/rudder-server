package destination_transformer

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/rudderlabs/rudder-server/processor/internal/transformer/destination_transformer/embedded/pubsub"
	"github.com/rudderlabs/rudder-server/processor/types"
)

type transformerImpl func(ctx context.Context, clientEvents []types.TransformerEvent) types.Response

var embeddedTransformerImpls = map[string]transformerImpl{
	"GOOGLEPUBSUB": pubsub.Transform,
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
		embeddedTransformerResponse := impl(ctx, clientEvents)
		legacyTransformerResponse := c.transform(ctx, clientEvents)
		emb, _ := json.Marshal(embeddedTransformerResponse)
		leg, _ := json.Marshal(legacyTransformerResponse)
		// compare the two responses
		if len(embeddedTransformerResponse.Events) != len(legacyTransformerResponse.Events) {
			c.log.Error("Embedded transformer response does not match legacy transformer response")
			c.log.Info("Embedded transformer response: ", string(emb))
			c.log.Info("Legacy transformer response: ", string(leg))
			return legacyTransformerResponse
		}
		for i := 0; i < len(embeddedTransformerResponse.Events); i++ {
			embEvent, _ := json.Marshal(embeddedTransformerResponse.Events[i])
			legEvent, _ := json.Marshal(legacyTransformerResponse.Events[i])
			if !reflect.DeepEqual(embEvent, legEvent) {
				c.log.Error("Embedded transformer response does not match legacy transformer response")
				c.log.Info("Embedded transformer response: ", string(embEvent))
				c.log.Info("Legacy transformer response: ", string(legEvent))
				return legacyTransformerResponse
			}
		}
		if len(embeddedTransformerResponse.FailedEvents) != len(legacyTransformerResponse.FailedEvents) {
			c.log.Error("Embedded transformer response does not match legacy transformer response")
			c.log.Info("Embedded transformer response: ", string(emb))
			c.log.Info("Legacy transformer response: ", string(leg))
			return legacyTransformerResponse
		}
		for i := 0; i < len(embeddedTransformerResponse.FailedEvents); i++ {
			embEvent, _ := json.Marshal(embeddedTransformerResponse.FailedEvents[i])
			legEvent, _ := json.Marshal(legacyTransformerResponse.FailedEvents[i])
			if !reflect.DeepEqual(embEvent, legEvent) {
				c.log.Error("Embedded transformer response does not match legacy transformer response")
				c.log.Info("Embedded transformer response: ", string(embEvent))
				c.log.Info("Legacy transformer response: ", string(legEvent))
				return legacyTransformerResponse
			}
		}
		c.log.Info("Embedded transformer response matches legacy transformer response")
		return embeddedTransformerResponse
	}
	return impl(ctx, clientEvents)
}

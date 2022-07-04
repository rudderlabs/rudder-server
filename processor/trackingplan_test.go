package processor

import (
	"testing"

	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/stretchr/testify/assert"
)

func TestReportViolations(t *testing.T) {
	eventsFromTransformerResponse := func(response *transformer.ResponseT) (events []transformer.TransformerResponseT) {
		events = append(events, response.Events...)
		events = append(events, response.FailedEvents...)
		return
	}

	t.Run("Not propagating validation errors", func(t *testing.T) {
		var (
			trackingPlanId      string
			trackingPlanVersion int
		)

		response := transformer.ResponseT{
			Events: []transformer.TransformerResponseT{
				{
					Metadata: transformer.MetadataT{
						MergedTpConfig: map[string]interface{}{
							"propagateValidationErrors": "false",
						},
					},
					Output: map[string]interface{}{
						"context": map[string]interface{}{},
					},
				},
			},
			FailedEvents: []transformer.TransformerResponseT{
				{
					Metadata: transformer.MetadataT{
						MergedTpConfig: map[string]interface{}{
							"propagateValidationErrors": "false",
						},
					},
					Output: map[string]interface{}{
						"context": map[string]interface{}{},
					},
				},
			},
		}

		enhanceWithViolation(response, trackingPlanId, trackingPlanVersion)
		for _, event := range eventsFromTransformerResponse(&response) {
			eventContext, castOk := event.Output["context"].(map[string]interface{})
			assert.True(t, castOk)
			assert.Nil(t, eventContext["trackingPlanId"])
			assert.Nil(t, eventContext["trackingPlanVersion"])
			assert.Nil(t, eventContext["violationErrors"])
		}
	})

	t.Run("Propagate validation errors", func(t *testing.T) {
		response := transformer.ResponseT{
			Events: []transformer.TransformerResponseT{
				{
					Metadata: transformer.MetadataT{
						MergedTpConfig: map[string]interface{}{
							"propagateValidationErrors": "true",
						},
					},
					Output: map[string]interface{}{
						"context": map[string]interface{}{},
					},
					ValidationErrors: []transformer.ValidationErrorT{
						{
							Type: "Datatype-Mismatch",
							Meta: map[string]string{
								"schemaPath":  "#/properties/properties/properties/price/type",
								"instacePath": "/properties/price",
							},
							Message: "must be number",
						},
					},
				},
			},
			FailedEvents: []transformer.TransformerResponseT{
				{
					Metadata: transformer.MetadataT{
						MergedTpConfig: map[string]interface{}{
							"propagateValidationErrors": "true",
						},
					},
					Output: map[string]interface{}{
						"context": map[string]interface{}{},
					},
					ValidationErrors: []transformer.ValidationErrorT{
						{
							Type: "Datatype-Mismatch",
							Meta: map[string]string{
								"schemaPath":  "#/properties/properties/properties/price/type",
								"instacePath": "/properties/price",
							},
							Message: "must be number",
						},
					},
				},
			},
		}
		trackingPlanId := "tp_2BFrdaslxH9A7B2hSDFKxw8wPN6knOb57"
		trackingPlanVersion := 1

		enhanceWithViolation(response, trackingPlanId, trackingPlanVersion)
		for _, event := range eventsFromTransformerResponse(&response) {
			eventContext, castOk := event.Output["context"].(map[string]interface{})
			assert.True(t, castOk)
			assert.Equal(t, eventContext["trackingPlanId"], trackingPlanId)
			assert.Equal(t, eventContext["trackingPlanVersion"], trackingPlanVersion)
			assert.Equal(t, eventContext["violationErrors"], event.ValidationErrors)
		}
	})
}

package processor

import (
	"testing"

	"github.com/rudderlabs/rudder-server/processor/types"

	"github.com/stretchr/testify/assert"
)

func TestReportViolations(t *testing.T) {
	eventsFromTransformerResponse := func(response *types.Response) (events []types.TransformerResponse) {
		events = append(events, response.Events...)
		events = append(events, response.FailedEvents...)
		return
	}

	t.Run("Not propagating validation errors", func(t *testing.T) {
		var (
			trackingPlanId      string
			trackingPlanVersion int
		)

		response := types.Response{
			Events: []types.TransformerResponse{
				{
					Metadata: types.Metadata{
						MergedTpConfig: map[string]interface{}{
							"propagateValidationErrors": "false",
						},
					},
					Output: map[string]interface{}{
						"context": map[string]interface{}{},
					},
				},
			},
			FailedEvents: []types.TransformerResponse{
				{
					Metadata: types.Metadata{
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

	t.Run("Not propagating validation errors when context is not map", func(t *testing.T) {
		var (
			trackingPlanId      string
			trackingPlanVersion int
		)

		response := types.Response{
			Events: []types.TransformerResponse{
				{
					Metadata: types.Metadata{
						MergedTpConfig: map[string]interface{}{
							"propagateValidationErrors": "false",
						},
					},
					Output: map[string]interface{}{
						"context": "some context",
					},
				},
			},
			FailedEvents: []types.TransformerResponse{
				{
					Metadata: types.Metadata{
						MergedTpConfig: map[string]interface{}{
							"propagateValidationErrors": "true",
						},
					},
					Output: map[string]interface{}{
						"context": 1234,
					},
					ValidationErrors: []types.ValidationError{
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

		enhanceWithViolation(response, trackingPlanId, trackingPlanVersion)
		for _, event := range eventsFromTransformerResponse(&response) {
			_, castOk := event.Output["context"].(map[string]interface{})
			assert.False(t, castOk)
		}
	})

	t.Run("Propagate validation errors", func(t *testing.T) {
		response := types.Response{
			Events: []types.TransformerResponse{
				{
					Metadata: types.Metadata{
						MergedTpConfig: map[string]interface{}{
							"propagateValidationErrors": "true",
						},
					},
					Output: map[string]interface{}{
						"context": map[string]interface{}{},
					},
					ValidationErrors: []types.ValidationError{
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
				{
					Metadata: types.Metadata{
						MergedTpConfig: map[string]interface{}{
							"propagateValidationErrors": "true",
						},
					},
					Output: map[string]interface{}{
						"context": nil,
					},
					ValidationErrors: []types.ValidationError{
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
			FailedEvents: []types.TransformerResponse{
				{
					Metadata: types.Metadata{
						MergedTpConfig: map[string]interface{}{
							"propagateValidationErrors": "true",
						},
					},
					Output: map[string]interface{}{
						"context": map[string]interface{}{},
					},
					ValidationErrors: []types.ValidationError{
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
				{
					Metadata: types.Metadata{
						MergedTpConfig: map[string]interface{}{
							"propagateValidationErrors": "true",
						},
					},
					Output: map[string]interface{}{},
					ValidationErrors: []types.ValidationError{
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

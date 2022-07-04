package processor

import (
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReportViolations(t *testing.T) {
	var (
		validateEvent       *transformer.TransformerResponseT
		trackingPlanId      string
		trackingPlanVersion int
	)

	// Propagate validation errors
	validateEvent = &transformer.TransformerResponseT{
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
	}

	trackingPlanId = "tp_2BFrdaslxH9A7B2hSDFKxw8wPN6knOb57"
	trackingPlanVersion = 1

	reportViolations(validateEvent, trackingPlanId, trackingPlanVersion)
	eventContext, castOk := validateEvent.Output["context"].(map[string]interface{})
	assert.True(t, castOk)
	assert.Equal(t, eventContext["trackingPlanId"], trackingPlanId)
	assert.Equal(t, eventContext["trackingPlanVersion"], trackingPlanVersion)
	assert.Equal(t, eventContext["violationErrors"], validateEvent.ValidationErrors)

	// Don't propagate validation error
	validateEvent = &transformer.TransformerResponseT{
		Metadata: transformer.MetadataT{
			MergedTpConfig: map[string]interface{}{
				"propagateValidationErrors": "false",
			},
		},
		Output: map[string]interface{}{
			"context": map[string]interface{}{},
		},
	}
	reportViolations(validateEvent, trackingPlanId, trackingPlanVersion)
	eventContext, castOk = validateEvent.Output["context"].(map[string]interface{})
	assert.True(t, castOk)
	assert.Nil(t, eventContext["trackingPlanId"])
	assert.Nil(t, eventContext["trackingPlanVersion"])
	assert.Nil(t, eventContext["violationErrors"])
}

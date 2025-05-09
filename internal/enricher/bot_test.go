package enricher

import (
	"testing"

	"github.com/rudderlabs/rudder-server/processor/types"
	"github.com/stretchr/testify/assert"
)

func TestBotEnricher(t *testing.T) {
	tests := []struct {
		name           string
		request        *types.GatewayBatchRequest
		eventParams    *types.EventParams
		expectedEvents []types.SingularEventT
		expectError    bool
	}{
		{
			name: "non-bot event should not be enriched",
			request: &types.GatewayBatchRequest{
				Batch: []types.SingularEventT{
					{
						"event": "test-event",
					},
				},
			},
			eventParams: &types.EventParams{
				IsBot: false,
			},
			expectedEvents: []types.SingularEventT{
				{
					"event": "test-event",
				},
			},
			expectError: false,
		},
		{
			name: "bot event without context should be enriched with new context",
			request: &types.GatewayBatchRequest{
				Batch: []types.SingularEventT{
					{
						"event": "test-event",
					},
				},
			},
			eventParams: &types.EventParams{
				IsBot:               true,
				BotName:             "test-bot",
				BotURL:              "https://test-bot.com",
				BotIsInvalidBrowser: false,
			},
			expectedEvents: []types.SingularEventT{
				{
					"event": "test-event",
					"context": map[string]interface{}{
						"isBot": true,
						"bot": BotDetails{
							Name:             "test-bot",
							URL:              "https://test-bot.com",
							IsInvalidBrowser: false,
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "bot event with invalid browser should be enriched with isInvalidBrowser as true",
			request: &types.GatewayBatchRequest{
				Batch: []types.SingularEventT{
					{
						"event": "test-event",
					},
				},
			},
			eventParams: &types.EventParams{
				IsBot:               true,
				BotIsInvalidBrowser: true,
			},
			expectedEvents: []types.SingularEventT{
				{
					"event": "test-event",
					"context": map[string]interface{}{
						"isBot": true,
						"bot": BotDetails{
							IsInvalidBrowser: true,
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "bot event with existing context should be enriched",
			request: &types.GatewayBatchRequest{
				Batch: []types.SingularEventT{
					{
						"event": "test-event",
						"context": map[string]interface{}{
							"existing": "value",
						},
					},
				},
			},
			eventParams: &types.EventParams{
				IsBot:   true,
				BotName: "test-bot",
				BotURL:  "https://test-bot.com",
			},
			expectedEvents: []types.SingularEventT{
				{
					"event": "test-event",
					"context": map[string]interface{}{
						"existing": "value",
						"isBot":    true,
						"bot": BotDetails{
							Name:             "test-bot",
							URL:              "https://test-bot.com",
							IsInvalidBrowser: false,
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "bot event with existing isBot should be enriched with new bot details",
			request: &types.GatewayBatchRequest{
				Batch: []types.SingularEventT{
					{
						"event": "test-event",
						"context": map[string]interface{}{
							"isBot": false,
							"bot": BotDetails{
								Name:             "old-bot",
								URL:              "https://old-bot.com",
								IsInvalidBrowser: false,
							},
						},
					},
				},
			},
			eventParams: &types.EventParams{
				IsBot:               true,
				BotName:             "new-bot",
				BotURL:              "https://new-bot.com",
				BotIsInvalidBrowser: false,
			},
			expectedEvents: []types.SingularEventT{
				{
					"event": "test-event",
					"context": map[string]interface{}{
						"isBot": true,
						"bot": BotDetails{
							Name:             "new-bot",
							URL:              "https://new-bot.com",
							IsInvalidBrowser: false,
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "bot event with invalid context should return error",
			request: &types.GatewayBatchRequest{
				Batch: []types.SingularEventT{
					{
						"event":   "test-event",
						"context": "invalid-context",
					},
				},
			},
			eventParams: &types.EventParams{
				IsBot: true,
			},
			expectedEvents: []types.SingularEventT{
				{
					"event":   "test-event",
					"context": "invalid-context",
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enricher, err := NewBotEnricher()
			assert.NoError(t, err)

			err = enricher.Enrich(nil, tt.request, tt.eventParams)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expectedEvents, tt.request.Batch)
		})
	}
}

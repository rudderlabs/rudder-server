package enricher

import (
	"errors"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/processor/types"
	utils "github.com/rudderlabs/rudder-server/utils/types"
)

type botDetails struct {
	Name             string `json:"name,omitempty"`
	URL              string `json:"url,omitempty"`
	IsInvalidBrowser bool   `json:"isInvalidBrowser,omitempty"`
}

type botEnricher struct{}

func NewBotEnricher() (PipelineEnricher, error) {
	return &botEnricher{}, nil
}

func (e *botEnricher) Enrich(_ *backendconfig.SourceT, request *types.GatewayBatchRequest, eventParams *types.EventParams) error {
	var enrichErrs []error
	for _, event := range request.Batch {
		// if the event is not a bot, we don't need to enrich it
		if !eventParams.IsBot {
			continue
		}

		if eventParams.BotAction != utils.FlagBotEventAction {
			continue
		}

		// if the context section is missing on the event
		// set it with default as map[string]any
		if _, ok := event["context"]; !ok {
			event["context"] = map[string]any{}
		}

		// if the context is other than map[string]any, add error and continue
		context, ok := event["context"].(map[string]any)
		if !ok {
			enrichErrs = append(enrichErrs, errors.New("event doesn't have a valid context section"))
			continue
		}

		context["isBot"] = true
		context["bot"] = botDetails{
			Name:             eventParams.BotName,
			URL:              eventParams.BotURL,
			IsInvalidBrowser: eventParams.BotIsInvalidBrowser,
		}
	}

	return errors.Join(enrichErrs...)
}

func (e *botEnricher) Close() error {
	return nil
}

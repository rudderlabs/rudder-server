package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/httputil"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
)

func (a *API) GetChannel(ctx context.Context, channelID string) (*model.ChannelResponse, error) {
	getChannelURL := a.clientURL + "/channels/" + channelID
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, getChannelURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating get channel request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, reqErr := a.requestDoer.Do(req)
	if reqErr != nil {
		return nil, fmt.Errorf("sending get channel request: %w", reqErr)
	}
	defer func() { httputil.CloseResponse(resp) }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status code for get channel: %d, body: %s", resp.StatusCode, string(mustReadAll(resp.Body)))
	}

	var res model.ChannelResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decoding get channel response: %w", err)
	}
	return &res, nil
}

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

func (a *API) Status(ctx context.Context, channelID string) (*model.StatusResponse, error) {
	statusReqURL := a.clientURL + "/channels/" + channelID + "/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusReqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating status request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, reqErr := a.requestDoer.Do(req)
	if reqErr != nil {
		return nil, fmt.Errorf("sending status request: %w", reqErr)
	}
	defer func() { httputil.CloseResponse(resp) }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status code for status: %d, body: %s", resp.StatusCode, string(mustReadAll(resp.Body)))
	}

	var res model.StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decoding status response: %w", err)
	}
	return &res, nil
}

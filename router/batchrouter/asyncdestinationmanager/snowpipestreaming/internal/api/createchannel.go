package api

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/httputil"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
)

func (a *API) CreateChannel(ctx context.Context, channelReq *model.CreateChannelRequest) (*model.ChannelResponse, error) {
	reqJSON, err := json.Marshal(channelReq)
	if err != nil {
		return nil, fmt.Errorf("marshalling create channel request: %w", err)
	}

	createChannelURL := a.clientURL + "/channels"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, createChannelURL, bytes.NewBuffer(reqJSON))
	if err != nil {
		return nil, fmt.Errorf("creating create channel request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, reqErr := a.requestDoer.Do(req)
	if reqErr != nil {
		return nil, fmt.Errorf("sending create channel request: %w", reqErr)
	}
	defer func() { httputil.CloseResponse(resp) }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status code for create channel: %d, body: %s", resp.StatusCode, string(mustRead(resp.Body)))
	}

	var res model.ChannelResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decoding create channel response: %w", err)
	}
	return &res, nil
}

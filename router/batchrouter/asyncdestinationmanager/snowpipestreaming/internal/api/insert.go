package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

func (a *API) Insert(ctx context.Context, channelID string, insertRequest *model.InsertRequest) (*model.InsertResponse, error) {
	reqJSON, err := json.Marshal(insertRequest)
	if err != nil {
		return nil, fmt.Errorf("marshalling insert request: %w", err)
	}

	insertReqURL := a.clientURL + "/channels/" + channelID + "/insert"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, insertReqURL, bytes.NewBuffer(reqJSON))
	if err != nil {
		return nil, fmt.Errorf("creating insert request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, reqErr := a.requestDoer.Do(req)
	if reqErr != nil {
		return nil, fmt.Errorf("sending insert request: %w", reqErr)
	}
	defer func() { httputil.CloseResponse(resp) }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status code for insert: %d, body: %s", resp.StatusCode, string(mustReadAll(resp.Body)))
	}

	var res model.InsertResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decoding insert response: %w", err)
	}
	return &res, nil
}

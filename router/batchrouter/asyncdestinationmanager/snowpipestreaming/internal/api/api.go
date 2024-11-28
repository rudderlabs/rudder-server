package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"

	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type (
	API struct {
		clientURL   string
		requestDoer requestDoer
	}

	requestDoer interface {
		Do(*http.Request) (*http.Response, error)
	}
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func New(clientURL string, requestDoer requestDoer) *API {
	return &API{
		clientURL:   clientURL,
		requestDoer: requestDoer,
	}
}

func mustRead(r io.Reader) []byte {
	data, err := io.ReadAll(r)
	if err != nil {
		return []byte(fmt.Sprintf("error reading response: %v", err))
	}
	return data
}

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

func (a *API) DeleteChannel(ctx context.Context, channelID string, sync bool) error {
	deleteChannelURL := a.clientURL + "/channels/" + channelID
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, deleteChannelURL, nil)
	if err != nil {
		return fmt.Errorf("creating delete channel request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	queryParams := req.URL.Query()
	queryParams.Add("sync", strconv.FormatBool(sync))
	req.URL.RawQuery = queryParams.Encode()

	resp, reqErr := a.requestDoer.Do(req)
	if reqErr != nil {
		return fmt.Errorf("sending delete channel request: %w", reqErr)
	}
	defer func() { httputil.CloseResponse(resp) }()

	switch resp.StatusCode {
	case http.StatusNoContent, http.StatusAccepted:
		return nil
	default:
		return fmt.Errorf("invalid status code for delete channel: %d, body: %s", resp.StatusCode, string(mustRead(resp.Body)))
	}
}

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
		return nil, fmt.Errorf("invalid status code for get channel: %d, body: %s", resp.StatusCode, string(mustRead(resp.Body)))
	}

	var res model.ChannelResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decoding get channel response: %w", err)
	}
	return &res, nil
}

func (a *API) Insert(ctx context.Context, channelID string, insertRequest *model.InsertRequest) (*model.InsertResponse, error) {
	reqJSON, err := json.Marshal(insertRequest)
	if err != nil {
		return nil, fmt.Errorf("marshalling insert request: %w", err)
	}

	insertURL := a.clientURL + "/channels/" + channelID + "/insert"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, insertURL, bytes.NewBuffer(reqJSON))
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
		return nil, fmt.Errorf("invalid status code for insert: %d, body: %s", resp.StatusCode, string(mustRead(resp.Body)))
	}

	var res model.InsertResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decoding insert response: %w", err)
	}
	return &res, nil
}

func (a *API) GetStatus(ctx context.Context, channelID string) (*model.StatusResponse, error) {
	statusURL := a.clientURL + "/channels/" + channelID + "/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
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
		return nil, fmt.Errorf("invalid status code for status: %d, body: %s", resp.StatusCode, string(mustRead(resp.Body)))
	}

	var res model.StatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decoding status response: %w", err)
	}
	return &res, nil
}

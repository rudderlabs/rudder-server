package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"

	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type (
	API struct {
		clientURL   string
		requestDoer requestDoer
		config      struct {
			enableCompression config.ValueLoader[bool]
		}
		stats struct {
			insertRequestBodySize stats.Histogram
		}
	}

	requestDoer interface {
		Do(*http.Request) (*http.Response, error)
	}
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func New(conf *config.Config, statsFactory stats.Stats, clientURL string, requestDoer requestDoer) *API {
	a := &API{
		clientURL:   clientURL,
		requestDoer: requestDoer,
	}
	a.config.enableCompression = conf.GetReloadableBoolVar(true, "SnowpipeStreaming.enableCompression")
	a.stats.insertRequestBodySize = statsFactory.NewTaggedStat("snowpipe_streaming_request_body_size", stats.HistogramType, stats.Tags{
		"api": "insert",
	})

	return a
}

func mustRead(r io.Reader) []byte {
	data, err := io.ReadAll(r)
	if err != nil {
		return []byte(fmt.Sprintf("error reading response: %v", err))
	}
	return data
}

// CreateChannel creates a new channel with the given request.
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

// DeleteChannel deletes the channel with the given ID.
// If sync is true, the server waits for the flushing of all records in the channel, then do the soft delete.
// If sync is false, the server do the soft delete immediately and we need to wait for the flushing of all records.
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

// GetChannel retrieves the channel with the given ID.
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

// Insert inserts the given rows into the channel with the given ID.
func (a *API) Insert(ctx context.Context, channelID string, insertRequest *model.InsertRequest) (*model.InsertResponse, error) {
	reqJSON, err := json.Marshal(insertRequest)
	if err != nil {
		return nil, fmt.Errorf("marshalling insert request: %w", err)
	}

	enableCompression := a.config.enableCompression.Load()

	var (
		r           io.Reader
		payloadSize int
	)

	if enableCompression {
		r, payloadSize, err = gzippedReader(reqJSON)
		if err != nil {
			return nil, fmt.Errorf("creating gzip reader: %w", err)
		}
	} else {
		r = bytes.NewBuffer(reqJSON)
		payloadSize = len(reqJSON)
	}

	a.stats.insertRequestBodySize.Observe(float64(payloadSize))

	insertURL := a.clientURL + "/channels/" + channelID + "/insert"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, insertURL, r)
	if err != nil {
		return nil, fmt.Errorf("creating insert request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if enableCompression {
		req.Header.Set("Content-Encoding", "gzip")
	}

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

// GetStatus retrieves the status of the channel with the given ID.
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

func gzippedReader(reqJSON []byte) (io.Reader, int, error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write(reqJSON); err != nil {
		return nil, 0, fmt.Errorf("writing to gzip writer: %w", err)
	}
	if err := gz.Close(); err != nil {
		return nil, 0, fmt.Errorf("closing gzip writer: %w", err)
	}
	return &b, b.Len(), nil
}

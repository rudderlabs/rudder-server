package snowpipestreaming

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/httputil"
)

type accountConfig struct {
	Account              string `json:"account"`
	User                 string `json:"user"`
	Role                 string `json:"role"`
	PrivateKey           string `json:"privateKey"`
	PrivateKeyPassphrase string `json:"privateKeyPassphrase"`
}

type tableConfig struct {
	Database string `json:"database"`
	Schema   string `json:"schema"`
	Table    string `json:"table"`
}

type createChannelRequest struct {
	RudderIdentifier string        `json:"rudderIdentifier"`
	Partition        string        `json:"partition"`
	AccountConfig    accountConfig `json:"account"`
	TableConfig      tableConfig   `json:"table"`
}

type createChannelResponse struct {
	ChannelID   string                    `json:"channelId"`
	ChannelName string                    `json:"channelName"`
	ClientName  string                    `json:"clientName"`
	Valid       bool                      `json:"valid"`
	TableSchema map[string]map[string]any `json:"tableSchema"`
}

func (m *Manager) createChannel(ctx context.Context, channelReq *createChannelRequest) (*createChannelResponse, error) {
	reqJSON, err := json.Marshal(channelReq)
	if err != nil {
		return nil, fmt.Errorf("marshalling create channel request: %w", err)
	}

	channelReqURL := m.config.clientURL + "/channels"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, channelReqURL, bytes.NewBuffer(reqJSON))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, reqErr := m.requestDoer.Do(req)
	if reqErr != nil {
		return nil, fmt.Errorf("sending request: %w", reqErr)
	}
	defer func() { httputil.CloseResponse(resp) }()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("invalid status code: %d, body: %s", resp.StatusCode, string(b))
	}

	var res createChannelResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &res, nil
}

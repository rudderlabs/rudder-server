package snowpipestreaming

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type statusResponse struct {
	Offset string `json:"offset"`
	Valid  bool   `json:"valid"`
}

func (m *Manager) status(ctx context.Context, channelId string) (*statusResponse, error) {
	statusReqURL := m.config.clientURL + "/channels/" + channelId + "/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusReqURL, nil)
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

	var res statusResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &res, nil
}

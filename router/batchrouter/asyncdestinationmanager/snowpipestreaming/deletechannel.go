package snowpipestreaming

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/httputil"
)

func (m *Manager) deleteChannel(ctx context.Context, channelReq *createChannelRequest) error {
	reqJSON, err := json.Marshal(channelReq)
	if err != nil {
		return fmt.Errorf("marshalling create channel request: %w", err)
	}

	channelReqURL := m.config.clientURL + "/channels"
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, channelReqURL, bytes.NewBuffer(reqJSON))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, reqErr := m.requestDoer.Do(req)
	if reqErr != nil {
		return fmt.Errorf("sending request: %w", reqErr)
	}
	defer func() { httputil.CloseResponse(resp) }()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("invalid status code: %d, body: %s", resp.StatusCode, string(b))
	}

	return nil
}

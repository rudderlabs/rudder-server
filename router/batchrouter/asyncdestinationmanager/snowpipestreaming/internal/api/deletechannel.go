package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/rudderlabs/rudder-go-kit/httputil"
)

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

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("invalid status code for delete channel: %d, body: %s", resp.StatusCode, string(mustReadAll(resp.Body)))
	}
	return nil
}

package snowpipestreaming

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type Row map[string]any

type insertRequest struct {
	Rows   []Row  `json:"rows"`
	Offset string `json:"offset"`
}

type insertError struct {
	RowIndex                    int64    `json:"rowIndex"`
	ExtraColNames               []string `json:"extraColNames"`
	MissingNotNullColNames      []string `json:"missingNotNullColNames"`
	NullValueForNotNullColNames []string `json:"nullvalueForNotNullColNames"`
}

type insertResponse struct {
	Success bool          `json:"success"`
	Errors  []insertError `json:"errors"`
}

// extraColumns returns the extra columns present in the insert errors.
func (i *insertResponse) extraColumns() []string {
	extraColNamesSet := make(map[string]struct{})
	for _, err := range i.Errors {
		for _, colName := range err.ExtraColNames {
			if _, exists := extraColNamesSet[colName]; !exists {
				extraColNamesSet[colName] = struct{}{}
			}
		}
	}
	return lo.Keys(extraColNamesSet)
}

func (m *Manager) insert(ctx context.Context, channelId string, insertRequest *insertRequest) (*insertResponse, error) {
	reqJSON, err := json.Marshal(insertRequest)
	if err != nil {
		return nil, fmt.Errorf("marshalling insert request: %w", err)
	}

	insertReqURL := m.config.clientURL + "/channels/" + channelId + "/insert"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, insertReqURL, bytes.NewBuffer(reqJSON))
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

	var res insertResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &res, nil
}

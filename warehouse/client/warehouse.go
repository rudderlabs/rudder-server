package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-server/utils/httputil"
)

const defaultTimeout = 10 * time.Second

var json = jsoniter.ConfigCompatibleWithStandardLibrary

// StagingFile contains the require metadata to process a staging file.
type StagingFile struct {
	WorkspaceID   string
	SourceID      string
	DestinationID string
	Location      string

	Schema map[string]map[string]interface{}

	FirstEventAt          string
	LastEventAt           string
	TotalEvents           int
	TotalBytes            int
	UseRudderStorage      bool
	DestinationRevisionID string
	// cloud sources specific info
	SourceTaskRunID string
	SourceJobID     string
	SourceJobRunID  string
	TimeWindow      time.Time
}

// legacyPayload is used to maintain backwards compatibility with the /v1 endpoint.
type legacyPayload struct {
	WorkspaceID      string
	Schema           map[string]map[string]interface{}
	BatchDestination stagingFileBatchDestination

	Location              string
	FirstEventAt          string
	LastEventAt           string
	TotalEvents           int
	TotalBytes            int
	UseRudderStorage      bool
	DestinationRevisionID string
	// cloud sources specific info
	SourceTaskRunID string
	SourceJobID     string
	SourceJobRunID  string
	TimeWindow      time.Time
}

type stagingFileBatchDestination struct {
	Source      struct{ ID string }
	Destination struct{ ID string }
}

type Warehouse struct {
	baseURL string
	client  *http.Client
}

type WarehouseOpts func(*Warehouse)

func WithTimeout(timeout time.Duration) WarehouseOpts {
	return func(warehouse *Warehouse) {
		warehouse.client.Timeout = timeout
	}
}

func NewWarehouse(baseURL string, opts ...WarehouseOpts) *Warehouse {
	warehouse := &Warehouse{
		baseURL: baseURL,
		client:  &http.Client{Timeout: defaultTimeout},
	}

	for _, opt := range opts {
		opt(warehouse)
	}

	return warehouse
}

func (warehouse *Warehouse) Process(ctx context.Context, stagingFile StagingFile) error {
	legacy := legacyPayload{
		WorkspaceID: stagingFile.WorkspaceID,
		Schema:      stagingFile.Schema,
		BatchDestination: stagingFileBatchDestination{
			Source:      struct{ ID string }{ID: stagingFile.SourceID},
			Destination: struct{ ID string }{ID: stagingFile.DestinationID},
		},
		Location:              stagingFile.Location,
		FirstEventAt:          stagingFile.FirstEventAt,
		LastEventAt:           stagingFile.LastEventAt,
		TotalEvents:           stagingFile.TotalEvents,
		TotalBytes:            stagingFile.TotalBytes,
		UseRudderStorage:      stagingFile.UseRudderStorage,
		DestinationRevisionID: stagingFile.DestinationRevisionID,
		SourceTaskRunID:       stagingFile.SourceTaskRunID,
		SourceJobID:           stagingFile.SourceJobID,
		SourceJobRunID:        stagingFile.SourceJobRunID,
		TimeWindow:            stagingFile.TimeWindow,
	}

	jsonPayload, err := json.Marshal(legacy)
	if err != nil {
		return fmt.Errorf("marshaling staging file: %w", err)
	}

	uri := fmt.Sprintf(`%s/v1/process`, warehouse.baseURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := warehouse.client.Do(req)
	if err != nil {
		return fmt.Errorf("http request to %q: %w", warehouse.baseURL, err)
	}
	defer func() { httputil.CloseResponse(resp) }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code %q on %s: %v", resp.Status, warehouse.baseURL, string(body))
	}

	return nil
}

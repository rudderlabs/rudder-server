package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-server/utils/httputil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const defaultTimeout = 10 * time.Second

type StagingFile struct {
	WorkspaceID           string
	Schema                map[string]map[string]interface{}
	BatchDestination      warehouseutils.DestinationT
	Location              string
	FirstEventAt          string
	LastEventAt           string
	TotalEvents           int
	TotalBytes            int
	UseRudderStorage      bool
	DestinationRevisionID string
	// cloud sources specific info
	SourceBatchID   string
	SourceTaskID    string
	SourceTaskRunID string
	SourceJobID     string
	SourceJobRunID  string
	TimeWindow      time.Time
}

type Warehouse struct {
	baseURL string
	client  http.Client
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
		client:  http.Client{Timeout: defaultTimeout},
	}

	for _, opt := range opts {
		opt(warehouse)
	}

	return warehouse
}

func (warehouse *Warehouse) Process(ctx context.Context, stagingFile StagingFile) error {
	jsonPayload, err := json.Marshal(stagingFile)
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
		return fmt.Errorf("routing staging file URL to warehouse service@%v: %w", warehouse.baseURL, err)
	}
	defer func() { httputil.CloseResponse(resp) }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status code %q on %s: %v", resp.Status, warehouse.baseURL, string(body))
	}

	return nil
}

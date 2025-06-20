package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"

	"github.com/rudderlabs/rudder-server/utils/misc"
	ierrors "github.com/rudderlabs/rudder-server/warehouse/internal/errors"
	lf "github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/go-chi/chi/v5"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
)

type stagingFilesRepo interface {
	Insert(ctx context.Context, stagingFile *model.StagingFileWithSchema) (int64, error)
}

type WarehouseAPI struct {
	Logger      logger.Logger
	Stats       stats.Stats
	Repo        stagingFilesRepo
	Multitenant *multitenant.Manager
}

type destinationSchema struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
}

type stagingFileSchema struct {
	WorkspaceID           string
	Schema                map[string]map[string]interface{}
	BatchDestination      destinationSchema
	Location              string
	FirstEventAt          time.Time
	LastEventAt           time.Time
	TotalEvents           int
	TotalBytes            int
	BytesPerTable         map[string]int64
	UseRudderStorage      bool
	DestinationRevisionID string
	// cloud sources specific info
	SourceTaskRunID string
	SourceJobID     string
	SourceJobRunID  string
	TimeWindow      time.Time
}

func mapStagingFile(payload *stagingFileSchema) (model.StagingFileWithSchema, error) {
	if payload.WorkspaceID == "" {
		return model.StagingFileWithSchema{}, errors.New("workspaceId is required")
	}

	if payload.Location == "" {
		return model.StagingFileWithSchema{}, errors.New("location is required")
	}

	if payload.BatchDestination.Source.ID == "" {
		return model.StagingFileWithSchema{}, errors.New("batchDestination.source.id is required")
	}
	if payload.BatchDestination.Destination.ID == "" {
		return model.StagingFileWithSchema{}, errors.New("batchDestination.destination.id is required")
	}

	if len(payload.Schema) == 0 {
		return model.StagingFileWithSchema{}, errors.New("schema is required")
	}

	var schema []byte
	schema, err := jsonrs.Marshal(payload.Schema)
	if err != nil {
		return model.StagingFileWithSchema{}, fmt.Errorf("invalid field: schema: %w", err)
	}

	return (&model.StagingFile{
		WorkspaceID:           payload.WorkspaceID,
		Location:              payload.Location,
		SourceID:              payload.BatchDestination.Source.ID,
		DestinationID:         payload.BatchDestination.Destination.ID,
		FirstEventAt:          payload.FirstEventAt,
		LastEventAt:           payload.LastEventAt,
		UseRudderStorage:      payload.UseRudderStorage,
		DestinationRevisionID: payload.DestinationRevisionID,
		TotalEvents:           payload.TotalEvents,
		TotalBytes:            payload.TotalBytes,
		BytesPerTable:         payload.BytesPerTable,
		SourceTaskRunID:       payload.SourceTaskRunID,
		SourceJobID:           payload.SourceJobID,
		SourceJobRunID:        payload.SourceJobRunID,
		TimeWindow:            payload.TimeWindow,
	}).WithSchema(schema), nil
}

// Handler returns a http handler for the warehouse API.
//
// Implemented routes:
// - POST /v1/process
func (api *WarehouseAPI) Handler() http.Handler {
	srvMux := chi.NewRouter()
	srvMux.Post("/v1/process", api.processHandler)
	return srvMux
}

func (api *WarehouseAPI) processHandler(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()

	var payload stagingFileSchema
	err := jsonrs.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		api.Logger.Warnw("invalid JSON in request body for processing staging file", lf.Error, err.Error())
		http.Error(w, ierrors.ErrInvalidJSONRequestBody.Error(), http.StatusBadRequest)
		return
	}

	stagingFile, err := mapStagingFile(&payload)
	if err != nil {
		api.Logger.Warnw("invalid payload for processing staging file", lf.Error, err.Error())
		http.Error(w, fmt.Sprintf("invalid payload: %s", err.Error()), http.StatusBadRequest)
		return
	}

	if api.Multitenant.DegradedWorkspace(stagingFile.WorkspaceID) {
		api.Logger.Infow("workspace is degraded for processing staging file", lf.WorkspaceID, stagingFile.WorkspaceID)
		http.Error(w, ierrors.ErrWorkspaceDegraded.Error(), http.StatusServiceUnavailable)
		return
	}

	schemaBytes, err := jsonrs.Marshal(stagingFile.Schema)
	if err != nil {
		api.Logger.Warnw("Unable to marshal staging file schema", lf.Error, err.Error())
		http.Error(w, fmt.Sprintf("Unable to marshal staging file schema: %s", err.Error()), http.StatusBadRequest)
		return
	}
	if _, err := api.Repo.Insert(r.Context(), &stagingFile); err != nil {
		if errors.Is(r.Context().Err(), context.Canceled) {
			http.Error(w, ierrors.ErrRequestCancelled.Error(), http.StatusBadRequest)
			return
		}
		api.Logger.Errorw("inserting staging file", lf.Error, err.Error())
		http.Error(w, "can't insert staging file", http.StatusInternalServerError)
		return
	}

	tags := stats.Tags{
		"module":        "warehouse",
		"workspaceId":   stagingFile.WorkspaceID,
		"sourceId":      payload.BatchDestination.Source.ID,
		"destinationId": payload.BatchDestination.Destination.ID,
		"warehouseID": misc.GetTagName(
			payload.BatchDestination.Destination.ID,
			payload.BatchDestination.Source.Name,
			payload.BatchDestination.Destination.Name,
			misc.TailTruncateStr(payload.BatchDestination.Source.ID, 6),
		),
	}
	api.Stats.NewTaggedStat("warehouse_staged_schema_size", stats.HistogramType, tags).Observe(float64(len(schemaBytes)))
	api.Stats.NewTaggedStat("rows_staged", stats.CountType, tags).Count(stagingFile.TotalEvents)

	w.WriteHeader(http.StatusOK)
}

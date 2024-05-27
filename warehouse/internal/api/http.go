package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	ierrors "github.com/rudderlabs/rudder-server/warehouse/internal/errors"
	lf "github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/go-chi/chi/v5"
	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

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
	schema, err := json.Marshal(payload.Schema)
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
	err := json.NewDecoder(r.Body).Decode(&payload)
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

	if _, err := api.Repo.Insert(r.Context(), &stagingFile); err != nil {
		if errors.Is(r.Context().Err(), context.Canceled) {
			http.Error(w, ierrors.ErrRequestCancelled.Error(), http.StatusBadRequest)
			return
		}
		api.Logger.Errorw("inserting staging file", lf.Error, err.Error())
		http.Error(w, "can't insert staging file", http.StatusInternalServerError)
		return
	}

	api.Stats.NewTaggedStat("rows_staged", stats.CountType, stats.Tags{
		"workspaceId":   stagingFile.WorkspaceID,
		"module":        "warehouse",
		"destType":      payload.BatchDestination.Destination.DestinationDefinition.Name,
		"sourceId":      payload.BatchDestination.Source.ID,
		"destinationId": payload.BatchDestination.Destination.ID,
		"warehouseID": misc.GetTagName(
			payload.BatchDestination.Destination.ID,
			payload.BatchDestination.Source.Name,
			payload.BatchDestination.Destination.Name,
			misc.TailTruncateStr(payload.BatchDestination.Source.ID, 6)),
	}).Count(stagingFile.TotalEvents)

	w.WriteHeader(http.StatusOK)
}

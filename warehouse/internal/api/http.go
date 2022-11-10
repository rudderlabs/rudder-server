package api

import (
	jsoniter "github.com/json-iterator/go"

	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type stagingFilesRepo interface {
	Insert(stagingFile warehouse.StagingFileT) (int64, error)
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

func mapStagingFile(payload stagingFileSchema) (warehouse.StagingFileT, error) {
	if payload.WorkspaceID == "" {
		return warehouse.StagingFileT{}, fmt.Errorf("workspaceId is required")
	}

	if payload.Location == "" {
		return warehouse.StagingFileT{}, fmt.Errorf("location is required")
	}

	if payload.BatchDestination.Source.ID == "" {
		return warehouse.StagingFileT{}, fmt.Errorf("batchDestination.source.id is required")
	}
	if payload.BatchDestination.Destination.ID == "" {
		return warehouse.StagingFileT{}, fmt.Errorf("batchDestination.destination.id is required")
	}

	if len(payload.Schema) == 0 {
		return warehouse.StagingFileT{}, fmt.Errorf("schema is required")
	}

	var schema []byte
	schema, err := json.Marshal(payload.Schema)
	if err != nil {
		return warehouse.StagingFileT{}, fmt.Errorf("invalid field: schema: %w", err)
	}

	return warehouse.StagingFileT{
		WorkspaceID:           payload.WorkspaceID,
		Location:              payload.Location,
		SourceID:              payload.BatchDestination.Source.ID,
		DestinationID:         payload.BatchDestination.Destination.ID,
		Schema:                schema,
		FirstEventAt:          payload.FirstEventAt,
		LastEventAt:           payload.LastEventAt,
		UseRudderStorage:      payload.UseRudderStorage,
		DestinationRevisionID: payload.DestinationRevisionID,
		TotalEvents:           payload.TotalEvents,
		SourceBatchID:         payload.SourceBatchID,
		SourceTaskID:          payload.SourceTaskID,
		SourceTaskRunID:       payload.SourceTaskRunID,
		SourceJobID:           payload.SourceJobID,
		SourceJobRunID:        payload.SourceJobRunID,
		TimeWindow:            payload.TimeWindow,
	}, nil
}

func (api *WarehouseAPI) Handler() http.Handler {
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/v1/process", api.processHandler).Methods("POST")

	return srvMux
}

func (api *WarehouseAPI) processHandler(w http.ResponseWriter, r *http.Request) {
	api.Logger.LogRequest(r)

	defer r.Body.Close()

	var payload stagingFileSchema
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		api.Logger.Errorf("Error parsing body: %v", err)
		http.Error(w, "can't unmarshal body", http.StatusBadRequest)
		return
	}

	stagingFile, err := mapStagingFile(payload)
	if err != nil {
		api.Logger.Warnf("invalid payload: %v", err)
		http.Error(w, fmt.Sprintf("invalid payload: %s", err.Error()), http.StatusBadRequest)
		return
	}

	if api.Multitenant.DegradedWorkspace(stagingFile.WorkspaceID) {
		http.Error(w, "Workspace is degraded", http.StatusServiceUnavailable)
		return
	}

	if _, err := api.Repo.Insert(stagingFile); err != nil {
		api.Logger.Errorf("Error inserting staging file: %v", err)
		http.Error(w, "can't insert staging file", http.StatusInternalServerError)
		return
	}

	api.Stats.NewTaggedStat("rows_staged", stats.CountType, stats.Tags{
		"workspace_id": stagingFile.WorkspaceID,
		"module":       "warehouse",
		"destType":     payload.BatchDestination.Destination.DestinationDefinition.Name,
		"warehouseID": misc.GetTagName(
			payload.BatchDestination.Destination.ID,
			payload.BatchDestination.Source.Name,
			payload.BatchDestination.Destination.Name,
			misc.TailTruncateStr(payload.BatchDestination.Source.ID, 6)),
	}).Count(stagingFile.TotalEvents)

	w.WriteHeader(http.StatusOK)
}

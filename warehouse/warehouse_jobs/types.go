package warehouse_jobs

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	JOB_TYPES = []string{"DELETE_BY_JOB_RUN_ID"}
)

//For processing requests payload in handlers.go
type StartJobReqPayload struct {
	SourceID      string `json:"sourceid"`
	Type          string `json:"type"`
	Channel       string `json:"channel"`
	DestinationID string `json:"destinationid"`
	StartTime     string `json:"starttime"`
	JobRunID      string `json:"jobrunid"`
	TaskRunID     string `json:"taskrunid"`
}

//For sending message payload to pgnotifier
type asyncJobMessagePayload json.RawMessage

type AsyncJobReqT struct {
	WorkspaceID     string
	SourceID        string
	DestinationID   string
	DestinationType string
}

type AsyncJobWhT struct {
	dbHandle       *sql.DB
	log            logger.LoggerI
	enabled        bool
	Cancel         context.CancelFunc
	pgnotifier     *pgnotifier.PgNotifierT
	connectionsMap *map[string]map[string]warehouseutils.WarehouseT
}

type asyncJobT struct {
	whManager       manager.ManagerI
	asyncjobpayload AsyncJobPayloadT
}

//For creating job payload
type AsyncJobPayloadT struct {
	Id            string `json:"id"`
	SourceID      string `json:"sourceid"`
	JobType       string `json:"jobtype"`
	DestinationID string `json:"destinationid"`
	StartTime     string `json:"starttime"`
	JobRunID      string `json:"jobrunid"`
	TaskRunID     string `json:"taskrunid"`
	DestType      string `json:"destination_type"`
	Namespace     string `json:"namespace"`
	TableName     string `json:"tablename"`
}

const (
	asyncJobWaiting    string = "waiting"
	asyncJobProcessing string = "processing"
	asyncJobCompleted  string = "completed"
	asyncJobError      string = "error"
)

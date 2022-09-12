package jobs

import (
	"context"
	"database/sql"

	"github.com/rudderlabs/rudder-server/services/pgnotifier"
	"github.com/rudderlabs/rudder-server/utils/logger"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	JOB_TYPES = []string{"DELETE_BY_JOB_RUN_ID_TASK_RUN_ID"}
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
	AsyncJobType  string `json:"async_job_type"`
}

type AsyncJobWhT struct {
	dbHandle       *sql.DB
	log            logger.LoggerI
	enabled        bool
	Cancel         context.CancelFunc
	pgnotifier     *pgnotifier.PgNotifierT
	connectionsMap *map[string]map[string]warehouseutils.WarehouseT
}

//For creating job payload to wh_async_jobs table
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
	AsyncJobType  string `json:"async_job_type"`
}

const (
	WhJobWaiting   string = "waiting"
	WhJobExecuting string = "executing"
	WhJobSucceeded string = "succeeded"
	WhJobAborted   string = "aborted"
	WhJobFailed    string = "failed"
	WhJobError     string = "error"
)

type WhAddJobResponse struct {
	JobIds []int64 `json:"jobids"`
	Err    error   `json:"error"`
}

type WhStatusResponse struct {
	Status string
	Err    string
}

type WhAsyncJobRunnerI interface {
	startAsyncJobRunner(context.Context)
	getTableNamesBy(jobrunid string, taskrunid string)
	getPendingAsyncJobs(context.Context) ([]AsyncJobPayloadT, error)
	getStatusAsyncJob(*StartJobReqPayload) (string, error)
	updateMultipleAsyncJobs(*[]AsyncJobPayloadT, string, string)
}

const (
	MaxBatchSizeToProcess int = 10
	MaxCleanUpRetries     int = 5
	MaxQueryRetries       int = 3
)

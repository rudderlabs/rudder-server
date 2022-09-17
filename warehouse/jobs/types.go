package jobs

import (
	"context"
	"database/sql"
	"time"

	"github.com/rudderlabs/rudder-server/services/pgnotifier"
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
	dbHandle   *sql.DB
	enabled    bool
	pgnotifier *pgnotifier.PgNotifierT
	context    context.Context
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
	AsyncJobType   string = "async_job"
)

type WhAddJobResponse struct {
	JobIds []int64 `json:"jobids"`
	Err    error   `json:"error"`
}

type WhStatusResponse struct {
	Status string
	Err    string
}

type WhAsyncJobRunner interface {
	startAsyncJobRunner(context.Context)
	getTableNamesBy(context.Context, string, string)
	getPendingAsyncJobs(context.Context) ([]AsyncJobPayloadT, error)
	getStatusAsyncJob(*StartJobReqPayload) (string, error)
	updateMultipleAsyncJobs(*[]AsyncJobPayloadT, string, string)
}

type AsyncJobsStatusMap struct {
	Id     string
	Status string
	Error  error
}

const (
	MaxBatchSizeToProcess int           = 10
	MaxCleanUpRetries     int           = 5
	MaxQueryRetries       int           = 3
	RetryTimeInterval     time.Duration = 10 * time.Second
)

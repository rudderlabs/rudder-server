package uploadjob

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
)

type state struct {
	name    string
	handler func(*UploadJob) (string, error)
}

var stateTransitions = map[string]*state{
	model.Waiting: {
		name:    "waiting",
		handler: handleWaiting,
	},
	model.GeneratedUploadSchema: {
		name:    "generated_upload_schema",
		handler: handleGeneratedUploadSchema,
	},
	model.CreatedTableUploads: {
		name:    "created_table_uploads",
		handler: handleCreatedTableUploads,
	},
	model.GeneratingLoadFiles: {
		name:    "generating_load_files",
		handler: handleGeneratingLoadFiles,
	},
	model.GeneratedLoadFiles: {
		name:    "generated_load_files",
		handler: handleGeneratedLoadFiles,
	},
	model.UpdatedTableUploadsCounts: {
		name:    "updated_table_uploads_counts",
		handler: handleUpdatedTableUploadsCounts,
	},
	model.CreatedRemoteSchema: {
		name:    "created_remote_schema",
		handler: handleCreatedRemoteSchema,
	},
	model.ExportedData: {
		name:    "exported_data",
		handler: handleExportedData,
	},
	model.ExportingData: {
		name:    "exporting_data",
		handler: handleExportingData,
	},
	model.ExportingDataFailed: {
		name:    "exporting_data_failed",
		handler: handleExportingDataFailed,
	},
	model.Aborted: {
		name:    "aborted",
		handler: handleAborted,
	},
	model.Failed: {
		name:    "failed",
		handler: handleFailed,
	},
}

func nextState(currentState string) *state {
	return stateTransitions[currentState]
}

func handleWaiting(job *UploadJob) (string, error) {
	job.logger.Infon("Generating upload schema")
	return model.GeneratedUploadSchema, nil
}

func handleGeneratedUploadSchema(job *UploadJob) (string, error) {
	job.logger.Infon("Creating table uploads")
	return model.CreatedTableUploads, nil
}

func handleCreatedTableUploads(job *UploadJob) (string, error) {
	job.logger.Infon("Generating load files")
	return model.GeneratingLoadFiles, nil
}

func handleGeneratingLoadFiles(job *UploadJob) (string, error) {
	job.logger.Infon("Load files generated")
	return model.GeneratedLoadFiles, nil
}

func handleGeneratedLoadFiles(job *UploadJob) (string, error) {
	job.logger.Infon("Updating table uploads counts")
	return model.UpdatedTableUploadsCounts, nil
}

func handleUpdatedTableUploadsCounts(job *UploadJob) (string, error) {
	job.logger.Infon("Creating remote schema")
	return model.CreatedRemoteSchema, nil
}

func handleCreatedRemoteSchema(job *UploadJob) (string, error) {
	job.logger.Infon("Exporting data")
	return model.ExportedData, nil
}

func handleExportedData(job *UploadJob) (string, error) {
	job.logger.Infon("Data exported")
	return "", nil
}

func handleExportingData(job *UploadJob) (string, error) {
	job.logger.Infon("Exporting data")
	return model.ExportedData, nil
}

func handleExportingDataFailed(job *UploadJob) (string, error) {
	job.logger.Infon("Exporting data failed")
	return model.Failed, nil
}

func handleAborted(job *UploadJob) (string, error) {
	job.logger.Infon("Job aborted")
	return "", nil
}

func handleFailed(job *UploadJob) (string, error) {
	job.logger.Infon("Job failed")
	return "", nil
}

func (job *UploadJob) trackLongRunningUpload() chan struct{} {
	ch := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ch:
				return
			case <-ticker.C:
				if job.now().Sub(job.upload.FirstAttemptAt) > job.config.longRunningUploadStatThresholdInMin {
					job.statsFactory.NewTaggedStat(
						"warehouse_upload_long_running",
						stats.CountType,
						map[string]string{
							"workspace_id":     job.upload.WorkspaceID,
							"source_id":        job.upload.SourceID,
							"destination_id":   job.upload.DestinationID,
							"destination_type": job.upload.DestinationType,
							"namespace":        job.upload.Namespace,
						},
					).Count(1)
				}
			}
		}
	}()
	return ch
}

func (job *UploadJob) getUploadFirstAttemptTime() time.Time {
	return job.upload.FirstAttemptAt
}

func (job *UploadJob) getNewTimings(status string) ([]byte, model.Timings, error) {
	timings := job.upload.Timings
	if timings == nil {
		timings = make(model.Timings, 0)
	}

	timings = append(timings, map[string]time.Time{
		status: job.now(),
	})

	marshalledTimings, err := json.Marshal(timings)
	if err != nil {
		return nil, nil, fmt.Errorf("marshalling timings: %w", err)
	}

	return marshalledTimings, timings, nil
}

func (job *UploadJob) setUploadError(statusError error, state string) (string, error) {
	var (
		jobErrorType               = job.errorHandler.MatchUploadJobErrorType(statusError)
		destCredentialsValidations *bool
	)

	defer func() {
		job.logger.Warnw("upload error",
			"upload_status", state,
			"error", statusError,
			"priority", job.upload.Priority,
			"retried", job.upload.Retried,
			"attempt", job.upload.Attempts,
			"load_file_type", job.upload.LoadFileType,
			"error_mapping", jobErrorType,
			"destination_creds_valid", destCredentialsValidations,
		)
	}()

	job.counterStat(fmt.Sprintf("error_%s", state)).Count(1)
	upload := job.upload

	err := job.setUploadStatus(UploadStatusOpts{Status: state})
	if err != nil {
		return "", fmt.Errorf("unable to set upload's job: %d status: %w", job.upload.ID, err)
	}

	uploadErrors, err := extractAndUpdateUploadErrorsByState(job.upload.Error, state, statusError)
	if err != nil {
		return "", fmt.Errorf("unable to handle upload errors in job: %d by state: %s, err: %v",
			job.upload.ID,
			state,
			err)
	}

	// Reset the state as aborted if max retries
	// exceeded.
	uploadErrorAttempts := uploadErrors[state]["attempt"].(int)

	if job.Aborted(uploadErrorAttempts, job.getUploadFirstAttemptTime()) {
		state = model.Aborted
	}

	metadata := repo.ExtractUploadMetadata(job.upload)

	metadata.NextRetryTime = job.now().Add(job.durationBeforeNextAttempt(upload.Attempts + 1))
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		metadataJSON = []byte("{}")
	}

	serializedErr, _ := json.Marshal(&uploadErrors)
	serializedErr, _ = utils.SanitizeJSON(serializedErr)

	txn, err := job.db.BeginTx(job.ctx, &sql.TxOptions{})
	if err != nil {
		return "", fmt.Errorf("starting transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = txn.Rollback()
		}
	}()

	err = job.uploadsRepo.UpdateWithTx(job.ctx, txn, job.upload.ID, []repo.UpdateKeyValue{
		repo.UploadFieldStatus(state),
		repo.UploadFieldError(serializedErr),
		repo.UploadFieldMetadata(metadataJSON),
		repo.UploadFieldUpdatedAt(job.now()),
	})
	if err != nil {
		return "", fmt.Errorf("updating upload: %w", err)
	}

	if err = txn.Commit(); err != nil {
		return "", fmt.Errorf("committing transaction: %w", err)
	}

	return state, nil
}

func (job *UploadJob) Aborted(attempts int, startTime time.Time) bool {
	// Defensive check to prevent garbage startTime
	if startTime.IsZero() {
		return false
	}

	return attempts > job.config.minRetryAttempts && job.now().Sub(startTime) > job.config.retryTimeWindow
}

func (job *UploadJob) durationBeforeNextAttempt(attempt int64) time.Duration {
	var d time.Duration
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = job.config.minUploadBackoff
	b.MaxInterval = job.config.maxUploadBackoff
	b.MaxElapsedTime = 0
	b.Multiplier = 2
	b.RandomizationFactor = 0
	b.Reset()
	for index := int64(0); index < attempt; index++ {
		d = b.NextBackOff()
	}
	return d
}

func (job *UploadJob) validateDestinationCredentials() (bool, error) {
	if job.destinationValidator == nil {
		return false, errors.New("failed to validate as destinationValidator is not set")
	}
	response := job.destinationValidator.Validate(job.ctx, &job.warehouse.Destination)
	return response.Success, nil
}

func (job *UploadJob) counterStat(name string) stats.Stat {
	return job.statsFactory.NewTaggedStat(
		fmt.Sprintf("warehouse_%s", name),
		stats.CountType,
		map[string]string{
			"workspace_id":     job.upload.WorkspaceID,
			"source_id":        job.upload.SourceID,
			"destination_id":   job.upload.DestinationID,
			"destination_type": job.upload.DestinationType,
			"namespace":        job.upload.Namespace,
		},
	)
}

func (job *UploadJob) timerStat(name string) stats.Stat {
	return job.statsFactory.NewTaggedStat(
		fmt.Sprintf("warehouse_%s", name),
		stats.TimerType,
		map[string]string{
			"workspace_id":     job.upload.WorkspaceID,
			"source_id":        job.upload.SourceID,
			"destination_id":   job.upload.DestinationID,
			"destination_type": job.upload.DestinationType,
			"namespace":        job.upload.Namespace,
		},
	)
}

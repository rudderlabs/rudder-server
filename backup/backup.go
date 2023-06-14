package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/samber/lo"
)

var successfulBackupResponse = []byte(`{"status": "backup successful"}`)

type jobQueue interface {
	GetProcessed(
		ctx context.Context,
		params jobsdb.GetQueryParamsT,
	) (jobsdb.JobsResult, error)

	UpdateJobStatus(
		ctx context.Context,
		statusList []*jobsdb.JobStatusT,
		customValFilters []string,
		parameterFilters []jobsdb.ParameterFilterT,
	) error

	Identifier() string
}

type BackupContext struct {
	QueryParams          jobsdb.GetQueryParamsT
	Queue                jobQueue
	FileUploaderProvider fileuploader.Provider
	Marshaller           func(*jobsdb.JobT) ([]byte, error)
}

func Backup(
	ctx context.Context,
	backupContext BackupContext,
	log logger.Logger,
) {
	// 1. Get jobs
	jobs, err := backupContext.Queue.GetProcessed(ctx, backupContext.QueryParams)
	if err != nil {
		log.Infof("backup Error: Error getting jobs from jobsdb: %w", err)
		panic(err)
	}
	workspaceJobsMap := lo.GroupBy(jobs.Jobs, func(job *jobsdb.JobT) string {
		return job.WorkspaceId
	})
	statusList := make([]*jobsdb.JobStatusT, 0)
	for workspaceID, wJobs := range workspaceJobsMap {
		// write to file
		backupPathDirName := "/rudder-s3-dumps/"
		tmpDirPath, err := misc.CreateTMPDIR()
		if err != nil {
			panic(err)
		}
		// pathPrefix := strings.TrimPrefix("gw_jobs", preDropTablePrefix)
		pathPrefix := backupContext.Queue.Identifier() // TODO: remove
		path := fmt.Sprintf(
			"%v%v.%v.%v.%v.%v.%v.gz",
			tmpDirPath+backupPathDirName,
			pathPrefix,
			wJobs[0].JobID,
			wJobs[len(wJobs)-1].JobID,
			wJobs[0].CreatedAt.UnixNano()/int64(time.Millisecond),
			wJobs[len(wJobs)-1].CreatedAt.UnixNano()/int64(time.Millisecond),
			workspaceID,
		)

		if backupContext.Marshaller == nil {
			backupContext.Marshaller = jobsdb.MarshalJob
		}
		err = writeJobsToFile(wJobs, path, backupContext.Marshaller, log)
		if err != nil {
			log.Errorf("backup Error: Error writing jobs to file: %w for workspace %s",
				err,
				workspaceID,
			)
			continue
		}

		// 2. Upload jobs
		{
			pathPrefixes := make([]string, 0) // TOOO
			var output filemanager.UploadOutput
			fileUploader, err := backupContext.FileUploaderProvider.GetFileManager(workspaceID)
			if err != nil {
				log.Errorf("backup Error: Error getting file uploader: %w", err)
				continue
			}

			bo := backoff.NewExponentialBackOff()
			bo.MaxInterval = time.Minute
			bo.MaxElapsedTime = config.GetDuration(
				"backup.maxRetryTime",
				5,
				time.Minute,
			)
			boRetries := backoff.WithMaxRetries(
				bo,
				uint64(config.GetInt64("backup.maxRetries", 3)),
			)
			boCtx := backoff.WithContext(boRetries, ctx)
			file, err := os.Open(path)
			if err != nil {
				panic(err)
			}
			defer func() { _ = file.Close() }()
			backup := func() error {
				output, err = fileUploader.Upload(ctx, file, pathPrefixes...)
				return err
			}
			if err = backoff.Retry(backup, boCtx); err != nil {
				log.Errorf("backup Error: Error uploading file: %w for workspace %s",
					err,
					workspaceID,
				)
				continue
			}
			log.Debugf(
				"[JobsDB] :: Backed up table at %s for workspaceId %s",
				output.Location,
				workspaceID,
			)
		}

		statuses := lo.Map(
			wJobs,
			func(job *jobsdb.JobT, _ int) *jobsdb.JobStatusT {
				js := &jobsdb.JobStatusT{
					JobID:         job.JobID,
					ExecTime:      time.Now(),
					RetryTime:     time.Now(),
					ErrorCode:     "",
					ErrorResponse: successfulBackupResponse, // check
					Parameters:    []byte(`{}`),             // check
					JobParameters: job.Parameters,
				}
				switch job.LastJobStatus.ErrorCode {
				case "200", "0":
					js.JobState = jobsdb.Succeeded.State
				default:
					js.JobState = jobsdb.Aborted.State
				}
				return js
			},
		)
		statusList = append(statusList, statuses...)
	}

	// 3. Update jobs status
	if err = misc.RetryWithNotify(
		ctx,
		config.GetDuration("backup.updateStatusTimeout", 60, time.Second),
		config.GetInt("backup.updateStatusRetries", 3),
		func(ctx context.Context) error {
			return backupContext.Queue.UpdateJobStatus(
				ctx,
				statusList,
				nil,
				nil,
			)
		},
		func(attempt int) {
			log.Infof(
				"backup Error: Error updating job status: %w, retrying %d",
				err,
				attempt,
			)
		},
	); err != nil {
		log.Errorf("backup Error: Error updating job status: %w",
			err,
		)
	}

}

// Writes a list of jobs to a .gz file at given path
func writeJobsToFile(
	jobs []*jobsdb.JobT,
	path string,
	marshaller func(*jobsdb.JobT) ([]byte, error),
	log logger.Logger,
) error {
	gzipFilePath := fmt.Sprintf(`%v.gz`, path)
	err := os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	if err != nil {
		panic(err)
	}
	gzWriter, err := misc.CreateGZ(gzipFilePath)
	if err != nil {
		panic(err)
	}
	defer func() { _ = gzWriter.CloseGZ() }()

	// gzWriter.Write([]byte(jobs[0].Headings()))
	for _, item := range jobs {
		bytes, err := marshaller(item)
		if err != nil {
			log.Errorf("backup Error: Error marshalling job: %w for jobID - %d",
				err,
				item.JobID,
			)
			continue
		}
		_, err = gzWriter.Write(append(bytes, '\n'))
		if err != nil {
			return err
		}
	}

	return nil
}

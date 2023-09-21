package replay

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
)

type SourceWorkerT struct {
	log           logger.Logger
	channel       chan *jobsdb.JobT
	workerID      int
	replayHandler *Handler
	tablePrefix   string
	transformer   transformer.Transformer
	uploader      filemanager.FileManager
}

var userTransformBatchSize misc.ValueLoader[int]

func (worker *SourceWorkerT) workerProcess(ctx context.Context) {
	worker.log.Debugf("worker started %d", worker.workerID)
	for job := range worker.channel {
		worker.log.Debugf("job received: %s", job.EventPayload)

		worker.replayJobsInFile(ctx, gjson.GetBytes(job.EventPayload, "location").String())

		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			JobState:      jobsdb.Succeeded.State,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "",
			ErrorResponse: []byte(`{}`), // check
			Parameters:    []byte(`{}`), // check
			JobParameters: job.Parameters,
		}
		err := worker.replayHandler.db.UpdateJobStatus(ctx, []*jobsdb.JobStatusT{&status}, "replay", nil)
		if err != nil {
			panic(err)
		}
	}
}

func (worker *SourceWorkerT) replayJobsInFile(ctx context.Context, filePath string) {
	filePathTokens := strings.Split(filePath, "/")

	var err error
	dumpDownloadPathDirName := "/rudder-s3-dumps/"
	tmpdirPath := strings.TrimSuffix(config.GetString("RUDDER_TMPDIR", "/tmp"), "/")
	if tmpdirPath == "" {
		tmpdirPath, err = os.UserHomeDir()
		if err != nil {
			panic(err)
		}
	}
	path := fmt.Sprintf(`%v%v%v`, tmpdirPath, dumpDownloadPathDirName, filePathTokens[len(filePathTokens)-1])

	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		panic(err)
	}

	file, err := os.Create(path)
	if err != nil {
		panic(err) // Cannot open file to write
	}

	err = worker.uploader.Download(ctx, file, filePath)
	if err != nil {
		panic(err) // failed to download
	}
	worker.log.Debugf("file downloaded at %s", path)
	defer func() { _ = file.Close() }()

	rawf, err := os.Open(path)
	if err != nil {
		panic(err) // failed to open file
	}

	reader, err := gzip.NewReader(rawf)
	if err != nil {
		panic(err) // failed to read gzip file
	}

	sc := bufio.NewScanner(reader)
	// default scanner buffer maxCapacity is 64K
	// set it to higher value to avoid read stop on read size error
	maxCapacity := 10240 * 1024 // 10MB
	buf := make([]byte, maxCapacity)
	sc.Buffer(buf, maxCapacity)

	defer func() { _ = rawf.Close() }()

	var jobs []*jobsdb.JobT

	var transEvents []transformer.TransformerEvent
	transformationVersionID := config.GetString("TRANSFORMATION_VERSION_ID", "")

	for sc.Scan() {
		lineBytes := sc.Bytes()
		copyLineBytes := make([]byte, len(lineBytes))
		copy(copyLineBytes, lineBytes)

		if transformationVersionID == "" {
			timeStamp := gjson.GetBytes(copyLineBytes, worker.getFieldIdentifier(createdAt)).String()
			createdAt, err := time.Parse(misc.NOTIMEZONEFORMATPARSE, getFormattedTimeStamp(timeStamp))
			if err != nil {
				worker.log.Errorf("failed to parse created at: %s", err)
				continue
			}
			if !(worker.replayHandler.dumpsLoader.startTime.Before(createdAt) && worker.replayHandler.dumpsLoader.endTime.After(createdAt)) {
				continue
			}
			job := jobsdb.JobT{
				UUID:         uuid.New(),
				UserID:       gjson.GetBytes(copyLineBytes, worker.getFieldIdentifier(userID)).String(),
				Parameters:   []byte(gjson.GetBytes(copyLineBytes, worker.getFieldIdentifier(parameters)).String()),
				CustomVal:    gjson.GetBytes(copyLineBytes, worker.getFieldIdentifier(customVal)).String(),
				EventPayload: []byte(gjson.GetBytes(copyLineBytes, worker.getFieldIdentifier(eventPayload)).String()),
				WorkspaceId:  gjson.GetBytes(copyLineBytes, worker.getFieldIdentifier(workspaceID)).String(),
			}
			jobs = append(jobs, &job)
			continue
		}

		message, ok := gjson.ParseBytes(copyLineBytes).Value().(map[string]interface{})
		if !ok {
			worker.log.Errorf("EventPayload not a json: %v", copyLineBytes)
			continue
		}

		messageID := uuid.New().String()

		metadata := transformer.Metadata{
			MessageID:     messageID,
			DestinationID: gjson.GetBytes(copyLineBytes, "parameters.destination_id").String(),
		}

		transformation := backendconfig.TransformationT{VersionID: config.GetString("TRANSFORMATION_VERSION_ID", "")}

		transEvent := transformer.TransformerEvent{
			Message:     message,
			Metadata:    metadata,
			Destination: backendconfig.DestinationT{Transformations: []backendconfig.TransformationT{transformation}},
		}

		transEvents = append(transEvents, transEvent)
	}

	if transformationVersionID != "" {
		response := worker.transformer.UserTransform(context.TODO(), transEvents, userTransformBatchSize.Load())

		for _, ev := range response.Events {
			destEventJSON, err := json.Marshal(ev.Output[worker.getFieldIdentifier(eventPayload)])
			if err != nil {
				worker.log.Errorf("Error unmarshalling transformer output: %v", err)
				continue
			}
			createdAtString, ok := ev.Output[worker.getFieldIdentifier(createdAt)].(string)
			if !ok {
				worker.log.Errorf("Error getting created at from transformer output: %v", err)
				continue
			}
			createdAt, err := time.Parse(misc.NOTIMEZONEFORMATPARSE, getFormattedTimeStamp(createdAtString))
			if err != nil {
				worker.log.Errorf("failed to parse created at: %s", err)
				continue
			}
			if !(worker.replayHandler.dumpsLoader.startTime.Before(createdAt) && worker.replayHandler.dumpsLoader.endTime.After(createdAt)) {
				continue
			}
			params, err := json.Marshal(ev.Output[worker.getFieldIdentifier(parameters)])
			if err != nil {
				worker.log.Errorf("Error unmarshalling transformer output: %v", err)
				continue
			}
			job := jobsdb.JobT{
				UUID:         uuid.New(),
				UserID:       ev.Output[worker.getFieldIdentifier(userID)].(string),
				Parameters:   params,
				CustomVal:    ev.Output[worker.getFieldIdentifier(customVal)].(string),
				EventPayload: destEventJSON,
				WorkspaceId:  ev.Output[worker.getFieldIdentifier(workspaceID)].(string),
			}
			jobs = append(jobs, &job)
		}

		for _, failedEv := range response.FailedEvents {
			worker.log.Errorf(`Event failed in transformer with err: %v`, failedEv.Error)
		}

	}
	worker.log.Infof("brt-debug: TO_DB=%s", worker.replayHandler.toDB.Identifier())

	err = worker.replayHandler.toDB.Store(ctx, jobs)
	if err != nil {
		panic(err)
	}

	err = os.Remove(path)
	if err != nil {
		worker.log.Errorf("[%s]: failed to remove file with error: %w", err)
	}
}

const (
	userID       = "userID"
	parameters   = "parameters"
	customVal    = "customVal"
	eventPayload = "eventPayload"
	workspaceID  = "workspaceID"
	createdAt    = "createdAt"
)

func (worker *SourceWorkerT) getFieldIdentifier(field string) string {
	if worker.tablePrefix == "gw" {
		switch field {
		case userID:
			return "user_id"
		case parameters:
			return "parameters"
		case customVal:
			return "custom_val"
		case eventPayload:
			return "event_payload"
		case workspaceID:
			return "workspace_id"
		case createdAt:
			return "created_at"
		default:
			return ""
		}
	}
	switch field {
	case userID:
		return "UserID"
	case parameters:
		return "Parameters"
	case customVal:
		return "CustomVal"
	case eventPayload:
		return "EventPayload"
	case workspaceID:
		return "WorkspaceID"
	case createdAt:
		return "CreatedAt"
	default:
		return ""
	}
}

func getFormattedTimeStamp(timeStamp string) string {
	return strings.Split(strings.TrimSuffix(timeStamp, "Z"), ".")[0]
}

package replay

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/tidwall/gjson"
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

var userTransformBatchSize int

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
		err := worker.replayHandler.db.UpdateJobStatus(ctx, []*jobsdb.JobStatusT{&status}, []string{"replay"}, nil)
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

	var jobs []*MessageJob
	var lineNumber int
	for sc.Scan() {
		lineBytes := sc.Bytes()
		copyLineBytes := make([]byte, len(lineBytes))
		copy(copyLineBytes, lineBytes)

		timeStamp := gjson.GetBytes(copyLineBytes, worker.getFieldIdentifier(createdAt)).String()
		createdAt, err := time.Parse(misc.NOTIMEZONEFORMATPARSE, getFormattedTimeStamp(timeStamp))
		if err != nil {
			worker.log.Errorf("failed to parse created at: %s", err)
			continue
		}
		if !(worker.replayHandler.dumpsLoader.startTime.Before(createdAt) && worker.replayHandler.dumpsLoader.endTime.After(createdAt)) {
			continue
		}
		eventPayloadString := gjson.GetBytes(copyLineBytes, worker.getFieldIdentifier(eventPayload)).String()
		eventsBatch := gjson.GetBytes([]byte(eventPayloadString), "batch").Array()
		sourceId := gjson.GetBytes(copyLineBytes, "parameters.source_id").String()
		workspaceIDString := gjson.GetBytes(copyLineBytes, "workspace_id").String()
		for _, event := range eventsBatch {
			sdkVersion := gjson.GetBytes([]byte(event.Raw), "context.library.version").String()
			sdkName := gjson.GetBytes([]byte(event.Raw), "context.library.name").String()
			userAgent := gjson.GetBytes([]byte(event.Raw), "context.userAgent").String()
			anonymousID := gjson.GetBytes([]byte(event.Raw), "anonymousId").String()
			userIDString := gjson.GetBytes([]byte(event.Raw), "userId").String()
			messageID := gjson.GetBytes([]byte(event.Raw), "messageId").String()
			sentAt := gjson.GetBytes([]byte(event.Raw), "originalTimestamp").String()
			sentAtFormatted, err := time.Parse(misc.NOTIMEZONEFORMATPARSE, getFormattedTimeStamp(sentAt))
			if err != nil {
				worker.log.Errorf("failed to parse sent at: %s sentAt : %s", err, sentAt)
				continue
			}
			job := MessageJob{
				UserAgent:   userAgent,
				AnonymousID: anonymousID,
				UserID:      userIDString,
				SDKVersion:  sdkVersion,
				SDKName:     sdkName,
				MessageID:   messageID,
				CreatedAt:   sentAtFormatted,
				SourceID:    sourceId,
				WorkspaceID: workspaceIDString,
				FilePath:    filePath,
				LineNumber:  lineNumber,
			}
			jobs = append(jobs, &job)
		}
		lineNumber++
	}

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
	stringsArr := strings.Split(strings.TrimSuffix(timeStamp, "Z"), ".")[0]
	if len(stringsArr) == 19 {
		return stringsArr
	}
	return timeStamp
}

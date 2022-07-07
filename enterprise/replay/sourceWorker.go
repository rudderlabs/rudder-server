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

	uuid "github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/replay/fileuploader"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/tidwall/gjson"
)

type SourceWorkerT struct {
	channel     chan *jobsdb.JobT
	workerID    int
	replayer    *HandleT
	tablePrefix string
	transformer transformer.Transformer
}

var userTransformBatchSize int

func (worker *SourceWorkerT) workerProcess() {
	pkgLogger.Debugf("worker started %d", worker.workerID)
	for job := range worker.channel {
		pkgLogger.Debugf("job received: %s", job.EventPayload)

		worker.replayJobsInFile(gjson.GetBytes(job.EventPayload, "location").String())

		status := jobsdb.JobStatusT{
			JobID:         job.JobID,
			JobState:      jobsdb.Succeeded.State,
			ExecTime:      time.Now(),
			RetryTime:     time.Now(),
			ErrorCode:     "",
			ErrorResponse: []byte(`{}`), // check
			Parameters:    []byte(`{}`), // check
		}
		worker.replayer.db.UpdateJobStatus([]*jobsdb.JobStatusT{&status}, []string{"replay"}, nil)
	}
}

func (worker *SourceWorkerT) replayJobsInFile(filePath string) {
	filePathTokens := strings.Split(filePath, "/")

	var err error
	dumpDownloadPathDirName := "/rudder-s3-dumps/"
	tmpdirPath := strings.TrimSuffix(config.GetEnv("RUDDER_TMPDIR", "/tmp"), "/")
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
		panic(err) // Cant open file to write
	}

	_, err = fileuploader.Download(file, filePath, worker.replayer.bucket)
	if err != nil {
		panic(err) // failed to download
	}
	pkgLogger.Debugf("file downloaded at %s", path)
	file.Close()

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

	defer rawf.Close()

	jobs := []*jobsdb.JobT{}

	var transEvents []transformer.TransformerEventT
	transformationVersionID := config.GetEnv("TRANSFORMATION_VERSION_ID", "")

	for sc.Scan() {
		lineBytes := sc.Bytes()
		copyLineBytes := make([]byte, len(lineBytes))
		copy(copyLineBytes, lineBytes)

		if transformationVersionID == "" {
			job := jobsdb.JobT{
				UUID:         uuid.Must(uuid.NewV4()),
				UserID:       gjson.GetBytes(copyLineBytes, worker.getFieldIdentifier(userID)).String(),
				Parameters:   []byte(gjson.GetBytes(copyLineBytes, worker.getFieldIdentifier(parameters)).String()),
				CustomVal:    gjson.GetBytes(copyLineBytes, worker.getFieldIdentifier(customVal)).String(),
				EventPayload: []byte(gjson.GetBytes(copyLineBytes, worker.getFieldIdentifier(eventPayload)).String()),
			}
			jobs = append(jobs, &job)
			continue
		}

		// message, ok := gjson.GetBytes(copyLineBytes, worker.getFieldIdentifier(eventPayload)).Value().(map[string]interface{})
		message, ok := gjson.ParseBytes(copyLineBytes).Value().(map[string]interface{})
		if !ok {
			pkgLogger.Errorf("EventPayload not a json: %v", copyLineBytes)
			continue
		}

		messageID := uuid.Must(uuid.NewV4()).String()

		metadata := transformer.MetadataT{
			MessageID:     messageID,
			DestinationID: gjson.GetBytes(copyLineBytes, "parameters.destination_id").String(),
		}

		transformation := backendconfig.TransformationT{VersionID: config.GetEnv("TRANSFORMATION_VERSION_ID", "")}

		transEvent := transformer.TransformerEventT{
			Message:     message,
			Metadata:    metadata,
			Destination: backendconfig.DestinationT{Transformations: []backendconfig.TransformationT{transformation}},
		}

		transEvents = append(transEvents, transEvent)
	}

	if transformationVersionID != "" {
		response := worker.transformer.Transform(context.TODO(), transEvents, integrations.GetUserTransformURL(), userTransformBatchSize)

		for _, ev := range response.Events {
			destEventJSON, err := json.Marshal(ev.Output[worker.getFieldIdentifier(eventPayload)])
			if err != nil {
				pkgLogger.Errorf("Error unmarshalling transformer output: %v", err)
				continue
			}
			params, err := json.Marshal(ev.Output[worker.getFieldIdentifier(parameters)])
			if err != nil {
				pkgLogger.Errorf("Error unmarshalling transformer output: %v", err)
				continue
			}
			job := jobsdb.JobT{
				UUID:         uuid.Must(uuid.NewV4()),
				UserID:       ev.Output[worker.getFieldIdentifier(userID)].(string),
				Parameters:   params,
				CustomVal:    ev.Output[worker.getFieldIdentifier(customVal)].(string),
				EventPayload: destEventJSON,
			}
			jobs = append(jobs, &job)
		}

		for _, failedEv := range response.FailedEvents {
			pkgLogger.Errorf(`Event failed in transformer with err: %v`, failedEv.Error)
		}

	}
	pkgLogger.Infof("brt-debug: TO_DB=%s", worker.replayer.toDB.Identifier())

	err = worker.replayer.toDB.Store(jobs)
	if err != nil {
		panic(err)
	}

	err = os.Remove(path)
	if err != nil {
		pkgLogger.Errorf("[%s]: failed to remove file with error: %w", err)
	}
}

const (
	userID       = "userID"
	parameters   = "parameters"
	customVal    = "customVal"
	eventPayload = "eventPaylod"
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
	default:
		return ""
	}
}

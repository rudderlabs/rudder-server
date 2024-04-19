package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stringify"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	kitconfig "github.com/rudderlabs/rudder-go-kit/config"

	"github.com/rudderlabs/rudder-go-kit/bytesize"

	"github.com/rudderlabs/rudder-server/utils/types"

	jsoniter "github.com/json-iterator/go"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	kitlogger "github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/enterprise/replay/utils"
)

const (
	gatewayJobsFilePrefix = "gw_jobs_"
	localDumpDirName      = "/rudder-s3-dumps/"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type fileMigrator struct {
	conf        *config
	logger      kitlogger.Logger
	fileManager filemanager.FileManager
}

type config struct {
	startTime            time.Time
	endTime              time.Time
	backupFileNamePrefix string
	uploadBatchSize      int
}

// list all the files that needs to be migrated
func (m *fileMigrator) listFilePathToMigrate(ctx context.Context) []*backupFileInfo {
	listOfFiles := make([]*backupFileInfo, 0)

	startTimeMilli := m.conf.startTime.UnixNano() / int64(time.Millisecond)
	endTimeMilli := m.conf.endTime.UnixNano() / int64(time.Millisecond)

	iterator := filemanager.IterateFilesWithPrefix(ctx,
		m.conf.backupFileNamePrefix,
		"",
		100,
		m.fileManager,
	)
	for iterator.Next() {
		object := iterator.Get()
		filePath := object.Key
		if !strings.Contains(filePath, gatewayJobsFilePrefix) {
			continue
		}

		// file name should be of format gw_jobs_9710.974705928.974806056.1604871241214.1604872598504.gz
		fileInfo, err := newBackupFileInfo(filePath)
		if err != nil {
			m.logger.Warnn("unable to parse file name", kitlogger.NewStringField("filePath", filePath), obskit.Error(err))
			continue
		}

		// gw dump file name format gw_jobs_<table_index>.<start_job_id>.<end_job_id>.<min_created_at>_<max_created_at>.gz
		// ex: gw_jobs_9710.974705928.974806056.1604871241214.1604872598504.gz
		minJobCreatedAt, maxJobCreatedAt, err := utils.GetMinMaxCreatedAt(filePath)
		var pass bool
		if err == nil {
			pass = maxJobCreatedAt >= startTimeMilli && minJobCreatedAt <= endTimeMilli
		} else {
			m.logger.Warnn("parsing failed, fallback to comparing start and end time stamps with gw dump last modified.", kitlogger.NewStringField("filePath", filePath), obskit.Error(err))
			pass = object.LastModified.After(m.conf.startTime) && object.LastModified.Before(m.conf.endTime)
		}
		if pass {
			listOfFiles = append(listOfFiles, fileInfo)
		}
	}
	return listOfFiles
}

// convert old file line entry to new file format
func (m *fileMigrator) convertToNewFormat(lineBytes []byte, createdAt time.Time) ([]*newFileFormat, error) {
	var gatewayBatchReq []types.SingularEventT
	err := jsonfast.Unmarshal([]byte(gjson.GetBytes(lineBytes, "event_payload.batch").String()), &gatewayBatchReq)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshall gateway events: %w", err)
	}
	listOfNewEvents := make([]*newFileFormat, 0, len(gatewayBatchReq))
	userID := gjson.GetBytes(lineBytes, "user_id").String()
	for _, singleEvent := range gatewayBatchReq {
		payloadBytes, err := json.Marshal(singleEvent)
		if err != nil {
			return nil, fmt.Errorf("unable to marshall single event: %w", err)
		}
		j := &newFileFormat{}
		j.UserID = userID
		j.EventPayload = payloadBytes
		j.CreatedAt = createdAt
		j.MessageID = stringify.Any(singleEvent["messageId"])
		listOfNewEvents = append(listOfNewEvents, j)
	}
	return listOfNewEvents, nil
}

// uploadFile creates a new format file and upload to storage
func (m *fileMigrator) uploadFile(ctx context.Context, jobs []*newFileFormat, sourceId, workspaceId, instanceId string) error {
	firstJobCreatedAt := jobs[0].CreatedAt.UTC()
	lastJobCreatedAt := jobs[len(jobs)-1].CreatedAt.UTC()

	gzWriter := fileuploader.NewGzMultiFileWriter()
	localFilePath := path.Join(
		lo.Must(misc.CreateTMPDIR()),
		"rudder-backups",
		sourceId,
		fmt.Sprintf("%d_%d_%s.json.gz", firstJobCreatedAt.Unix(), lastJobCreatedAt.Unix(), workspaceId),
	)

	for _, job := range jobs {
		jobBytes, err := json.Marshal(job)
		if err != nil {
			_ = gzWriter.Close()
			return fmt.Errorf("failed to marshal job: %w", err)
		}
		if _, err := gzWriter.Write(localFilePath, append(jobBytes, '\n')); err != nil {
			_ = gzWriter.Close()
			return fmt.Errorf("write to local file failed: %w", err)
		}
	}
	if err := gzWriter.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}
	defer func() { _ = os.Remove(localFilePath) }()

	localFile, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("opening local file: %w", err)
	}
	defer func() { _ = localFile.Close() }()
	prefixes := []string{
		sourceId,
		"gw",
		firstJobCreatedAt.Format("2006-01-02"),
		fmt.Sprintf("%d", firstJobCreatedAt.Hour()),
		instanceId,
	}
	_, err = m.fileManager.Upload(ctx, localFile, prefixes...)
	if err != nil {
		return fmt.Errorf("failed to upload file: %w", err)
	}
	return nil
}

// download file locally
func (m *fileMigrator) downloadFile(ctx context.Context, filePath string) (string, error) {
	filePathTokens := strings.Split(filePath, "/")
	// e.g. rudder-saas/dummy/dummy-v0-rudderstack-10/gw_jobs_11796.317963152.317994396.1703547948443.1703548519552.dummy-workspace-id.gz
	tmpdirPath, err := misc.CreateTMPDIR()
	if err != nil {
		return "", fmt.Errorf("failed to create tmp directory: %w", err)
	}
	tempPath := path.Join(tmpdirPath, localDumpDirName, filePathTokens[len(filePathTokens)-1])

	err = os.MkdirAll(filepath.Dir(tempPath), os.ModePerm)
	if err != nil {
		return "", fmt.Errorf("failed to make local directory: %w", err)
	}

	file, err := os.Create(tempPath)
	if err != nil {
		return "", fmt.Errorf("make local file: %w", err)
	}
	defer func() {
		err = file.Close()
		if err != nil {
			m.logger.Errorn("closing local file", kitlogger.NewStringField("tempFilePath", tempPath), obskit.Error(err))
		}
	}()

	err = m.fileManager.Download(ctx, file, filePath)
	if err != nil {
		return "", fmt.Errorf("downloading file: %w", err)
	}
	m.logger.Debugf("file downloaded at %s", tempPath)
	return tempPath, nil
}

func (m *fileMigrator) processFile(ctx context.Context, fileInfo *backupFileInfo) error {
	instanceID := fileInfo.instance
	workspaceID := fileInfo.workspaceID

	localFilePath, err := m.downloadFile(ctx, fileInfo.filePath)
	if err != nil {
		return fmt.Errorf("failed to download file locally, err: %w", err)
	}
	defer func() { _ = os.Remove(localFilePath) }()

	rawFile, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("creating gzip reader, err: %w", err)
	}

	reader, err := gzip.NewReader(rawFile)
	if err != nil {
		return fmt.Errorf("failed to get new gzip reader, err: %w", err)
	}

	sc := bufio.NewScanner(reader)
	// default scanner buffer maxCapacity is 64K
	// set it to higher value to avoid read stop on read size error
	maxCapacity := kitconfig.GetInt64("FileMigrator.maxScannerCapacity", 10*bytesize.MB) // 10MB
	buf := make([]byte, maxCapacity)
	sc.Buffer(buf, int(maxCapacity))

	defer func() { _ = rawFile.Close() }()

	eventsToDump := make(map[string][]*newFileFormat)
	for sc.Scan() {
		lineBytes := sc.Bytes()
		copyLineBytes := make([]byte, len(lineBytes))
		copy(copyLineBytes, lineBytes)
		timeStamp := gjson.GetBytes(copyLineBytes, "created_at").String()
		createdAt, err := time.Parse(time.RFC3339Nano, timeStamp)
		if err != nil {
			return fmt.Errorf("failed to parse created_at, err: %w", err)
		}
		if !(m.conf.startTime.Before(createdAt) && m.conf.endTime.After(createdAt)) {
			continue
		}

		// convert to new format
		newFormatFileEntries, err := m.convertToNewFormat(copyLineBytes, createdAt)
		if err != nil {
			return fmt.Errorf("failed to convert to new file format, err: %w", err)
		}
		sourceID := gjson.GetBytes(copyLineBytes, "parameters.source_id").String()
		if eventsToDump[sourceID] == nil {
			eventsToDump[sourceID] = make([]*newFileFormat, 0)
		}
		// prepare batch dump to a new file
		eventsToDump[sourceID] = append(eventsToDump[sourceID], newFormatFileEntries...)
		// save to new file
		if len(eventsToDump[sourceID]) >= m.conf.uploadBatchSize {
			err := m.uploadFile(ctx, eventsToDump[sourceID], sourceID, workspaceID, instanceID)
			if err != nil {
				return fmt.Errorf("failed to upload file, sourceID:%s, err: %w", sourceID, err)
			}
			delete(eventsToDump, sourceID)
		}
	}
	for sourceID, jobs := range eventsToDump {
		err := m.uploadFile(ctx, jobs, sourceID, workspaceID, instanceID)
		if err != nil {
			return fmt.Errorf("failed to upload file, sourceID:%s, err: %w", sourceID, err)
		}
	}
	return nil
}

package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"os"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	kitConfig "github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/utils/filemanagerutil"
)

type newFileFormat struct {
	UserID       string          `json:"userId"`
	EventPayload json.RawMessage `json:"payload"`
	CreatedAt    time.Time       `json:"createdAt"`
	MessageID    string          `json:"messageId"`
}

// step 1: fetch files to migrate
func main() {
	ctx := context.Background()
	log := logger.NewLogger().Child("backup-file-migrator")
	conf := kitConfig.Default

	var startTimeStr string
	var endTimeStr string
	var uploadBatchSize int
	var backupFileNamePrefix string
	flag.StringVar(&startTimeStr, "startTime", "", "start time from which we need to convert files in RFC3339 format")
	flag.StringVar(&endTimeStr, "endTime", "", "end time upto which we need to convert files in RFC3339 format")
	flag.IntVar(&uploadBatchSize, "uploadBatchSize", 1000, "no. of messages to upload in single file")
	flag.StringVar(&backupFileNamePrefix, "backupFileNamePrefix", "", "prefix to identify files in the bucket")
	flag.Parse()

	startTime, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(startTimeStr))
	if err != nil {
		log.Errorn("unable to parse start time", logger.NewStringField("startTime", startTimeStr), obskit.Error(err))
		return
	}
	endTime, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(endTimeStr))
	if err != nil {
		log.Errorn("unable to parse end time", logger.NewStringField("endTime", endTimeStr), obskit.Error(err))
		return
	}
	backupFileNamePrefix = strings.TrimSpace(backupFileNamePrefix)
	if backupFileNamePrefix == "" {
		log.Errorn("backupFileNamePrefix should not be empty")
	}

	storageProvider := kitConfig.GetString("JOBS_BACKUP_STORAGE_PROVIDER", "S3")
	fileManagerOpts := filemanagerutil.ProviderConfigOpts(ctx, storageProvider, conf)
	fileManagerOpts.Prefix = backupFileNamePrefix
	fileManager, err := filemanager.New(&filemanager.Settings{
		Provider: storageProvider,
		Config:   filemanager.GetProviderConfigFromEnv(fileManagerOpts),
		Conf:     conf,
	})
	if err != nil {
		log.Errorf("error creating file manager: %s", err.Error())
		return
	}

	migratorConfig := &config{
		startTime:            startTime,
		endTime:              endTime,
		backupFileNamePrefix: backupFileNamePrefix,
		uploadBatchSize:      uploadBatchSize,
	}

	migrator := &fileMigrator{
		conf:        migratorConfig,
		logger:      log,
		fileManager: fileManager,
	}
	// list all files to migrate
	listOfFiles := migrator.listFilePathToMigrate(ctx)

	// step 2: download data from file
	for _, filePath := range listOfFiles {
		filePathTokens := strings.Split(filePath, "/")
		// file path should be in format like {prefix}/{instance_id}/gw_jobs_<table_index>.<start_job_id>.<end_job_id>.<min_created_at>_<max_created_at>.gz
		// e.g. dummy/dummy-v0-rudderstack-10/gw_jobs_11796.317963152.317994396.1703547948443.1703548519552.workspace.gz
		if len(filePathTokens) < 3 {
			log.Errorn("file path is in invalid format", logger.NewStringField("filePath", filePath))
			continue
		}
		instanceID := filePathTokens[len(filePathTokens)-2]
		workspaceID := strings.Split(filePathTokens[len(filePathTokens)-1], ".")[len(strings.Split(filePathTokens[len(filePathTokens)-1], "."))-2]

		localFilePath, err := migrator.downloadFile(ctx, filePath)
		if err != nil {
			log.Errorn("error downloading file locally", logger.NewStringField("filePath", filePath), obskit.Error(err))
			continue
		}
		defer func() { _ = os.Remove(localFilePath) }()

		rawf, err := os.Open(localFilePath)
		if err != nil {
			log.Errorn("failed to open file", obskit.Error(err))
			continue // failed to open file
		}

		reader, err := gzip.NewReader(rawf)
		if err != nil {
			log.Errorn("failed to get new gzip reader", obskit.Error(err))
			continue // failed to read gzip file
		}

		sc := bufio.NewScanner(reader)
		// default scanner buffer maxCapacity is 64K
		// set it to higher value to avoid read stop on read size error
		maxCapacity := 10240 * 1024 // 10MB
		buf := make([]byte, maxCapacity)
		sc.Buffer(buf, maxCapacity)

		defer func() { _ = rawf.Close() }()

		eventsToDump := make(map[string][]*newFileFormat)
		for sc.Scan() {
			lineBytes := sc.Bytes()
			copyLineBytes := make([]byte, len(lineBytes))
			copy(copyLineBytes, lineBytes)
			timeStamp := gjson.GetBytes(copyLineBytes, "created_at").String()
			createdAt, err := time.Parse(time.RFC3339Nano, timeStamp)
			if err != nil {
				log.Errorf("failed to parse created at: %s", err)
				continue
			}
			if !(startTime.Before(createdAt) && endTime.After(createdAt)) {
				continue
			}

			// step 3: convert to new format
			newFormatFileEntries, err := migrator.convertToNewFormat(copyLineBytes, createdAt)
			if err != nil {
				log.Errorn("failed to get new file entries", obskit.Error(err))
				return
			}
			sourceID := gjson.GetBytes(copyLineBytes, "parameters.source_id").String()
			if eventsToDump[sourceID] == nil {
				eventsToDump[sourceID] = make([]*newFileFormat, 0)
			}
			// step 4: prepare batch dump to a new file
			eventsToDump[sourceID] = append(eventsToDump[sourceID], newFormatFileEntries...)
			// step 5: save to new file
			if len(eventsToDump[sourceID]) == uploadBatchSize {
				err := migrator.uploadFile(ctx, eventsToDump[sourceID], sourceID, workspaceID, instanceID)
				if err != nil {
					log.Errorn("failed to upload files", obskit.Error(err))
					return
				}
				delete(eventsToDump, sourceID)
			}
		}
		for sourceID, jobs := range eventsToDump {
			err := migrator.uploadFile(ctx, jobs, sourceID, workspaceID, instanceID)
			if err != nil {
				log.Errorn("failed to upload files", obskit.Error(err))
				return
			}
		}
	}
}

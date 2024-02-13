package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	path2 "path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	kitConfig "github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-server/utils/types"

	jsoniter "github.com/json-iterator/go"

	"github.com/pkg/errors"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/services/fileuploader"
	"github.com/rudderlabs/rudder-server/utils/misc"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/enterprise/replay/utils"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

type fileMigrator struct {
	conf        *config
	logger      logger.Logger
	fileManager filemanager.FileManager
}

type config struct {
	startTime            time.Time
	endTime              time.Time
	backupFileNamePrefix string
	uploadBatchSize      int
}

// list all the files that needs to be migrated
func (m *fileMigrator) listFilePathToMigrate(ctx context.Context) []string {
	listOfFiles := make([]string, 0)
	iter := filemanager.IterateFilesWithPrefix(ctx,
		m.conf.backupFileNamePrefix,
		"",
		100,
		m.fileManager,
	)
	for iter.Next() {
		object := iter.Get()
		filePath := object.Key
		if strings.Contains(filePath, "gw_jobs_") {
			startTimeMilli := m.conf.startTime.UnixNano() / int64(time.Millisecond)
			endTimeMilli := m.conf.endTime.UnixNano() / int64(time.Millisecond)
			key := object.Key

			// file name should be of format gw_jobs_9710.974705928.974806056.1604871241214.1604872598504.gz
			tokens := strings.Split(key, "gw_jobs_")
			tokens = strings.Split(tokens[1], ".")
			if _, err := strconv.Atoi(tokens[0]); err != nil {
				continue
			}

			// gw dump file name format gw_jobs_<table_index>.<start_job_id>.<end_job_id>.<min_created_at>_<max_created_at>.gz
			// ex: gw_jobs_9710.974705928.974806056.1604871241214.1604872598504.gz
			minJobCreatedAt, maxJobCreatedAt, err := utils.GetMinMaxCreatedAt(object.Key)
			var pass bool
			if err == nil {
				pass = maxJobCreatedAt >= startTimeMilli && minJobCreatedAt <= endTimeMilli
			} else {
				m.logger.Infof("gw dump name(%s) is not of the expected format. Parse failed with error %w", object.Key, err)
				m.logger.Info("Falling back to comparing start and end time stamps with gw dump last modified.")
				pass = object.LastModified.After(m.conf.startTime) && object.LastModified.Before(m.conf.endTime)
			}
			if pass {
				listOfFiles = append(listOfFiles, object.Key)
			}
		}
	}
	return listOfFiles
}

// convert old file line entry to new file format
func (m *fileMigrator) convertToNewFormat(lineBytes []byte, createdAt time.Time) ([]*newFileFormat, error) {
	var gatewayBatchReq []types.SingularEventT
	err := jsonfast.Unmarshal([]byte(gjson.GetBytes(lineBytes, "event_payload.batch").String()), &gatewayBatchReq)
	if err != nil {
		return nil, errors.Wrap(err, "unable to unmarshall gateway events")
	}
	listOfNewEvents := make([]*newFileFormat, 0)
	userId := gjson.GetBytes(lineBytes, "user_id").String()
	for _, singleEvent := range gatewayBatchReq {
		payloadBytes, err := json.Marshal(singleEvent)
		if err != nil {
			return nil, errors.Wrap(err, "unable to marshall single event")
		}
		j := &newFileFormat{}
		j.UserID = userId
		j.EventPayload = payloadBytes
		j.CreatedAt = createdAt
		j.MessageID = misc.GetStringifiedData(singleEvent["messageId"])
		listOfNewEvents = append(listOfNewEvents, j)
	}
	return listOfNewEvents, nil
}

// uploadFile creates a new format file and upload to storage
func (m *fileMigrator) uploadFile(ctx context.Context, jobs []*newFileFormat, sourceId, workspaceId, instanceId string) error {
	firstJobCreatedAt := jobs[0].CreatedAt.UTC()
	lastJobCreatedAt := jobs[len(jobs)-1].CreatedAt.UTC()

	gzWriter := fileuploader.NewGzMultiFileWriter()
	localFilePath := path2.Join(
		lo.Must(misc.CreateTMPDIR()),
		"rudder-backups",
		sourceId,
		fmt.Sprintf("%d_%d_%s.json.gz", firstJobCreatedAt.Unix(), lastJobCreatedAt.Unix(), workspaceId),
	)

	for _, job := range jobs {
		jobBytes, err := json.Marshal(job)
		if err != nil {
			_ = gzWriter.Close()
			return errors.Wrap(err, "failed to marshal job")
		}
		if _, err := gzWriter.Write(localFilePath, append(jobBytes, '\n')); err != nil {
			_ = gzWriter.Close()
			return errors.Wrap(err, "write to local file failed")
		}
	}
	if err := gzWriter.Close(); err != nil {
		return errors.Wrap(err, "failed to close writer")
	}
	defer func() { _ = os.Remove(localFilePath) }()

	localFile, err := os.Open(localFilePath)
	if err != nil {
		return errors.Wrap(err, "failed to open local file")
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
		return errors.Wrap(err, "failed to upload file")
	}
	return nil
}

// download file locally
func (m *fileMigrator) downloadFile(ctx context.Context, filePath string) (string, error) {
	filePathTokens := strings.Split(filePath, "/")
	// backup file is present in folder named instance id
	// e.g. rudder-saas/dummy/dummy-v0-rudderstack-10/gw_jobs_11796.317963152.317994396.1703547948443.1703548519552.dummy-workspace-id.gz
	var err error
	dumpDownloadPathDirName := "/rudder-s3-dumps/"
	tmpdirPath := strings.TrimSuffix(kitConfig.GetString("RUDDER_TMPDIR", "/tmp"), "/")
	if tmpdirPath == "" {
		tmpdirPath, err = os.UserHomeDir()
		if err != nil {
			return "", errors.Wrap(err, "failed to get user home directory")
		}
	}
	path := fmt.Sprintf(
		`%v%v%v`, tmpdirPath, dumpDownloadPathDirName, filePathTokens[len(filePathTokens)-1])

	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		return "", errors.Wrap(err, "failed to make local directory")
	}

	file, err := os.Create(path)
	if err != nil {
		return "", errors.Wrap(err, "failed to make local file")
	}

	err = m.fileManager.Download(ctx, file, filePath)
	if err != nil {
		return "", errors.Wrap(err, "failed to download file")
	}
	m.logger.Debugf("file downloaded at %s", path)
	defer func() { _ = file.Close() }()
	return path, nil
}

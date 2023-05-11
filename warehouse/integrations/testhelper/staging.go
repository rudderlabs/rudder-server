package testhelper

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"text/template"
	"time"
)

func createStagingFile(t testing.TB, testConfig *TestConfig) {
	stagingFile := prepareStagingFile(t, testConfig)

	uploadOutput := uploadStagingFile(t, testConfig, stagingFile)

	payload := prepareStagingPayload(t, testConfig, stagingFile, uploadOutput)

	err := warehouseclient.NewWarehouse(misc.GetWarehouseURL()).Process(context.Background(), payload)
	require.NoError(t, err)
}

func prepareStagingFile(t testing.TB, testConfig *TestConfig) string {
	t.Helper()

	path := fmt.Sprintf("%v%v.json", t.TempDir(), fmt.Sprintf("%d.%s.%s", time.Now().Unix(), testConfig.SourceID, uuid.New().String()))
	gzipFilePath := fmt.Sprintf(`%v.gz`, path)

	err := os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	require.NoError(t, err)

	gzWriter, err := misc.CreateGZ(gzipFilePath)
	require.NoError(t, err)
	defer func() { _ = gzWriter.CloseGZ() }()

	for eventTemplate, templ := range templatesMap {
		for i := 0; i < testConfig.EventTemplateCountMap[eventTemplate]; i++ {
			data := map[string]any{
				"userID":    testConfig.UserID,
				"destType":  testConfig.DestinationType,
				"msgID":     testConfig.msgID(),
				"recordID":  testConfig.recordID(),
				"jobRunID":  testConfig.JobRunID,
				"taskRunID": testConfig.TaskRunID,
				"sourceID":  testConfig.SourceID,
				"destID":    testConfig.DestinationID,
			}

			payload := parseTemplate(t, templ, data)
			payload += "\n"

			err = gzWriter.WriteGZ(payload)
			require.NoError(t, err)
		}
	}

	t.Cleanup(func() {
		if err := os.Remove(gzWriter.File.Name()); err != nil {
			t.Logf("failed to remove temp file: %s", gzWriter.File.Name())
		}
	})

	return gzipFilePath
}

func parseTemplate(t testing.TB, text string, data map[string]any) string {
	t.Helper()

	tpl, err := template.New(uuid.New().String()).Parse(text)
	require.NoError(t, err)

	b := new(strings.Builder)

	err = tpl.Execute(b, data)
	require.NoError(t, err)

	return b.String()
}

func uploadStagingFile(t testing.TB, testConfig *TestConfig, stagingFile string) filemanager.UploadOutput {
	t.Helper()

	storageProvider := warehouseutils.ObjectStorageType(testConfig.DestinationType, testConfig.Config, false)

	fm, err := filemanager.DefaultFileManagerFactory.New(&filemanager.SettingsT{
		Provider: storageProvider,
		Config: misc.GetObjectStorageConfig(misc.ObjectStorageOptsT{
			Provider:         storageProvider,
			Config:           testConfig.Config,
			UseRudderStorage: misc.IsConfiguredToUseRudderObjectStorage(testConfig.Config),
			WorkspaceID:      testConfig.WorkspaceID,
		}),
	})
	require.NoError(t, err)

	keyPrefixes := []string{"rudder-warehouse-staging-logs", testConfig.SourceID, time.Now().Format("2006-01-02")}

	f, err := os.Open(stagingFile)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	var uploadOutput filemanager.UploadOutput

	err = WithConstantRetries(func() error {
		if uploadOutput, err = fm.Upload(context.Background(), f, keyPrefixes...); err != nil {
			return fmt.Errorf("uploading staging file: %w", err)
		}

		return nil
	})
	require.NoError(t, err)

	return uploadOutput
}

func prepareStagingPayload(t testing.TB, testConfig *TestConfig, stagingFile string, uploadOutput filemanager.UploadOutput) warehouseclient.StagingFile {
	t.Helper()

	type StagingEvent struct {
		Metadata struct {
			Table   string            `json:"table"`
			Columns map[string]string `json:"columns"`
		}
		Data map[string]interface{} `json:"data"`
	}

	f, err := os.Open(stagingFile)
	require.NoError(t, err)

	reader, err := gzip.NewReader(f)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	scanner := bufio.NewScanner(reader)
	schemaMap := make(map[string]map[string]interface{})

	stagingEvents := make([]StagingEvent, 0)

	for scanner.Scan() {
		lineBytes := scanner.Bytes()

		var stagingEvent StagingEvent
		err := json.Unmarshal(lineBytes, &stagingEvent)
		require.NoError(t, err)

		stagingEvents = append(stagingEvents, stagingEvent)
	}

	for _, event := range stagingEvents {
		tableName := event.Metadata.Table

		if _, ok := schemaMap[tableName]; !ok {
			schemaMap[tableName] = make(map[string]interface{})
		}
		for columnName, columnType := range event.Metadata.Columns {
			if _, ok := schemaMap[tableName][columnName]; !ok {
				schemaMap[tableName][columnName] = columnType
			}
		}
	}

	receivedAt, err := time.Parse(time.RFC3339, stagingEvents[0].Data["received_at"].(string))
	require.NoError(t, err)

	stagingFileInfo, err := os.Stat(stagingFile)
	require.NoError(t, err)

	payload := warehouseclient.StagingFile{
		WorkspaceID:           testConfig.WorkspaceID,
		Schema:                schemaMap,
		SourceID:              testConfig.SourceID,
		DestinationID:         testConfig.DestinationID,
		DestinationRevisionID: testConfig.DestinationID,
		Location:              uploadOutput.ObjectName,
		FirstEventAt:          stagingEvents[0].Data["received_at"].(string),
		LastEventAt:           stagingEvents[len(stagingEvents)-1].Data["received_at"].(string),
		TotalEvents:           len(stagingEvents),
		TotalBytes:            int(stagingFileInfo.Size()),
		SourceTaskRunID:       testConfig.TaskRunID,
		SourceJobRunID:        testConfig.JobRunID,
		TimeWindow:            warehouseutils.GetTimeWindow(receivedAt),
	}
	return payload
}

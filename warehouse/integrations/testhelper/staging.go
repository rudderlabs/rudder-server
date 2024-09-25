package testhelper

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/filemanager"

	"github.com/rudderlabs/rudder-server/processor/transformer"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseclient "github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func createStagingFile(t testing.TB, testConfig *TestConfig) {
	var stagingFile string
	if testConfig.StagingFilePath != "" {
		stagingFile = prepareStagingFilePathUsingStagingFile(t, testConfig)
	} else {
		stagingFile = prepareStagingFilePathUsingEventsFile(t, testConfig)
	}

	uploadOutput := uploadStagingFile(t, testConfig, stagingFile)

	payload := prepareStagingPayload(t, testConfig, stagingFile, uploadOutput)

	url := fmt.Sprintf("http://localhost:%d", testConfig.HTTPPort)
	err := warehouseclient.NewWarehouse(url).Process(context.Background(), payload)
	require.NoError(t, err)
}

func prepareStagingFilePathUsingStagingFile(t testing.TB, testConfig *TestConfig) string {
	t.Helper()

	path := fmt.Sprintf("%v%v.json", t.TempDir(), fmt.Sprintf("%d.%s.%s", time.Now().Unix(), testConfig.SourceID, uuid.New().String()))
	gzipFilePath := fmt.Sprintf(`%v.gz`, path)

	err := os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	require.NoError(t, err)

	gzWriter, err := misc.CreateGZ(gzipFilePath)
	require.NoError(t, err)
	defer func() { _ = gzWriter.CloseGZ() }()

	f, err := os.ReadFile(testConfig.StagingFilePath)
	require.NoError(t, err)

	tpl, err := template.New(uuid.New().String()).Parse(string(f))
	require.NoError(t, err)

	b := new(strings.Builder)

	err = tpl.Execute(b, map[string]any{
		"userID":    testConfig.UserID,
		"sourceID":  testConfig.SourceID,
		"destID":    testConfig.DestinationID,
		"jobRunID":  testConfig.JobRunID,
		"taskRunID": testConfig.TaskRunID,
	})
	require.NoError(t, err)

	err = gzWriter.WriteGZ(b.String())
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := os.Remove(gzWriter.File.Name()); err != nil {
			t.Logf("failed to remove temp file: %s", gzWriter.File.Name())
		}
	})

	return gzipFilePath
}

func prepareStagingFilePathUsingEventsFile(t testing.TB, testConfig *TestConfig) string {
	t.Helper()

	path := fmt.Sprintf("%v%v.json", t.TempDir(), fmt.Sprintf("%d.%s.%s", time.Now().Unix(), testConfig.SourceID, uuid.New().String()))
	gzipFilePath := fmt.Sprintf(`%v.gz`, path)

	err := os.MkdirAll(filepath.Dir(gzipFilePath), os.ModePerm)
	require.NoError(t, err)

	gzWriter, err := misc.CreateGZ(gzipFilePath)
	require.NoError(t, err)
	defer func() { _ = gzWriter.CloseGZ() }()

	f, err := os.ReadFile(testConfig.EventsFilePath)
	require.NoError(t, err)

	tpl, err := template.New(uuid.New().String()).Parse(string(f))
	require.NoError(t, err)

	c := config.New()
	c.Set("DEST_TRANSFORM_URL", testConfig.TransformerURL)
	c.Set("USER_TRANSFORM_URL", testConfig.TransformerURL)

	b := new(strings.Builder)

	destinationJSON, err := json.Marshal(testConfig.Destination)
	require.NoError(t, err)

	err = tpl.Execute(b, map[string]any{
		"userID":      testConfig.UserID,
		"sourceID":    testConfig.SourceID,
		"workspaceID": testConfig.WorkspaceID,
		"destID":      testConfig.DestinationID,
		"destType":    testConfig.DestinationType,
		"destination": string(destinationJSON),
		"jobRunID":    testConfig.JobRunID,
		"taskRunID":   testConfig.TaskRunID,
	})
	require.NoError(t, err)

	var transformerEvents []transformer.TransformerEvent
	err = json.Unmarshal([]byte(b.String()), &transformerEvents)
	require.NoError(t, err)

	tr := transformer.NewTransformer(c, logger.NOP, stats.Default)
	response := tr.Transform(context.Background(), transformerEvents, 100)
	require.Zero(t, len(response.FailedEvents))
	responseOutputs := lo.Map(response.Events, func(r transformer.TransformerResponse, index int) map[string]interface{} {
		return r.Output
	})

	output := new(strings.Builder)
	for _, responseOutput := range responseOutputs {
		outputJSON, err := json.Marshal(responseOutput)
		require.NoError(t, err)

		_, err = output.WriteString(string(outputJSON) + "\n")
		require.NoError(t, err)
	}

	err = gzWriter.WriteGZ(output.String())
	require.NoError(t, err)

	t.Cleanup(func() {
		if err := os.Remove(gzWriter.File.Name()); err != nil {
			t.Logf("failed to remove temp file: %s", gzWriter.File.Name())
		}
	})

	return gzipFilePath
}

func uploadStagingFile(t testing.TB, testConfig *TestConfig, stagingFile string) filemanager.UploadedFile {
	t.Helper()

	storageProvider := warehouseutils.ObjectStorageType(testConfig.DestinationType, testConfig.Config, false)

	fm, err := filemanager.New(&filemanager.Settings{
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

	var uploadOutput filemanager.UploadedFile

	err = WithConstantRetries(func() error {
		if uploadOutput, err = fm.Upload(context.Background(), f, keyPrefixes...); err != nil {
			return fmt.Errorf("uploading staging file: %w", err)
		}

		return nil
	})
	require.NoError(t, err)

	return uploadOutput
}

func prepareStagingPayload(t testing.TB, testConfig *TestConfig, stagingFile string, uploadOutput filemanager.UploadedFile) warehouseclient.StagingFile {
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

	receivedAtProperty := "received_at"
	if testConfig.DestinationType == warehouseutils.SNOWFLAKE {
		receivedAtProperty = "RECEIVED_AT"
	}

	// merge rules and mappings events will not contain received_at, ignoring those
	eventsWithoutIDResolution := lo.Filter(stagingEvents, func(event StagingEvent, index int) bool {
		return event.Metadata.Table != warehouseutils.ToProviderCase(testConfig.DestinationType, warehouseutils.IdentityMergeRulesTable) &&
			event.Metadata.Table != warehouseutils.ToProviderCase(testConfig.DestinationType, warehouseutils.IdentityMappingsTable)
	})

	receivedAt, err := time.Parse(time.RFC3339, eventsWithoutIDResolution[0].Data[receivedAtProperty].(string))
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
		FirstEventAt:          eventsWithoutIDResolution[0].Data[receivedAtProperty].(string),
		LastEventAt:           eventsWithoutIDResolution[len(eventsWithoutIDResolution)-1].Data[receivedAtProperty].(string),
		TotalEvents:           len(stagingEvents),
		TotalBytes:            int(stagingFileInfo.Size()),
		SourceTaskRunID:       testConfig.TaskRunID,
		SourceJobRunID:        testConfig.JobRunID,
		TimeWindow:            warehouseutils.GetTimeWindow(receivedAt),
	}
	return payload
}

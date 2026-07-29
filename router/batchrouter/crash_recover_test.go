package batchrouter

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/filemanager/mock_filemanager"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/jobsdb"
	mocksJobsDB "github.com/rudderlabs/rudder-server/mocks/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

func TestCrashRecoverClosesFilesAndCleansUp(t *testing.T) {
	t.Parallel()

	tmpDir, err := misc.GetTmpDir()
	require.NoError(t, err)
	recoveryDir := filepath.Join(tmpDir, "rudder-raw-data-dest-upload-crash-recovery")
	require.NoError(t, os.MkdirAll(recoveryDir, 0o755))

	gzipPayload := func(t *testing.T, lines ...string) []byte {
		t.Helper()
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		for _, line := range lines {
			_, err := zw.Write([]byte(line + "\n"))
			require.NoError(t, err)
		}
		require.NoError(t, zw.Close())
		return buf.Bytes()
	}

	t.Run("successful recovery closes download and read handles and removes temp file", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockJobsDB := mocksJobsDB.NewMockJobsDB(ctrl)
		mockFM := mock_filemanager.NewMockFileManager(ctrl)

		payload, err := jsonrs.Marshal(ObjectStorageDefinition{
			Config:        map[string]any{"bucketName": "bucket"},
			Key:           "uploads/file.gz",
			Provider:      "S3",
			DestinationID: "dest-1",
		})
		require.NoError(t, err)

		mockJobsDB.EXPECT().GetJournalEntries(jobsdb.RawDataDestUploadOperation).Return([]jobsdb.JournalEntryT{
			{OpID: 42, OpPayload: payload},
		})
		mockJobsDB.EXPECT().JournalDeleteEntry(int64(42))

		content := gzipPayload(t, `{"messageId":"msg-1"}`, `{"messageId":"msg-2"}`)
		mockFM.EXPECT().Download(gomock.Any(), gomock.Any(), "uploads/file.gz").DoAndReturn(
			func(_ context.Context, w io.WriterAt, _ string, _ ...filemanager.DownloadOption) error {
				_, err := w.WriteAt(content, 0)
				return err
			},
		)

		before, err := os.ReadDir(recoveryDir)
		require.NoError(t, err)
		beforeCount := len(before)

		brt := &Handle{
			destType:                 "S3",
			logger:                   logger.NOP,
			conf:                     config.New(),
			jobsDB:                   mockJobsDB,
			uploadedRawDataJobsCache: make(map[string]map[string]bool),
			fileManagerFactory: func(_ *filemanager.Settings) (filemanager.FileManager, error) {
				return mockFM, nil
			},
		}
		brt.crashRecover()

		require.True(t, brt.uploadedRawDataJobsCache["dest-1"]["msg-1"])
		require.True(t, brt.uploadedRawDataJobsCache["dest-1"]["msg-2"])

		after, err := os.ReadDir(recoveryDir)
		require.NoError(t, err)
		require.Equal(t, beforeCount, len(after), "temp recovery files must be removed")
	})

	t.Run("download failure closes create handle and removes temp file", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockJobsDB := mocksJobsDB.NewMockJobsDB(ctrl)
		mockFM := mock_filemanager.NewMockFileManager(ctrl)

		payload, err := jsonrs.Marshal(ObjectStorageDefinition{
			Config:        map[string]any{"bucketName": "bucket"},
			Key:           "uploads/missing.gz",
			Provider:      "S3",
			DestinationID: "dest-2",
		})
		require.NoError(t, err)

		mockJobsDB.EXPECT().GetJournalEntries(jobsdb.RawDataDestUploadOperation).Return([]jobsdb.JournalEntryT{
			{OpID: 99, OpPayload: payload},
		})
		mockJobsDB.EXPECT().JournalDeleteEntry(int64(99))
		mockFM.EXPECT().Download(gomock.Any(), gomock.Any(), "uploads/missing.gz").Return(os.ErrNotExist)

		before, err := os.ReadDir(recoveryDir)
		require.NoError(t, err)
		beforeCount := len(before)

		brt := &Handle{
			destType:                 "S3",
			logger:                   logger.NOP,
			conf:                     config.New(),
			jobsDB:                   mockJobsDB,
			uploadedRawDataJobsCache: make(map[string]map[string]bool),
			fileManagerFactory: func(_ *filemanager.Settings) (filemanager.FileManager, error) {
				return mockFM, nil
			},
		}
		brt.crashRecover()

		after, err := os.ReadDir(recoveryDir)
		require.NoError(t, err)
		require.Equal(t, beforeCount, len(after), "temp recovery files must be removed on download failure")
		require.Empty(t, brt.uploadedRawDataJobsCache)
	})
}

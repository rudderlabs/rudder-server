package audience

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

func ensureAsyncDestLogsDir(t *testing.T) {
	t.Helper()
	tmpDirPath, err := misc.GetTmpDir()
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDirPath, misc.RudderAsyncDestinationLogs), 0o755))
}

func TestConvertCsvToZipClosesHandles(t *testing.T) {
	t.Parallel()
	ensureAsyncDestLogsDir(t)

	t.Run("empty event count closes write handle and removes csv", func(t *testing.T) {
		t.Parallel()

		actionFile, err := createActionFile("aud-1", "Add")
		require.NoError(t, err)
		require.NotNil(t, actionFile.CSVFile)

		writeHandle := actionFile.CSVFile
		require.NoError(t, convertCsvToZip(actionFile))
		require.Nil(t, actionFile.CSVFile)

		_, writeErr := writeHandle.Write([]byte("x"))
		require.Error(t, writeErr)
		_, statErr := os.Stat(actionFile.CSVFilePath)
		require.True(t, os.IsNotExist(statErr))
	})

	t.Run("non-empty event count closes write handle and creates zip", func(t *testing.T) {
		t.Parallel()

		actionFile, err := createActionFile("aud-1", "Add")
		require.NoError(t, err)
		actionFile.EventCount = 1
		actionFile.CSVWriter.Flush()

		writeHandle := actionFile.CSVFile
		require.NoError(t, convertCsvToZip(actionFile))
		require.Nil(t, actionFile.CSVFile)

		_, writeErr := writeHandle.Write([]byte("x"))
		require.Error(t, writeErr)

		_, err = os.Stat(actionFile.ZipFilePath)
		require.NoError(t, err)
		require.NoError(t, os.Remove(actionFile.ZipFilePath))
	})
}

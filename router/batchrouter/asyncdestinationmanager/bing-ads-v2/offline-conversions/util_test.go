package offline_conversions

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateActionFileTemplate(t *testing.T) {
	t.Parallel()

	t.Run("insert action template", func(t *testing.T) {
		t.Parallel()

		tempFile, err := os.CreateTemp("", "test_*.csv")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.Remove(tempFile.Name()))
		}()

		csvWriter, err := CreateActionFileTemplate(tempFile, "insert")
		require.NoError(t, err)
		require.NotNil(t, csvWriter)

		csvWriter.Flush()
		require.NoError(t, csvWriter.Error())

		// Verify file content
		content, err := os.ReadFile(tempFile.Name())
		require.NoError(t, err)
		require.Contains(t, string(content), "Type,Status,Id,Parent Id,Client Id,Name,Conversion Currency Code,Conversion Name,Conversion Time,Conversion Value,Microsoft Click Id,Hashed Email Address,Hashed Phone Number,External Attribution Credit,External Attribution Model")
		require.Contains(t, string(content), "Format Version,,,,,6.0,,,,,,,,,")
	})

	t.Run("update action template", func(t *testing.T) {
		t.Parallel()

		tempFile, err := os.CreateTemp("", "test_*.csv")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.Remove(tempFile.Name()))
		}()

		csvWriter, err := CreateActionFileTemplate(tempFile, "update")
		require.NoError(t, err)
		require.NotNil(t, csvWriter)

		csvWriter.Flush()
		require.NoError(t, csvWriter.Error())

		// Verify file content
		content, err := os.ReadFile(tempFile.Name())
		require.NoError(t, err)
		require.Contains(t, string(content), "Type,Adjustment Type,Client Id,Id,Name,Conversion Name,Conversion Time,Adjustment Value,Microsoft Click Id,Hashed Email Address,Hashed Phone Number,Adjusted Currency Code,Adjustment Time")
		require.Contains(t, string(content), "Format Version,,,,6.0,,,,,,,,")
	})

	t.Run("delete action template", func(t *testing.T) {
		t.Parallel()

		tempFile, err := os.CreateTemp("", "test_*.csv")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.Remove(tempFile.Name()))
		}()

		csvWriter, err := CreateActionFileTemplate(tempFile, "delete")
		require.NoError(t, err)
		require.NotNil(t, csvWriter)

		csvWriter.Flush()
		require.NoError(t, csvWriter.Error())

		// Verify file content - delete template has Format Version,,,,6.0,,,,,,
		content, err := os.ReadFile(tempFile.Name())
		require.NoError(t, err)
		require.Contains(t, string(content), "Type,Adjustment Type,Client Id,Id,Name,Conversion Name,Conversion Time,Microsoft Click Id,Hashed Email Address,Hashed Phone Number,Adjustment Time")
		require.Contains(t, string(content), "Format Version,,,,6.0,,,,,,")
	})

	t.Run("unknown action type", func(t *testing.T) {
		t.Parallel()

		tempFile, err := os.CreateTemp("", "test_*.csv")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, os.Remove(tempFile.Name()))
		}()

		csvWriter, err := CreateActionFileTemplate(tempFile, "unknown")
		require.NoError(t, err)
		require.NotNil(t, csvWriter)

		csvWriter.Flush()
		require.NoError(t, csvWriter.Error())

		// Should default to delete template
		content, err := os.ReadFile(tempFile.Name())
		require.NoError(t, err)
		require.Contains(t, string(content), "Type,Adjustment Type,Client Id,Id,Name,Conversion Name,Conversion Time,Microsoft Click Id,Hashed Email Address,Hashed Phone Number,Adjustment Time")
	})
}

func TestCreateActionFile(t *testing.T) {
	t.Parallel()

	t.Run("successful file creation", func(t *testing.T) {
		t.Parallel()

		actionFile, err := createActionFile("insert")
		require.NoError(t, err)
		require.NotNil(t, actionFile)

		// Clean up - only remove files if they exist
		defer func() {
			if actionFile.CSVFilePath != "" {
				if _, err := os.Stat(actionFile.CSVFilePath); err == nil {
					require.NoError(t, os.Remove(actionFile.CSVFilePath))
				}
			}
			if actionFile.ZipFilePath != "" {
				if _, err := os.Stat(actionFile.ZipFilePath); err == nil {
					require.NoError(t, os.Remove(actionFile.ZipFilePath))
				}
			}
		}()

		require.Equal(t, "insert", actionFile.Action)
		require.NotEmpty(t, actionFile.CSVFilePath)
		require.NotEmpty(t, actionFile.ZipFilePath)
		require.NotNil(t, actionFile.CSVWriter)

		// Verify CSV file exists
		_, err = os.Stat(actionFile.CSVFilePath)
		require.NoError(t, err)

		// Note: Zip file is only created when convertCsvToZip is called
		// So we don't check for its existence here
	})
}

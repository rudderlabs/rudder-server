package filehandler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-server/regulation-worker/internal/model"
)

func TestRemoveIdentityRecordsByUserId(t *testing.T) {
	handler := NewParquetLocalFileHandler()
	handler.records = []interface{}{
		struct {
			User_id          *string
			Context_app_name *string
		}{
			User_id:          getStringPtr("my-user-id"),
			Context_app_name: getStringPtr("my-app-name"),
		},
		struct {
			User_id          *string
			Context_app_name *string
		}{
			User_id:          getStringPtr("my-another-user-id"),
			Context_app_name: getStringPtr("my-app-name"),
		},
	}

	err := handler.RemoveIdentity(context.TODO(), []model.User{{ID: "my-user-id"}})
	require.Nil(t, err)
	require.Equal(t, len(handler.records), 1)
}

func TestIdentityRemovalProcessRunsSuccessfully(t *testing.T) {
	ctx := context.TODO()
	handler := NewParquetLocalFileHandler()

	inputFile := "testdata/test_tracks.parquet"
	outputFile := "testdata/actual_test_tracks_filtered.parquet"

	err := handler.Read(ctx, inputFile)
	require.Nil(t, err)

	err = handler.RemoveIdentity(ctx, []model.User{{ID: "68108b4d-245f-4aba-b240-8fb107c9d7b2"}})
	require.Nil(t, err)

	err = handler.Write(ctx, outputFile)
	require.Nil(t, err)

	// make sure the file gets removed
	defer cleanupFile(outputFile)

	same, err := sameFiles("testdata/expected_test_tracks_filtered.parquet", outputFile)
	require.Nil(t, err)
	require.True(t, same)
}

func cleanupFile(name string) {
	os.Remove(name)
}

func sameFiles(file1, file2 string) (bool, error) {
	f1, err := os.Open(file1)
	if err != nil {
		return false, fmt.Errorf("unable to open file: %s, err: %w", file1, err)
	}

	defer func() {
		_ = f1.Close()
	}()

	f2, err := os.Open(file2)
	if err != nil {
		return false, fmt.Errorf("unable to open file: %s, err: %w", file2, err)
	}

	defer func() {
		_ = f2.Close()
	}()

	byt1, err := io.ReadAll(f1)
	if err != nil {
		return false, fmt.Errorf("unable to read contents of file: %s, err: %w", file1, err)
	}

	byt2, err := io.ReadAll(f2)
	if err != nil {
		return false, fmt.Errorf("unable to read contents of file: %s, err: %w", file2, err)
	}

	if err := f1.Close(); err != nil {
		return false, err
	}

	if err := f2.Close(); err != nil {
		return false, err
	}

	return bytes.Equal(byt1, byt2), nil
}

func getStringPtr(val string) *string {
	return &val
}

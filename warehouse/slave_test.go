package warehouse

import (
	"context"
	"testing"

	"github.com/rudderlabs/rudder-server/services/filemanager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/stretchr/testify/require"
)

func TestPickupStagingFileBucket(t *testing.T) {
	inputs := []struct {
		job      *PayloadT
		expected bool
	}{
		{
			job:      &PayloadT{},
			expected: false,
		},
		{
			job: &PayloadT{
				StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
				DestinationRevisionID:        "1liYatjkkCEVkEMYUmSWOE9eZ4n",
			},
			expected: false,
		},
		{
			job: &PayloadT{
				StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
				DestinationRevisionID:        "2liYatjkkCEVkEMYUmSWOE9eZ4n",
			},
			expected: false,
		},
		{
			job: &PayloadT{
				StagingDestinationRevisionID: "1liYatjkkCEVkEMYUmSWOE9eZ4n",
				DestinationRevisionID:        "2liYatjkkCEVkEMYUmSWOE9eZ4n",
				StagingDestinationConfig:     map[string]string{},
			},
			expected: true,
		},
	}
	for _, input := range inputs {
		got := PickupStagingConfiguration(input.job)
		require.Equal(t, got, input.expected)
	}
}

type TestFileManagerFactory struct {
	filemanager.FileManagerFactory
}

type TestFileManager struct {
	filemanager.FileManager
	stagingFileLocation string
}

func (factory *TestFileManagerFactory) New(settings *filemanager.SettingsT) (filemanager.FileManager, error) {
	return &TestFileManager{
		stagingFileLocation: "slave/staging_file.json",
	}, nil
}

func TestStagingFileCreatesLoadObjects(t *testing.T) {

	// TODO: Write mainly implementation for the filemanager
	// which will be only looking for downloading a specific implementation for the staging file.
	job := PayloadT{
		FileManagerFactory: &TestFileManagerFactory{},
	}

	jobRun := JobRunT{
		job:            &job,
		stagingFileDIR: "slave/download",
		whIdentifier:   warehouseutils.GetWarehouseIdentifier(job.DestinationType, job.SourceID, job.DestinationID),
	}

	_, err := processStagingFile(context.TODO(), &job, &jobRun, 0)
	if err != nil {
		t.Errorf("process staging file should be successful, err: %s", err.Error())
		return
	}
}

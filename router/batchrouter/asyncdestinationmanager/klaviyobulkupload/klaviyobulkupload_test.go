package klaviyobulkupload_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"go.uber.org/mock/gomock"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	mocks "github.com/rudderlabs/rudder-server/mocks/router/klaviyobulkupload"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/klaviyobulkupload"
)

var destination = &backendconfig.DestinationT{
	ID:   "1",
	Name: "KLAVIYO_BULK_UPLOAD",
	DestinationDefinition: backendconfig.DestinationDefinitionT{
		Name: "KLAVIYO_BULK_UPLOAD",
	},
	Config: map[string]interface{}{
		"privateApiKey": "1234",
	},
	Enabled:     true,
	WorkspaceID: "1",
}

func TestNewManagerSuccess(t *testing.T) {
	manager, err := klaviyobulkupload.NewManager(logger.NOP, stats.NOP, destination)
	assert.NoError(t, err)
	assert.NotNil(t, manager)
	assert.Equal(t, "KLAVIYO_BULK_UPLOAD", destination.Name)
}

func TestUpload(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockUploader := mocks.NewMockUploader(ctrl)

	expectedOutput := common.AsyncUploadOutput{
		ImportingJobIDs: []int64{1, 2, 3},
	}

	mockUploader.EXPECT().Upload(gomock.Any()).Return(expectedOutput).Times(1)

	output := mockUploader.Upload(&common.AsyncDestinationStruct{
		ImportingJobIDs: []int64{1, 2, 3},
	})

	if !reflect.DeepEqual(output, expectedOutput) {
		t.Errorf("Expected %v but got %v", expectedOutput, output)
	}
}

// File is successfully opened and read line by line
func TestFileReadSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockUploader := mocks.NewMockUploader(ctrl)

	destination := &backendconfig.DestinationT{
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: "Klaviyo",
		},
		Config: map[string]interface{}{
			"listId":        "123",
			"privateApiKey": "test-api-key",
		},
		ID: "dest-123",
	}
	asyncDestStruct := &common.AsyncDestinationStruct{
		Destination:     destination,
		FileName:        "testfile.txt",
		ImportingJobIDs: []int64{1, 2, 3},
	}

	expectedOutput := common.AsyncUploadOutput{
		ImportingJobIDs: []int64{1, 2, 3},
	}

	mockUploader.EXPECT().Upload(asyncDestStruct).Return(expectedOutput).Times(1)

	output := mockUploader.Upload(asyncDestStruct)

	if !reflect.DeepEqual(output, expectedOutput) {
		t.Errorf("Expected %v but got %v", expectedOutput, output)
	}
}

func TestPoll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockUploader := mocks.NewMockPoller(ctrl)

	pollInput := common.AsyncPoll{
		ImportId: "123",
	}

	expectedOutput := common.PollStatusResponse{
		Complete:   true,
		InProgress: false,
		StatusCode: 200,
		HasFailed:  false,
		HasWarning: false,
	}

	mockUploader.EXPECT().Poll(pollInput).Return(expectedOutput).Times(1)

	output := mockUploader.Poll(pollInput)

	if !reflect.DeepEqual(output, expectedOutput) {
		t.Errorf("Expected %v but got %v", expectedOutput, output)
	}
}

func TestGetUploadStats(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockUploader := mocks.NewMockUploadStats(ctrl)

	importedJobSlice := []int64{1, 2, 3, 4, 5, 6}

	jobs := make([]*jobsdb.JobT, len(importedJobSlice))
	for i := range importedJobSlice {
		job := jobsdb.JobT{}
		jobs[i] = &job
	}

	statsInput := common.GetUploadStatsInput{
		ImportingList: jobs,
	}

	expectedOutput := common.GetUploadStatsResponse{
		StatusCode: 200,
		Metadata: common.EventStatMeta{
			FailedKeys:    []int64{1, 2, 3},
			SucceededKeys: []int64{4, 5, 6},
		},
	}

	mockUploader.EXPECT().GetUploadStats(statsInput).Return(expectedOutput).Times(1)

	output := mockUploader.GetUploadStats(statsInput)

	if !reflect.DeepEqual(output, expectedOutput) {
		t.Errorf("Expected %v but got %v", expectedOutput, output)
	}
}

func TestExtractProfileValidInput(t *testing.T) {
	kbu := klaviyobulkupload.KlaviyoBulkUploader{}

	inputPayloadJSON := `{"message":{"body":{"FORM":{},"JSON":{"data":{"attributes":{"profiles":{"data":[{"attributes":{"anonymous_id":111222334,"email":"qwe122@mail.com","first_name":"Testqwe0122","jobIdentifier":"111222334:1","last_name":"user0122","location":{"city":"delhi","country":"India","ip":"213.5.6.41"},"phone_number":"+919912000123"},"id":"111222334","type":"profile"}]}},"relationships":{"lists":{"data":[{"id":"UKth4J","type":"list"}]}},"type":"profile-bulk-import-job"}},"JSON_ARRAY":{},"XML":{}},"endpoint":"","files":{},"headers":{},"method":"POST","params":{},"type":"REST","userId":"","version":"1"},"metadata":{"job_id":1}}`
	var inputPayload klaviyobulkupload.Input
	err := json.Unmarshal([]byte(inputPayloadJSON), &inputPayload)
	if err != nil {
		t.Errorf("json.Unmarshal failed: %v", err)
	}
	expectedProfile := `{"attributes":{"email":"qwe122@mail.com","phone_number":"+919912000123","first_name":"Testqwe0122","last_name":"user0122","location":{"city":"delhi","country":"India","ip":"213.5.6.41"}},"id":"111222334","type":"profile"}`
	result := kbu.ExtractProfile(inputPayload)
	profileJson, _ := json.Marshal(result)
	assert.JSONEq(t, expectedProfile, string(profileJson))
}

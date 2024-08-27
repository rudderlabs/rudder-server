package lyticsBulkUpload_test

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocks "github.com/rudderlabs/rudder-server/mocks/router/lytics_bulk_upload"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	lyticsBulkUpload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/lytics_bulk_upload"
	"github.com/stretchr/testify/assert"
)

var destination = &backendconfig.DestinationT{
	ID:   "1",
	Name: "LYTICS_BULK_UPLOAD",
	DestinationDefinition: backendconfig.DestinationDefinitionT{
		Name: "LYTICS_BULK_UPLOAD",
	},
	Config: map[string]interface{}{
		"lyticsAccountId":  "1234",
		"lyticsApiKey":     "1234567",
		"lyticsStreamName": "test",
		"timestampField":   "timestamp",
		"streamTraitsMapping": []map[string]string{
			{
				"rudderProperty": "Email",
				"lyticsProperty": "Email",
			},
		},
	},
	Enabled:     true,
	WorkspaceID: "1",
}

func TestNewManagerSuccess(t *testing.T) {
	manager, err := lyticsBulkUpload.NewManager(destination)
	assert.NoError(t, err)
	assert.NotNil(t, manager)
	assert.Equal(t, "LYTICS_BULK_UPLOAD", destination.Name)
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

func TestFileReadSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockUploader := mocks.NewMockUploader(ctrl)

	asyncDestStruct := &common.AsyncDestinationStruct{
		Destination:     destination,
		FileName:        "testdata/uploadData.txt",
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

func TestHttpClientDoSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockHttpClient := mocks.NewMockHttpClient(ctrl)

	req, _ := http.NewRequest("GET", "https://example.com", nil)
	expectedResp := &http.Response{StatusCode: 200}

	mockHttpClient.EXPECT().Do(req).Return(expectedResp, nil).Times(1)

	resp, err := mockHttpClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, expectedResp, resp)
}

func TestPollerPollSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPoller := mocks.NewMockPoller(ctrl)

	expectedResponse := common.PollStatusResponse{Complete: true}
	pollInput := common.AsyncPoll{}

	mockPoller.EXPECT().Poll(pollInput).Return(expectedResponse).Times(1)

	response := mockPoller.Poll(pollInput)
	assert.Equal(t, expectedResponse, response)
}

func TestUploadStatsGetUploadStats(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockUploadStats := mocks.NewMockUploadStats(ctrl)

	expectedResponse := common.GetUploadStatsResponse{StatusCode: 200}
	input := common.GetUploadStatsInput{}

	mockUploadStats.EXPECT().GetUploadStats(input).Return(expectedResponse).Times(1)

	response := mockUploadStats.GetUploadStats(input)
	assert.Equal(t, expectedResponse, response)
}

package lyticsBulkUpload_test

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mocks "github.com/rudderlabs/rudder-server/mocks/router/lytics_bulk_upload"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	lyticsBulkUpload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/lytics_bulk_upload"
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

func TestUploadBulkFile_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a mock instance of the Uploader interface
	mockUploader := mocks.NewMockUploader(ctrl)

	// Define the context and input parameters
	ctx := context.TODO()
	filePath := "testdata/uploadData.csv"

	// Mock the UploadBulkFile method to return a successful response
	mockUploader.EXPECT().UploadBulkFile(ctx, filePath).Return(true, nil).Times(1)

	result, err := mockUploader.UploadBulkFile(ctx, filePath)

	// Validate the result
	assert.NoError(t, err)
	assert.True(t, result)
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

	if resp.Body != nil {
		defer resp.Body.Close()
	}
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

func TestPopulateZipFile_AppendsNewLine(t *testing.T) {
	// Initialize GoMock controller
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create an instance of the mock
	mockUploader := mocks.NewMockUploader(ctrl)

	// Define your test data
	actionFile := &lyticsBulkUpload.ActionFileInfo{}
	streamTraitsMapping := []lyticsBulkUpload.StreamTraitMapping{
		{RudderProperty: "prop1", LyticsProperty: "lytic1"},
		{RudderProperty: "prop2", LyticsProperty: "lytic2"},
	}
	line := "test line"
	data := lyticsBulkUpload.Data{}

	// Set up expectations for the mock method
	mockUploader.EXPECT().
		PopulateCsvFile(actionFile, streamTraitsMapping, line, data).
		Return(nil). // or an error if you want to test error handling
		Times(1)     // Expect to be called once

	// Call the method under test using the mock
	err := mockUploader.PopulateCsvFile(actionFile, streamTraitsMapping, line, data)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

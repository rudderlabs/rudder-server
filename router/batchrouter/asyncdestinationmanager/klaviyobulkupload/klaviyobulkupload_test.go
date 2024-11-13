package klaviyobulkupload_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/klaviyobulkupload"
)

var currentDir, _ = os.Getwd()

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

func TestExtractProfileValidInput(t *testing.T) {
	kbu := klaviyobulkupload.KlaviyoBulkUploader{}

	dataPayloadJSON := `{
		"attributes": {
			"profiles": {
				"data": [
					{
						"attributes": {
							"anonymous_id": 111222334,
							"email": "qwe122@mail.com",
							"first_name": "Testqwe0122",
							"jobIdentifier": "111222334:1",
							"last_name": "user0122",
							"location": {
								"city": "delhi",
								"country": "India",
								"ip": "213.5.6.41"
							},
							"phone_number": "+919912000123"
						},
						"id": "111222334",
						"type": "profile"
					}
				]
			}
		},
		"relationships": {
			"lists": {
				"data": [
					{
						"id": "UKth4J",
						"type": "list"
					}
				]
			}
		},
		"type": "profile-bulk-import-job"
	}`
	var data klaviyobulkupload.Data
	err := json.Unmarshal([]byte(dataPayloadJSON), &data)
	if err != nil {
		t.Errorf("json.Unmarshal failed: %v", err)
	}
	expectedProfile := `{"attributes":{"email":"qwe122@mail.com","phone_number":"+919912000123","first_name":"Testqwe0122","last_name":"user0122","location":{"city":"delhi","country":"India","ip":"213.5.6.41"}},"id":"111222334","type":"profile"}`
	result := kbu.ExtractProfile(data)
	profileJson, _ := json.Marshal(result)
	assert.JSONEq(t, expectedProfile, string(profileJson))
}

func TestUploadIntegration(t *testing.T) {
	kbu, err := klaviyobulkupload.NewManager(logger.NOP, stats.NOP, destination)
	assert.NoError(t, err)
	assert.NotNil(t, kbu)

	asyncDestStruct := &common.AsyncDestinationStruct{
		Destination:     destination,
		FileName:        filepath.Join(currentDir, "testdata/uploadData.jsonl"),
		ImportingJobIDs: []int64{1, 2, 3},
	}

	output := kbu.Upload(asyncDestStruct)
	assert.NotNil(t, output)
	assert.Equal(t, destination.ID, output.DestinationID)
	assert.Empty(t, output.FailedJobIDs)
	assert.Empty(t, output.AbortJobIDs)
	assert.Empty(t, output.AbortReason)
	assert.NotEmpty(t, output.ImportingJobIDs)
}

package cloudfunctions

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_googlecloudfunction "github.com/rudderlabs/rudder-server/mocks/services/streammanager/googlecloudfunction"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

// var (
// 	sampleDeliveryStreamName = "sampleDeliveryStream"
// 	sampleMessage            = "sample respMsg"
// )

func TestNewProducer(t *testing.T) {
	destinationConfig := map[string]interface{}{
		"FunctionEnvironment":    "gen1",
		"GoogleCloudFunctionUrl": "https://us-location-project-name.cloudfunctions.net/function-x",
		"Credentials":            "crdentials",
		"RequireAuthentication":  false,
	}
	destination := backendconfig.DestinationT{
		Config:      destinationConfig,
		WorkspaceID: "sampleWorkspaceID",
	}
	timeOut := 10 * time.Second
	producer, err := NewProducer(&destination, common.Opts{Timeout: timeOut})
	assert.Nil(t, err)
	assert.NotNil(t, producer)
	assert.NotNil(t, producer.client, producer.opts, producer.config)
}

func TestProduceWithInvalidData(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_googlecloudfunction.NewMockGoogleCloudFunctionClient(ctrl)
	producer := &GoogleCloudFunctionProducer{client: mockClient, config: &Config{FunctionEnvironment: "gen1", FunctionName: "sample-functionname"}}

	// Invalid Payload
	sampleEventJson := []byte("invalid json")
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[FireHose] error :: message from payload not found", respMsg)

	// Empty Payload
	sampleEventJson = []byte("{}")
	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[FireHose] error :: message from payload not found", respMsg)
}

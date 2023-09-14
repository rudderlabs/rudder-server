package cloudfunctions

import (
	"net/http"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"google.golang.org/api/cloudfunctions/v1"
	"google.golang.org/api/googleapi"

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
	conf := &Config{FunctionName: "sample-functionname", FunctionEnvironment: "gen1"}
	producer := &GoogleCloudFunctionProducer{client: mockClient, config: conf}

	// Invalid Payload
	sampleEventJson := []byte("invalid json")
	requestPayload := &cloudfunctions.CallFunctionRequest{
		Data: "0", // TODO: 'invalid json is producing this when we do parsedJSON.String()'
	}
	mockClient.
		EXPECT().
		InvokeGen1Function(conf.FunctionName, requestPayload).
		Return(&cloudfunctions.CallFunctionResponse{
			Error: "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n<title>Error</title>\n</head>\n<body>\n<pre>Bad Request</pre>\n</body>\n</html>\n",
			ServerResponse: googleapi.ServerResponse{
				HTTPStatusCode: http.StatusOK,
			},
		}, nil).
		MaxTimes(1)
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[FireHose] error :: message from payload not found", respMsg)

	// Empty Payload
	sampleEventJson = []byte("{}")
	requestPayload = &cloudfunctions.CallFunctionRequest{
		Data: "{}",
	}
	mockClient.
		EXPECT().
		InvokeGen1Function(conf.FunctionName, requestPayload).
		Return(&cloudfunctions.CallFunctionResponse{
			Error: "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n<title>Error</title>\n</head>\n<body>\n<pre>Bad Request</pre>\n</body>\n</html>\n",
			ServerResponse: googleapi.ServerResponse{
				HTTPStatusCode: http.StatusOK,
			},
		}, nil).MaxTimes(1)

	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, requestPayload)
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[FireHose] error :: message from payload not found", respMsg)
}

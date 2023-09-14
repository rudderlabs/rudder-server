package cloudfunctions

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"golang.org/x/oauth2"
	"google.golang.org/api/cloudfunctions/v1"
	"google.golang.org/api/googleapi"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_googlecloudfunction "github.com/rudderlabs/rudder-server/mocks/services/streammanager/googlecloudfunction"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

var (
	validData = "{\"type\": \"track\", \"event\": \"checkout started\"}"
	err       = "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n<title>Error</title>\n</head>\n<body>\n<pre>Bad Request</pre>\n</body>\n</html>\n"
)

func TestNewProducerForGen1(t *testing.T) {
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

func TestNewProducerForGen2(t *testing.T) {
	destinationConfig := map[string]interface{}{
		"FunctionEnvironment":    "gen2",
		"GoogleCloudFunctionUrl": "sampleFunctionUrl",
		"Credentials":            "crdentials",
		"RequireAuthentication":  false,
	}
	destination := backendconfig.DestinationT{
		Config: destinationConfig,
	}
	timeOut := 10 * time.Second
	producer, err := NewProducer(&destination, common.Opts{Timeout: timeOut})
	assert.Nil(t, err)
	assert.NotNil(t, producer)
}

func TestNewProduceForGen2WithInvalidData(t *testing.T) {
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Bad Request", http.StatusBadRequest)
	}))

	ctrl := gomock.NewController(t)
	mockClient := mock_googlecloudfunction.NewMockGoogleCloudFunctionClient(ctrl)
	conf := &Config{
		FunctionName:          "sample-functionname",
		FunctionEnvironment:   "gen2",
		RequireAuthentication: false,
		FunctionUrl:           testSrv.URL,
	}
	producer := &GoogleCloudFunctionProducer{client: mockClient, config: conf}
	statusCode, responseStatus, responseMessage := producer.Produce([]byte("invalid_json"), map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", responseStatus)
	assert.Contains(t, responseMessage, "Bad Request")
}

func TestNewProduceForGen2WithoutAuthenticationAndValidData(t *testing.T) {
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(validData))
		if err != nil {
			panic(err)
		}
	}))

	ctrl := gomock.NewController(t)
	mockClient := mock_googlecloudfunction.NewMockGoogleCloudFunctionClient(ctrl)
	conf := &Config{
		FunctionEnvironment:   "gen2",
		RequireAuthentication: false,
		FunctionUrl:           testSrv.URL,
	}
	producer := &GoogleCloudFunctionProducer{client: mockClient, config: conf}
	statusCode, responseStatus, responseMessage := producer.Produce(
		[]byte(validData),
		map[string]string{})
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", responseStatus)
	assert.Contains(t, responseMessage, "Message Payload inserted with messageId")
}

func TestNewProduceForGen2WithAuthenticationAndValidData(t *testing.T) {
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer someAccessToken" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
		}
		_, err := w.Write([]byte("sample respMsg"))
		if err != nil {
			panic(err)
		}
	}))

	ctrl := gomock.NewController(t)
	mockClient := mock_googlecloudfunction.NewMockGoogleCloudFunctionClient(ctrl)
	conf := &Config{
		FunctionEnvironment:   "gen2",
		RequireAuthentication: true,
		FunctionUrl:           testSrv.URL,
	}
	producer := &GoogleCloudFunctionProducer{client: mockClient, config: conf}
	mockClient.EXPECT().
		GetToken(gomock.Any(), testSrv.URL, gomock.Any()).
		Return(&oauth2.Token{AccessToken: "someAccessToken"}, nil).MaxTimes(1)
	statusCode, responseStatus, responseMessage := producer.Produce(
		[]byte(validData),
		map[string]string{})
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", responseStatus)
	assert.Contains(t, responseMessage, "Message Payload inserted with messageId")
}

func TestProduceWithInvalidAndValidData(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_googlecloudfunction.NewMockGoogleCloudFunctionClient(ctrl)
	conf := &Config{FunctionName: "sample-functionname", FunctionEnvironment: "gen1"}
	producer := &GoogleCloudFunctionProducer{client: mockClient, config: conf}

	// Invalid Payload
	sampleEventJson := []byte("invalid_json")
	requestPayload := &cloudfunctions.CallFunctionRequest{
		Data: "invalid_json",
	}
	mockClient.
		EXPECT().
		InvokeGen1Function(conf.FunctionName, requestPayload).
		Return(&cloudfunctions.CallFunctionResponse{
			Error: err,
			ServerResponse: googleapi.ServerResponse{
				HTTPStatusCode: http.StatusOK,
			},
		}, nil).
		MaxTimes(1)
	statusCode, statusMsg, respMsg := producer.Produce(sampleEventJson, map[string]string{})
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[GOOGLE_CLOUD_FUNCTION] error :: Function call was not executed (Not Modified)", respMsg)

	// Empty Payload
	sampleEventJson = []byte("{}")
	requestPayload = &cloudfunctions.CallFunctionRequest{
		Data: "{}",
	}
	mockClient.
		EXPECT().
		InvokeGen1Function(conf.FunctionName, requestPayload).
		Return(&cloudfunctions.CallFunctionResponse{
			Error: err,
			ServerResponse: googleapi.ServerResponse{
				HTTPStatusCode: http.StatusOK,
			},
		}, nil).MaxTimes(1)

	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, requestPayload)
	assert.Equal(t, 400, statusCode)
	assert.Equal(t, "Failure", statusMsg)
	assert.Equal(t, "[GOOGLE_CLOUD_FUNCTION] error :: Function call was not executed (Not Modified)", respMsg)

	// Valid Data
	sampleEventJson = []byte(validData)
	requestPayload = &cloudfunctions.CallFunctionRequest{
		Data: validData,
	}
	mockClient.
		EXPECT().
		InvokeGen1Function(conf.FunctionName, requestPayload).
		Return(&cloudfunctions.CallFunctionResponse{
			Error: "",
			ServerResponse: googleapi.ServerResponse{
				HTTPStatusCode: http.StatusOK,
			},
		}, nil).MaxTimes(1)

	statusCode, statusMsg, respMsg = producer.Produce(sampleEventJson, requestPayload)
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", statusMsg)
	assert.Equal(t, "[GoogleCloudFunction] :: Message Payload inserted with messageId :: ", respMsg)
}

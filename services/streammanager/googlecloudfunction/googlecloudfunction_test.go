package googlecloudfunction

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/mock/gomock"
	"golang.org/x/oauth2"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mock_googlecloudfunction "github.com/rudderlabs/rudder-server/mocks/services/streammanager/googlecloudfunction"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

var validData = "{\"type\": \"track\", \"event\": \"checkout started\"}"

func TestNewProducer(t *testing.T) {
	destinationConfig := map[string]interface{}{
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
	assert.NotNil(t, producer.client)
	assert.NotNil(t, producer.httpClient)
	assert.NotNil(t, producer.config)
	assert.Equal(t, destination.Config["GoogleCloudFunctionUrl"], producer.config.FunctionUrl)
	assert.Equal(t, destination.Config["Credentials"], producer.config.Credentials)
	assert.Equal(t, 55*time.Minute, producer.config.TokenTimeout)
}

func TestConfigShouldGenerateToken(t *testing.T) {
	config := &Config{
		RequireAuthentication: true,
		Token:                 &oauth2.Token{},
		TokenCreatedAt:        time.Now(),
		TokenTimeout:          1 * time.Minute,
	}
	assert.False(t, config.shouldGenerateToken())
	config.TokenCreatedAt = time.Now().Add(-2 * time.Minute)
	assert.True(t, config.shouldGenerateToken())
	config.RequireAuthentication = false
	assert.False(t, config.shouldGenerateToken())
}

func TestConfigGenerateToken(t *testing.T) {
	config := &Config{
		RequireAuthentication: true,
		Token:                 &oauth2.Token{},
		TokenCreatedAt:        time.Now(),
		TokenTimeout:          1 * time.Minute,
	}
	ctrl := gomock.NewController(t)
	mockClient := mock_googlecloudfunction.NewMockGoogleCloudFunctionClient(ctrl)
	mockClient.EXPECT().
		GetToken(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&oauth2.Token{}, nil)
	err := config.generateToken(context.Background(), mockClient)
	assert.Nil(t, err)
	mockClient.EXPECT().
		GetToken(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, errors.New("token is failed to generate"))
	err = config.generateToken(context.Background(), mockClient)
	assert.NotNil(t, err)
}

func TestNewProduceWithBadServer(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockClient := mock_googlecloudfunction.NewMockGoogleCloudFunctionClient(ctrl)
	conf := &Config{
		RequireAuthentication: false,
		FunctionUrl:           "http://rudderstack-non-existing-server:54321",
	}
	producer := &GoogleCloudFunctionProducer{client: mockClient, config: conf, httpClient: http.DefaultClient}
	statusCode, responseStatus, responseMessage := producer.Produce([]byte("invalid_json"), map[string]string{})
	assert.Equal(t, http.StatusBadRequest, statusCode)
	assert.Equal(t, "Failure", responseStatus)
	assert.Contains(t, responseMessage, "Function call was not executed")
}

func TestNewProduceWithInvalidData(t *testing.T) {
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Bad Request", http.StatusBadRequest)
	}))

	ctrl := gomock.NewController(t)
	mockClient := mock_googlecloudfunction.NewMockGoogleCloudFunctionClient(ctrl)
	conf := &Config{
		RequireAuthentication: false,
		FunctionUrl:           testSrv.URL,
	}
	producer := &GoogleCloudFunctionProducer{client: mockClient, config: conf, httpClient: http.DefaultClient}
	statusCode, responseStatus, responseMessage := producer.Produce([]byte("invalid_json"), map[string]string{})
	assert.Equal(t, http.StatusBadRequest, statusCode)
	assert.Equal(t, "Failure", responseStatus)
	assert.Contains(t, responseMessage, "Bad Request")
}

func TestNewProduceForWithoutAuthenticationAndValidData(t *testing.T) {
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(validData))
		if err != nil {
			panic(err)
		}
	}))

	ctrl := gomock.NewController(t)
	mockClient := mock_googlecloudfunction.NewMockGoogleCloudFunctionClient(ctrl)
	conf := &Config{
		RequireAuthentication: false,
		FunctionUrl:           testSrv.URL,
	}
	producer := &GoogleCloudFunctionProducer{client: mockClient, config: conf, httpClient: http.DefaultClient}
	statusCode, responseStatus, responseMessage := producer.Produce(
		[]byte(validData),
		map[string]string{})
	assert.Equal(t, http.StatusOK, statusCode)
	assert.Equal(t, "Success", responseStatus)
	assert.Contains(t, responseMessage, "Function call is executed")
}

func TestNewProduceForWithAuthenticationAndGetTokenFailed(t *testing.T) {
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
		RequireAuthentication: true,
		FunctionUrl:           testSrv.URL,
	}
	producer := &GoogleCloudFunctionProducer{client: mockClient, config: conf, httpClient: http.DefaultClient}
	mockClient.EXPECT().
		GetToken(gomock.Any(), testSrv.URL, gomock.Any()).
		Return(nil, errors.New("token is failed to generate")).MaxTimes(1)
	statusCode, responseStatus, responseMessage := producer.Produce(
		[]byte(validData),
		map[string]string{})
	assert.Equal(t, http.StatusUnauthorized, statusCode)
	assert.Equal(t, "Failure", responseStatus)
	assert.Contains(t, responseMessage, "Failed to receive token")
}

func TestNewProduceForWithAuthenticationAndValidData(t *testing.T) {
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
		RequireAuthentication: true,
		FunctionUrl:           testSrv.URL,
	}
	producer := &GoogleCloudFunctionProducer{client: mockClient, config: conf, httpClient: http.DefaultClient}
	mockClient.EXPECT().
		GetToken(gomock.Any(), testSrv.URL, gomock.Any()).
		Return(&oauth2.Token{AccessToken: "someAccessToken"}, nil).MaxTimes(1)
	statusCode, responseStatus, responseMessage := producer.Produce(
		[]byte(validData),
		map[string]string{})
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", responseStatus)
	assert.Contains(t, responseMessage, "Function call is executed")
}

func TestNewProduceForWithAuthenticationAndTokenTimeout(t *testing.T) {
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
		RequireAuthentication: true,
		FunctionUrl:           testSrv.URL,
		TokenTimeout:          1 * time.Millisecond,
	}
	producer := &GoogleCloudFunctionProducer{client: mockClient, config: conf, httpClient: http.DefaultClient}
	// Token will expire in 1 ms so it will call GetToken twice
	mockClient.EXPECT().
		GetToken(gomock.Any(), testSrv.URL, gomock.Any()).
		Return(&oauth2.Token{AccessToken: "someAccessToken"}, nil).Times(2)
	statusCode, responseStatus, responseMessage := producer.Produce(
		[]byte(validData),
		map[string]string{})
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", responseStatus)
	assert.Contains(t, responseMessage, "Function call is executed")
	time.Sleep(1 * time.Millisecond)
	statusCode, responseStatus, responseMessage = producer.Produce(
		[]byte(validData),
		map[string]string{})
	assert.Equal(t, 200, statusCode)
	assert.Equal(t, "Success", responseStatus)
	assert.Contains(t, responseMessage, "Function call is executed")
}

func TestNewProduceWhenRequestTimedout(t *testing.T) {
	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
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
		RequireAuthentication: true,
		FunctionUrl:           testSrv.URL,
	}
	producer := &GoogleCloudFunctionProducer{
		client:     mockClient,
		config:     conf,
		httpClient: &http.Client{Timeout: 1 * time.Millisecond},
	}
	mockClient.EXPECT().
		GetToken(gomock.Any(), testSrv.URL, gomock.Any()).
		Return(&oauth2.Token{AccessToken: "someAccessToken"}, nil).MaxTimes(1)
	statusCode, responseStatus, responseMessage := producer.Produce(
		[]byte(validData),
		map[string]string{})
	assert.Equal(t, http.StatusAccepted, statusCode)
	assert.Equal(t, "Success", responseStatus)
	assert.Contains(t, responseMessage, "Function is called")
}

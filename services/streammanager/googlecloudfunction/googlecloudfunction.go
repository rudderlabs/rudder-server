//go:generate mockgen -destination=../../../mocks/services/streammanager/googlecloudfunction/mock_googlecloudfunction.go -package mock_googlecloudfunction github.com/rudderlabs/rudder-server/services/streammanager/googlecloudfunction GoogleCloudFunctionClient

package cloudfunctions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/api/cloudfunctions/v1"
	"google.golang.org/api/idtoken"

	"google.golang.org/api/option"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

type Config struct {
	Credentials           string        `json:"credentials"`
	RequireAuthentication bool          `json:"requireAuthentication"`
	FunctionUrl           string        `json:"googleCloudFunctionUrl"`
	Token                 *oauth2.Token `json:"token"`
	TokenCreatedAt        time.Time     `json:"tokenCreatedAt"`
}

func (config *Config) shouldGenerateToken() bool {
	if !config.RequireAuthentication {
		return false
	}
	if config.Token == nil {
		return true
	}
	return time.Since(config.TokenCreatedAt) > 55*time.Minute
}

var pkgLogger logger.Logger

func Init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("GoogleCloudFunction")
}

func init() {
	Init()
}

type GoogleCloudFunctionProducer struct {
	client GoogleCloudFunctionClient
	opts   common.Opts
	config *Config
}

type GoogleCloudFunctionClient interface {
	GetToken(ctx context.Context, functionUrl string, opts ...option.ClientOption) (*oauth2.Token, error)
}

type GoogleCloudFunctionClientImpl struct {
	service *cloudfunctions.ProjectsLocationsFunctionsService
}

func (c *GoogleCloudFunctionClientImpl) GetToken(ctx context.Context, functionUrl string, opts ...option.ClientOption) (*oauth2.Token, error) {
	ts, err := idtoken.NewTokenSource(ctx, functionUrl, opts...)
	if err != nil {
		pkgLogger.Errorf("failed to create NewTokenSource: %w", err)
		return nil, err
	}

	// Get the ID token, to make an authenticated call to the target audience.
	return ts.Token()
}

func getConfig(config Config, client GoogleCloudFunctionClient) (*Config, error) {
	if config.RequireAuthentication {
		token, err := client.GetToken(context.Background(), config.FunctionUrl, option.WithCredentialsJSON([]byte(config.Credentials)))
		if err != nil {
			return nil, err
		}
		config.Token = token
		config.TokenCreatedAt = time.Now()
	}
	return &config, nil
}

// NewProducer creates a producer based on destination config
func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (*GoogleCloudFunctionProducer, error) {
	var config Config
	jsonConfig, err := json.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("[GoogleCloudFunction] Error while marshalling destination config: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return nil, fmt.Errorf("[GoogleCloudFunction] Error in GoogleCloudFunction while unmarshalling destination config: %w", err)
	}

	opts := []option.ClientOption{
		option.WithoutAuthentication(),
	}

	if config.RequireAuthentication {
		opts = []option.ClientOption{
			option.WithCredentialsJSON([]byte(config.Credentials)),
		}
	}

	service, err := generateService(opts...)
	// If err is not nil then return
	if err != nil {
		pkgLogger.Errorf("Error in generation of service: %w", err)
		return nil, err
	}

	client := &GoogleCloudFunctionClientImpl{service: service.Projects.Locations.Functions}
	destConfig, err := getConfig(config, client)
	if err != nil {
		pkgLogger.Errorf("Error in getting config: %w", err)
		return nil, err
	}

	return &GoogleCloudFunctionProducer{
		client: client,
		opts:   o,
		config: destConfig,
	}, err
}

func (producer *GoogleCloudFunctionProducer) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessage string) {
	destConfig := producer.config
	parsedJSON := gjson.ParseBytes(jsonData)

	return producer.invokeFunction(destConfig, parsedJSON)
}

func (producer *GoogleCloudFunctionProducer) invokeFunction(destConfig *Config, parsedJSON gjson.Result) (statusCode int, respStatus, responseMessage string) {
	ctx := context.Background()

	jsonBytes := []byte(parsedJSON.Raw)

	// Create a POST request
	req, err := http.NewRequest(http.MethodPost, destConfig.FunctionUrl, strings.NewReader(string(jsonBytes)))
	if err != nil {
		pkgLogger.Errorf("Failed to create httpRequest for Fn: %w", err)
		return http.StatusBadRequest, "Failure", fmt.Sprintf("[GoogleCloudFunction] Failed to create httpRequest for Fn: %s", err.Error())
	}

	// Set the appropriate headers
	req.Header.Set("Content-Type", "application/json")
	if destConfig.shouldGenerateToken() {
		token, err := producer.client.GetToken(ctx, destConfig.FunctionUrl, option.WithCredentialsJSON([]byte(destConfig.Credentials)))
		if err != nil {
			pkgLogger.Errorf("failed to receive token: %w", err)
			return http.StatusUnauthorized, "Failure", fmt.Sprintf("[GoogleCloudFunction] Failed to receive token: %s", err.Error())
		}
		destConfig.Token = token
		destConfig.TokenCreatedAt = time.Now()
	}
	if destConfig.RequireAuthentication && destConfig.Token != nil {
		req.Header.Set("Authorization", "Bearer "+destConfig.Token.AccessToken)
	}

	// Make the request using the client
	resp, err := http.DefaultClient.Do(req)
	var responseBody []byte
	defer resp.Body.Close()
	if err == nil {
		responseBody, err = io.ReadAll(resp.Body)
	}
	if err != nil {
		responseMessage = err.Error()
		statusCode = http.StatusBadRequest
		if errors.Is(err, context.DeadlineExceeded) {
			statusCode = http.StatusGatewayTimeout
		}
		respStatus = "Failure"
		responseMessage = "[GOOGLE_CLOUD_FUNCTION] error :: Function call was not executed " + responseMessage
		pkgLogger.Errorf("error while calling the function :: %v", err)
		return statusCode, respStatus, responseMessage
	}

	if resp.StatusCode == http.StatusOK {
		respStatus = "Success"
		responseMessage = "[GoogleCloudFunction] :: Function call is executed"
	} else {
		respStatus = "Failure"
		responseMessage = "[GOOGLE_CLOUD_FUNCTION] error :: Function call failed " + string(responseBody)
		pkgLogger.Error(responseMessage)
	}
	return resp.StatusCode, respStatus, responseMessage
}

// Initialize the Cloud Functions API client using service account credentials.
func generateService(opts ...option.ClientOption) (*cloudfunctions.Service, error) {
	ctx := context.Background()

	cloudFunctionService, err := cloudfunctions.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("[GoogleCloudFunction] error  :: Unable to create cloudfunction service :: %w", err)
	}
	return cloudFunctionService, err
}

func (*GoogleCloudFunctionProducer) Close() error {
	// no-op
	return nil
}

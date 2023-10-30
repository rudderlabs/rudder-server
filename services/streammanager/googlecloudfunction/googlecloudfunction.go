//go:generate mockgen -destination=../../../mocks/services/streammanager/googlecloudfunction/mock_googlecloudfunction.go -package mock_googlecloudfunction github.com/rudderlabs/rudder-server/services/streammanager/googlecloudfunction GoogleCloudFunctionClient

package googlecloudfunction

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"golang.org/x/oauth2"
	"google.golang.org/api/idtoken"

	"google.golang.org/api/option"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/httputil"
)

type Config struct {
	Credentials           string        `json:"credentials"`
	RequireAuthentication bool          `json:"requireAuthentication"`
	FunctionUrl           string        `json:"googleCloudFunctionUrl"`
	Token                 *oauth2.Token `json:"token"`
	TokenCreatedAt        time.Time     `json:"tokenCreatedAt"`
	TokenTimeout          time.Duration `json:"tokenTimeout"`
}

func (config *Config) shouldGenerateToken() bool {
	if !config.RequireAuthentication {
		return false
	}
	if config.Token == nil {
		return true
	}
	return time.Since(config.TokenCreatedAt) > config.TokenTimeout
}

func (config *Config) generateToken(ctx context.Context, client GoogleCloudFunctionClient) error {
	token, err := client.GetToken(ctx, config.FunctionUrl, option.WithCredentialsJSON([]byte(config.Credentials)))
	if err != nil {
		return err
	}
	config.Token = token
	config.TokenCreatedAt = time.Now()
	return nil
}

var pkgLogger logger.Logger

func Init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("GoogleCloudFunction")
}

func init() {
	Init()
}

type GoogleCloudFunctionProducer struct {
	client     GoogleCloudFunctionClient
	config     *Config
	httpClient *http.Client
}

type GoogleCloudFunctionClient interface {
	GetToken(ctx context.Context, functionUrl string, opts ...option.ClientOption) (*oauth2.Token, error)
}

type GoogleCloudFunctionClientImpl struct{}

func (c *GoogleCloudFunctionClientImpl) GetToken(ctx context.Context, functionUrl string, opts ...option.ClientOption) (*oauth2.Token, error) {
	ts, err := idtoken.NewTokenSource(ctx, functionUrl, opts...)
	if err != nil {
		pkgLogger.Errorf("failed to create NewTokenSource: %w", err)
		return nil, err
	}

	// Get the ID token, to make an authenticated call to the target audience.
	return ts.Token()
}

func getFunctionConfig(fnConfig Config) *Config {
	fnConfig.TokenTimeout = config.GetDurationVar(55, time.Minute, "google.cloudfunction.token.timeout")
	return &fnConfig
}

// NewProducer creates a producer based on destination config
func NewProducer(destination *backendconfig.DestinationT, _ common.Opts) (*GoogleCloudFunctionProducer, error) {
	var fnConfig Config
	jsonConfig, err := json.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("[GoogleCloudFunction] Error while marshalling destination config: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &fnConfig)
	if err != nil {
		return nil, fmt.Errorf("[GoogleCloudFunction] Error in GoogleCloudFunction while unmarshalling destination config: %w", err)
	}

	client := &GoogleCloudFunctionClientImpl{}
	destConfig := getFunctionConfig(fnConfig)

	return &GoogleCloudFunctionProducer{
		httpClient: &http.Client{Timeout: 1 * time.Second},
		client:     client,
		config:     destConfig,
	}, err
}

func (producer *GoogleCloudFunctionProducer) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessage string) {
	// Create a POST request
	req, err := http.NewRequest(http.MethodPost, producer.config.FunctionUrl, bytes.NewReader(jsonData))
	if err != nil {
		pkgLogger.Errorf("Failed to create httpRequest for Fn: %w", err)
		return http.StatusBadRequest, "Failure", fmt.Sprintf("[GoogleCloudFunction] Failed to create httpRequest for Fn: %s", err.Error())
	}

	// Set the appropriate headers
	req.Header.Set("Content-Type", "application/json")
	if producer.config.shouldGenerateToken() {
		err := producer.config.generateToken(context.Background(), producer.client)
		if err != nil {
			pkgLogger.Errorf("failed to receive token: %w", err)
			return http.StatusUnauthorized, "Failure", fmt.Sprintf("[GoogleCloudFunction] Failed to receive token: %s", err.Error())
		}
	}
	if producer.config.RequireAuthentication && producer.config.Token != nil {
		req.Header.Set("Authorization", "Bearer "+producer.config.Token.AccessToken)
	}

	// Make the request using the client
	resp, err := producer.httpClient.Do(req)

	var responseBody []byte
	if err == nil {
		defer func() { httputil.CloseResponse(resp) }()
		responseBody, err = io.ReadAll(resp.Body)
	}

	if err != nil {
		if os.IsTimeout(err) {
			return http.StatusAccepted, "Success", "[GoogleCloudFunction] :: Function is called"
		}
		responseMessage = err.Error()
		respStatus = "Failure"
		responseMessage = "[GOOGLE_CLOUD_FUNCTION] error :: Function call was not executed " + responseMessage
		pkgLogger.Errorf("error while calling the function :: %v", err)
		return http.StatusBadRequest, respStatus, responseMessage
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

func (producer *GoogleCloudFunctionProducer) Close() error {
	producer.httpClient.CloseIdleConnections()
	return nil
}

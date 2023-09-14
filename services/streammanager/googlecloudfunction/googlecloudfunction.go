//go:generate mockgen -destination=../../../mocks/services/streammanager/googlecloudfunction/mock_googlecloudfunction.go -package mock_googlecloudfunction github.com/rudderlabs/rudder-server/services/streammanager/googlecloudfunction GoogleCloudFunctionClient

package cloudfunctions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/oauth2"
	"google.golang.org/api/cloudfunctions/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/idtoken"

	"google.golang.org/api/option"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

type Config struct {
	Credentials           string     `json:"credentials"`
	FunctionEnvironment   string     `json:"functionEnvironment"`
	RequireAuthentication bool       `json:"requireAuthentication"`
	FunctionUrl           string     `json:"googleCloudFunctionUrl"`
	FunctionName          string     `json:"functionName"`
	TestConfig            TestConfig `json:"testConfig"`
}

type TestConfig struct {
	Credentials  string `json:"crdentials"`
	FunctionName string `json:"functionName"`
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
	InvokeGen1Function(name string, callfunctionrequest *cloudfunctions.CallFunctionRequest) (*cloudfunctions.CallFunctionResponse, error)
	GetToken(ctx context.Context, functionUrl string, opts ...option.ClientOption) (*oauth2.Token, error)
}

type GoogleCloudFunctionClientImpl struct {
	service *cloudfunctions.ProjectsLocationsFunctionsService
}

func (c *GoogleCloudFunctionClientImpl) InvokeGen1Function(name string, callfunctionrequest *cloudfunctions.CallFunctionRequest) (*cloudfunctions.CallFunctionResponse, error) {
	call := c.service.Call(name, callfunctionrequest)
	return call.Do()
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

	functionName := getFunctionName(config.FunctionUrl)

	destConfig := &Config{config.Credentials, config.FunctionEnvironment, config.RequireAuthentication, config.FunctionUrl, functionName, config.TestConfig}

	return &GoogleCloudFunctionProducer{
		client: &GoogleCloudFunctionClientImpl{service: service.Projects.Locations.Functions},
		opts:   o,
		config: destConfig,
	}, err
}

func (producer *GoogleCloudFunctionProducer) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessage string) {
	destConfig := producer.config
	parsedJSON := gjson.ParseBytes(jsonData)

	if destConfig.FunctionEnvironment == "gen1" {
		return producer.invokeGen1Functions(destConfig.FunctionName, parsedJSON)
	}

	return producer.invokeGen2Functions(destConfig, parsedJSON)
}

func (producer *GoogleCloudFunctionProducer) invokeGen1Functions(functionName string, parsedJSON gjson.Result) (statusCode int, respStatus, responseMessage string) {
	if producer.client == nil {
		respStatus = "Failure"
		responseMessage = "[GoogleCloudFunction]:: Failed to initialize client"
		return 400, respStatus, responseMessage
	}

	requestPayload := &cloudfunctions.CallFunctionRequest{
		Data: parsedJSON.Raw,
	}

	// Make the HTTP request
	response, err := producer.client.InvokeGen1Function(functionName, requestPayload)
	if response.Error != "" {
		statCode := 400
		respStatus = "Failure"
		responseMessage = "[GOOGLE_CLOUD_FUNCTION] error :: Function call was not executed (Not Modified)"
		pkgLogger.Errorf("error while calling the Gen1 function :: %v", response.Error)
		return statCode, respStatus, responseMessage
	}
	if err != nil {
		statCode, serviceMessage := handleServiceError(err)
		respStatus = "Failure"
		responseMessage = "[GOOGLE_CLOUD_FUNCTION] error :: Function call was not executed (Not Modified :: " + serviceMessage
		pkgLogger.Errorf("error while calling the Gen1 function :: %v", err)
		return statCode, respStatus, responseMessage
	}

	respStatus = "Success"
	responseMessage = "[GoogleCloudFunction] :: Message Payload inserted with messageId :: " + parsedJSON.Get("id").String()
	return http.StatusOK, respStatus, responseMessage
}

func (producer *GoogleCloudFunctionProducer) invokeGen2Functions(destConfig *Config, parsedJSON gjson.Result) (statusCode int, respStatus, responseMessage string) {
	ctx := context.Background()

	jsonBytes := []byte(parsedJSON.Raw)

	// Create a POST request
	req, err := http.NewRequest(http.MethodPost, destConfig.FunctionUrl, strings.NewReader(string(jsonBytes)))
	if err != nil {
		pkgLogger.Errorf("Failed to create httpRequest for Gen2 Fn: %w", err)
		return http.StatusBadRequest, "Failure", fmt.Sprintf("[GoogleCloudFunction] Failed to create httpRequest for Gen2 Fn: %s", err.Error())
	}

	// Set the appropriate headers
	req.Header.Set("Content-Type", "application/json")
	if destConfig.RequireAuthentication {
		token, err := producer.client.GetToken(ctx, destConfig.FunctionUrl, option.WithCredentialsJSON([]byte(destConfig.Credentials)))
		if err != nil {
			pkgLogger.Errorf("failed to receive token: %w", err)
			return http.StatusInternalServerError, "Failure", fmt.Sprintf("[GoogleCloudFunction] Failed to receive token: %s", err.Error())
		}
		req.Header.Add("Authorization", "Bearer "+token.AccessToken)
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
		pkgLogger.Errorf("error while calling the Gen2 function :: %v", err)
		return statusCode, respStatus, responseMessage
	}
	
	if resp.StatusCode == http.StatusOK {
		respStatus = "Success"
		responseMessage = "[GoogleCloudFunction] :: Message Payload inserted with messageId :: " + parsedJSON.Get("id").String()
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

func getFunctionName(url string) (functionName string) {
	if !isValidCloudFunctionURL(url) {
		pkgLogger.Errorf("Invalid Function URL")
		return
	}

	// Define a regular expression pattern to match URLs between "https://" and dot (.)
	pattern := `https://(.*?)\.`

	// Compile the regular expression pattern
	regExp := regexp.MustCompile(pattern)

	// Find all matches in the input string
	matches := regExp.FindAllStringSubmatch(url, -1)

	// Extract the captured groups
	var resultArray []string
	for _, match := range matches {
		if len(match) > 1 {
			resultArray = append(resultArray, match[1])
		}
	}

	// Combine the first two elements
	splitValues := strings.Split(resultArray[0], "-")
	// Join the first two values with "-" and assign to region
	region := strings.Join(splitValues[:2], "-")

	// Join the rest of the values with "-" and assign to projectId
	projectId := strings.Join(splitValues[2:], "-")

	var gcFnName string

	index := strings.Index(url, "cloudfunctions.net/")

	pkgLogger.Debugf("Match found: %b", strconv.FormatBool(index != -1))
	if index != -1 {
		// Extract the substring after "cloudfunctions.net/"
		gcFnName = url[index+len("cloudfunctions.net/"):]
	}

	functionName = "projects/" + projectId + "/locations/" + region + "/functions/" + gcFnName

	return functionName
}

func isValidCloudFunctionURL(url string) bool {
	// Define a regular expression pattern to match a valid Google Cloud Function URL
	pattern := `^https:\/\/[a-z1-9-]+\.cloudfunctions\.net\/[a-zA-Z0-9_-]+$`

	// Compile the regular expression
	regex := regexp.MustCompile(pattern)

	// Use the regular expression to check if the URL matches the pattern
	return regex.MatchString(url)
}

// handleServiceError is created for fail safety, if in any case when err type is not googleapi.Error
// server should not crash with a type error.
func handleServiceError(err error) (statusCode int, responseMessage string) {
	statusCode = 500
	responseMessage = err.Error()

	if err != nil && errors.Is(err, context.DeadlineExceeded) {
		statusCode = 504
	}

	if reflect.TypeOf(err).String() == "*googleapi.Error" {
		serviceErr := err.(*googleapi.Error)
		statusCode = serviceErr.Code
		responseMessage = serviceErr.Message
	}
	return statusCode, responseMessage
}

func (*GoogleCloudFunctionProducer) Close() error {
	// no-op
	return nil
}

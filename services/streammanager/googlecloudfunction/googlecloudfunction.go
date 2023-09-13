package cloudfunctions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"

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
	Credentials            string     `json:"credentials"`
	FunctionEnvironment    string     `json:"functionEnvironment"`
	RequireAuthentication  bool       `json:"requireAuthentication"`
	GoogleCloudFunctionUrl string     `json:"googleCloudFunctionUrl"`
	FunctionName           string     `json:"functionName"`
	TestConfig             TestConfig `json:"testConfig"`
}

type TestConfig struct {
	Credentials  string `json:"crdentials"`
	FunctionName string `json:"functionName"`
}

type Client struct {
	service *cloudfunctions.Service
	opts    common.Opts
}

var pkgLogger logger.Logger

func Init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("GoogleCloudFunction")
}

func init() {
	Init()
}

type GoogleCloudFunctionProducer struct {
	client *Client
	config *Config
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

	functionName := getFunctionName(config.GoogleCloudFunctionUrl)

	client := &Client{service, o}
	destConfig := &Config{config.Credentials, config.FunctionEnvironment, config.RequireAuthentication, config.GoogleCloudFunctionUrl, functionName, config.TestConfig}

	return &GoogleCloudFunctionProducer{client, destConfig}, err
}

func (producer *GoogleCloudFunctionProducer) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessage string) {
	destConfig := producer.config
	parsedJSON := gjson.ParseBytes(jsonData)

	if destConfig.FunctionEnvironment == "gen1" {
		return invokeGen1Functions(producer.client, destConfig.FunctionName, parsedJSON)
	}

	return invokeGen2Functions(destConfig, parsedJSON)
}

func invokeGen1Functions(client *Client, functionName string, parsedJSON gjson.Result) (statusCode int, respStatus, responseMessage string) {
	if client == nil {
		respStatus = "Failure"
		responseMessage = "[GoogleCloudFunction]:: Failed to initialize client"
		return 400, respStatus, responseMessage
	}

	requestPayload := &cloudfunctions.CallFunctionRequest{
		Data: parsedJSON.String(),
	}

	// Make the HTTP request
	call := client.service.Projects.Locations.Functions.Call(functionName, requestPayload)

	response, err := call.Do()
	if err != nil {
		statCode, serviceMessage := handleServiceError(err)
		respStatus = "Failure"
		responseMessage = "[GOOGLE_CLOUD_FUNCTION] error :: Function call was not executed (Not Modified :: " + serviceMessage
		pkgLogger.Errorf("error while calling the Gen1 function :: %v", err)
		return statCode, respStatus, responseMessage
	}

	// ---------- To be Removed
	// Process the response (sample response handling here).
	if response != nil {
		fmt.Printf("Function call status code: %d\n", response.HTTPStatusCode)
		// Handle response content as needed.
	}
	fmt.Println("Request successful!")
	// -----------

	respStatus = "Success"
	responseMessage = "[GoogleCloudFunction] :: Message Payload inserted with messageId :: " + parsedJSON.Get("id").String()
	return http.StatusOK, respStatus, responseMessage
}

func invokeGen2Functions(destConfig *Config, parsedJSON gjson.Result) (statusCode int, respStatus, responseMessage string) {
	ctx := context.Background()

	jsonBytes := []byte(parsedJSON.String())

	// Create a POST request
	req, err := http.NewRequest(http.MethodPost, destConfig.GoogleCloudFunctionUrl, strings.NewReader(string(jsonBytes)))
	if err != nil {
		pkgLogger.Errorf("Failed to create httpRequest for Gen2 Fn: %w", err)
		return http.StatusBadRequest, "Failure", fmt.Sprintf("[GoogleCloudFunction] Failed to create httpRequest for Gen2 Fn: %s", err.Error())
	}

	// Set the appropriate headers
	req.Header.Set("Content-Type", "application/json")
	if destConfig.RequireAuthentication {
		ts, err := idtoken.NewTokenSource(ctx, destConfig.GoogleCloudFunctionUrl, option.WithCredentialsJSON([]byte(destConfig.Credentials)))
		if err != nil {
			pkgLogger.Errorf("failed to create NewTokenSource: %w", err)
			return http.StatusBadRequest, "Failure", fmt.Sprintf("[GoogleCloudFunction] Failed to create NewTokenSource: %s", err.Error())
		}

		// Get the ID token, to make an authenticated call to the target audience.
		token, err := ts.Token()
		if err != nil {
			pkgLogger.Errorf("failed to receive token: %w", err)
			return http.StatusInternalServerError, "Failure", fmt.Sprintf("[GoogleCloudFunction] Failed to receive token: %s", err.Error())
		}
		req.Header.Add("Authorization", "Bearer "+token.AccessToken)
	}

	// Make the request using the client
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		responseMessage = err.Error()
		statusCode = http.StatusBadRequest
		if errors.Is(err, context.DeadlineExceeded) {
			statusCode = http.StatusGatewayTimeout
		}
		respStatus = "Failure"
		responseMessage = "[GOOGLE_CLOUD_FUNCTION] error :: Function call was not executed (Not Modified :: " + responseMessage
		pkgLogger.Errorf("error while calling the Gen2 function :: %v", err)
		return statusCode, respStatus, responseMessage
	}
	defer resp.Body.Close()

	if resp.Status == "OK" {
		respStatus = "Success"
		responseMessage = "[GoogleCloudFunction] :: Message Payload inserted with messageId :: " + parsedJSON.Get("id").String()
	}
	return http.StatusOK, respStatus, responseMessage
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

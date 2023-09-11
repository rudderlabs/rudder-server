package cloudfunctions

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"

	"google.golang.org/api/cloudfunctions/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/idtoken"

	"google.golang.org/api/option"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/tidwall/gjson"
)

type Config struct {
	Credentials            string `json:"credentials"`
	FunctionEnvironment    string `json:"functionEnvironment"`
	RequireAuthentication  bool   `json:"requireAuthentication"`
	GoogleCloudFunctionUrl string `json:"googleCloudFunctionUrl"`
}

type Client struct {
	service *cloudfunctions.Service
	opts    common.Opts
}

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("GoogleCloudFunction")
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
		return nil, fmt.Errorf("[GoogleCloudFunction] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return nil, fmt.Errorf("[GoogleCloudFunction] error  :: error in GoogleCloudFunction while unmarshalling destination config:: %w", err)
	}

	opts := []option.ClientOption{
		option.WithCredentialsJSON([]byte(config.Credentials)),
	}

	service, err := generateService(destination, opts...)
	// If err is not nil then return
	if err != nil {
		pkgLogger.Errorf("[GoogleCloudFunction] error  :: %w", err)
		return nil, err
	}
	client := &Client{service, o}
	destConfig := &Config{config.Credentials, config.FunctionEnvironment, config.RequireAuthentication, config.GoogleCloudFunctionUrl}

	return &GoogleCloudFunctionProducer{client, destConfig}, err
}

func (producer *GoogleCloudFunctionProducer) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessage string) {
	destConfig := producer.config
	parsedJSON := gjson.ParseBytes(jsonData)

	if destConfig.FunctionEnvironment == "gen1" && destConfig.RequireAuthentication {
		return invokeAuthenticatedGen1Functions(producer.client, destConfig.GoogleCloudFunctionUrl, parsedJSON)
	}

	return invokeGen2AndUnauthenticatedFunctions(destConfig.GoogleCloudFunctionUrl, destConfig.Credentials, destConfig.RequireAuthentication, parsedJSON)
}

func invokeAuthenticatedGen1Functions(client *Client, functionUrl string, parsedJSON gjson.Result) (statusCode int, respStatus, responseMessage string) {
	if client == nil {
		respStatus = "Failure"
		responseMessage = "[GoogleCloudFunction] error  :: Failed to initialize GoogleCloudFunction client"
		return 400, respStatus, responseMessage
	}

	functionName := getFunctionName(functionUrl)

	requestPayload := &cloudfunctions.CallFunctionRequest{
		Data: string(parsedJSON.String()),
	}

	// Make the HTTP request
	call := client.service.Projects.Locations.Functions.Call(functionName, requestPayload)

	response, err := call.Do()
	fmt.Print(err)
	if err != nil {
		if apiErr, ok := err.(*googleapi.Error); ok && apiErr.Code == http.StatusNotModified {
			fmt.Println("Function call was not executed (Not Modified)")
		} else {
			log.Fatalf("Failed to call function: %v", err)
		}
	}

	// Process the response (sample response handling here).
	if response != nil {
		fmt.Printf("Function call status code: %d\n", response.HTTPStatusCode)
		// Handle response content as needed.
	}
	fmt.Println("Request successful!")

	respStatus = "Success"
	responseMessage = "[GoogleCloudFunction] :: Message Payload inserted with messageId :: "
	return 200, respStatus, responseMessage
}

func invokeGen2AndUnauthenticatedFunctions(functionUrl, credentials string, requireAuthentication bool, parsedJSON gjson.Result) (statusCode int, respStatus, responseMessage string) {
	ctx := context.Background()

	jsonBytes := []byte(parsedJSON.String())

	// Create a POST request
	req, err := http.NewRequest("POST", functionUrl, strings.NewReader(string(jsonBytes)))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return
	}

	// Set the appropriate headers
	req.Header.Set("Content-Type", "application/json")
	if requireAuthentication {
		ts, err := idtoken.NewTokenSource(ctx, functionUrl, option.WithCredentialsJSON([]byte(credentials)))
		if err != nil {
			fmt.Print("failed to create NewTokenSource: %w", err)
		}

		// Get the ID token, to make an authenticated call to the target audience.
		token, err := ts.Token()
		if err != nil {
			fmt.Print("failed to receive token: %w", err)
		}
		req.Header.Add("Authorization", "Bearer "+token.AccessToken)
	}

	// Make the request using the client
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Error making request:", err)
		return
	}

	if resp.Status == "OK" {
		respStatus = "Success"
		responseMessage = "[GoogleCloudFunction] :: Message Payload inserted with messageId :: " + parsedJSON.Get("id").String()
	}
	fmt.Print(resp.Status)
	return 200, respStatus, responseMessage
}

// Initialize the Cloud Functions API client using service account credentials.
func generateService(destination *backendconfig.DestinationT, opts ...option.ClientOption) (*cloudfunctions.Service, error) {
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
	// Join the first two values with "-" and assign to REGION
	REGION := strings.Join(splitValues[:2], "-")

	// Join the rest of the values with "-" and assign to PROJECT_ID
	PROJECT_ID := strings.Join(splitValues[2:], "-")

	var FUNCTION_NAME string

	index := strings.Index(url, "cloudfunctions.net/")

	if index != -1 {
		// Extract the substring after "cloudfunctions.net/"
		FUNCTION_NAME = url[index+len("cloudfunctions.net/"):]
	} else {
		fmt.Println("No match found")
	}

	functionName = "projects/" + PROJECT_ID + "/locations/" + REGION + "/functions/" + FUNCTION_NAME

	return functionName
}

func (*GoogleCloudFunctionProducer) Close() error {
	// no-op
	return nil
}

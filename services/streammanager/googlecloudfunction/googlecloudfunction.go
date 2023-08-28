package cloudfunctions

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"

	"google.golang.org/api/cloudfunctions/v1"
	v1 "google.golang.org/api/cloudfunctions/v1"
	"google.golang.org/api/googleapi"

	"google.golang.org/api/option"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

type Config struct {
	Credentials            string `json:"credentials"`
	FunctionEnvironment    string `json:"functionEnvironment"`
	RequireAuthentication  string `json:"requireAuthentication"`
	GoogleCloudFunctionUrl string `json:"googleCloudFunctionUrl"`
}

type Credentials struct {
	Email      string `json:"client_email"`
	PrivateKey string `json:"private_key"`
	TokenUrl   string `json:"token_uri"`
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

	var opts []option.ClientOption
	if opts, err = clientOptions(&config); err != nil {
		return nil, fmt.Errorf("[GoogleCloudFunction] error :: %w", err)
	}

	service, err := generateService(destination, opts...)
	// If err is not nil then retrun
	if err != nil {
		pkgLogger.Errorf("[GoogleCloudFunction] error  :: %w", err)
		return nil, err
	}
	client := &Client{service, o}

	return &GoogleCloudFunctionProducer{client}, err
}

func (producer *GoogleCloudFunctionProducer) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessage string) {
	client := producer.client
	if client == nil {
		respStatus = "Failure"
		responseMessage = "[GoogleCloudFunction] error  :: Failed to initialize GoogleCloudFunction client"
		return 400, respStatus, responseMessage
	}

	// Make the HTTP request
	call := client.service.Projects.Locations.Functions.Call(functionName, requestPayload)

	response, err := call.Do()
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

// Initialize the Cloud Functions API client using service account credentials.
func generateService(destination *backendconfig.DestinationT, opts ...option.ClientOption) (*cloudfunctions.Service, error) {
	ctx := context.Background()

	cloudFunctionService, err := v1.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("[GoogleCloudFunction] error  :: Unable to create cloudfunction service :: %w", err)
	}
	return cloudFunctionService, err
}

func clientOptions(config *Config) ([]option.ClientOption, error) {
	var credentials Credentials
	if config.Credentials != "" {
		err := json.Unmarshal([]byte(config.Credentials), &credentials)
		if err != nil {
			return nil, fmt.Errorf("[GoogleCloudFunction] error  :: error in GoogleCloudFunction while unmarshalling credentials json:: %w", err)
		}
	}
	// Creating token URL from Credentials file if not using constant from google.JWTTOkenURL
	tokenURI := google.JWTTokenURL
	if credentials.TokenUrl != "" {
		tokenURI = credentials.TokenUrl
	}
	// Creating JWT Config which we are using for getting the oauth token
	jwtconfig := &jwt.Config{
		Email:      credentials.Email,
		PrivateKey: []byte(credentials.PrivateKey),
		Scopes: []string{
			"https://www.googleapis.com/auth",
		},
		UseIDToken: true,
		TokenURL:   tokenURI,
	}
	client, err := generateOAuthClient(jwtconfig)
	if err != nil {
		pkgLogger.Errorf("[GoogleCloudFunction] error  :: %v", err)
		return nil, err
	}
	return []option.ClientOption{option.WithHTTPClient(client)}, nil
}

// generateOAuthClient produces an OAuth client based on a jwt Config
func generateOAuthClient(jwtconfig *jwt.Config) (*http.Client, error) {
	ctx := context.Background()
	var oauthconfig *oauth2.Config
	token, err := jwtconfig.TokenSource(ctx).Token()
	if err != nil {
		return nil, fmt.Errorf("[GoogleCloudFunction] error  :: error in GoogleCloudFunction while Retrieving token for service account:: %w", err)
	}
	// Once the token is received we are generating the oauth-config client which are using for generating the google-cloud-function service
	client := oauthconfig.Client(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("[GoogleCloudFunction] error  :: Unable to create oauth client :: %w", err)
	}
	return client, err
}
func (*GoogleCloudFunctionProducer) Close() error {
	// no-op
	return nil
}

package googlesheets

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

type Config struct {
	Credentials string              `json:"credentials"`
	SheetId     string              `json:"sheetId"`
	SheetName   string              `json:"sheetName"`
	EventKeyMap []map[string]string `json:"eventKeyMap"`
	DestID      string              `json:"destId"`
	TestConfig  TestConfig          `json:"testConfig"`
}

type TestConfig struct {
	Endpoint     string `json:"endpoint"`
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
}

type Credentials struct {
	Email      string `json:"client_email"`
	PrivateKey string `json:"private_key"`
	TokenUrl   string `json:"token_uri"`
}

type Client struct {
	service *sheets.Service
	opts    common.Opts
}

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("googlesheets")
}

type GoogleSheetsProducer struct {
	client *Client
}

// NewProducer creates a producer based on destination config
func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (*GoogleSheetsProducer, error) {
	var config Config
	var headerRowStr []string
	jsonConfig, err := json.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("[GoogleSheets] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return nil, fmt.Errorf("[GoogleSheets] error  :: error in GoogleSheets while unmarshalling destination config:: %w", err)
	}

	var opts []option.ClientOption
	if config.TestConfig.Endpoint != "" { // test configuration
		opts = testClientOptions(&config)
	} else { // normal configuration
		if opts, err = clientOptions(&config); err != nil {
			return nil, fmt.Errorf("[GoogleSheets] error :: %w", err)
		}
	}

	service, err := generateService(opts...)
	// If err is not nil then retrun
	if err != nil {
		pkgLogger.Errorf("[Googlesheets] error  :: %w", err)
		return nil, err
	}

	// ** Preparing the Header Data **
	// Creating the array of string which are then converted in to an array of interface which are to
	// be added as header to each of the above spreadsheets.
	// Example: | First Name | Last Name | Birth Day | Item Purchased | ..
	// Here messageId is by default the first column
	headerRowStr = append(headerRowStr, "messageId")
	for _, eventmap := range config.EventKeyMap {
		headerRowStr = append(headerRowStr, eventmap["to"])
	}
	headerRow := getSheetsData(headerRowStr)

	client := &Client{service, o}
	// *** Adding the header ***
	// Inserting header to the sheet
	err = insertHeaderDataToSheet(client, config.SheetId, config.SheetName, headerRow)

	return &GoogleSheetsProducer{client}, err
}

func (producer *GoogleSheetsProducer) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessage string) {
	client := producer.client
	if client == nil {
		respStatus = "Failure"
		responseMessage = "[GoogleSheets] error  :: Failed to initialize google-sheets client"
		return 400, respStatus, responseMessage
	}
	parsedJSON := gjson.ParseBytes(jsonData)
	spreadSheetId := parsedJSON.Get("spreadSheetId").String()
	spreadSheet := parsedJSON.Get("spreadSheet").String()
	valueList, parseErr := parseTransformedData(parsedJSON)

	if parseErr != nil {
		respStatus = "Failure"
		responseMessage = "[GoogleSheets] error :: Failed to parse transformed data ::" + parseErr.Error()
		pkgLogger.Errorf("[Googlesheets] error while parsing transformed data :: %v", parseErr)
		return 400, respStatus, responseMessage

	}

	err := insertRowDataToSheet(client, spreadSheetId, spreadSheet, valueList)
	if err != nil {
		statCode, serviceMessage := handleServiceError(err)
		respStatus = "Failure"
		responseMessage = "[GoogleSheets] error :: Failed to insert Payload :: " + serviceMessage
		pkgLogger.Errorf("[Googlesheets] error while inserting data to sheet :: %v", err)
		return statCode, respStatus, responseMessage
	}

	respStatus = "Success"
	responseMessage = "[GoogleSheets] :: Message Payload inserted with messageId :: " + parsedJSON.Get("id").String()
	return 200, respStatus, responseMessage
}

// generateService produces a google-sheets client using the specified client options
func generateService(opts ...option.ClientOption) (*sheets.Service, error) {
	ctx := context.Background()
	sheetService, err := sheets.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("[GoogleSheets] error  :: Unable to create sheet service :: %w", err)
	}
	return sheetService, err
}

// insertHeaderDataToSheet inserts header data.
// Returns error for failure cases of API calls otherwise returns nil
func insertHeaderDataToSheet(client *Client, spreadSheetId, spreadSheetTab string, data []interface{}) error {
	// Creating value range for inserting row into sheet
	var vr sheets.ValueRange
	vr.MajorDimension = "ROWS"
	vr.Range = spreadSheetTab + "!A1"
	vr.Values = append(vr.Values, data)
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), client.opts.Timeout)
	defer cancel()

	_, err = client.service.Spreadsheets.Values.Update(spreadSheetId, spreadSheetTab+"!A1", &vr).ValueInputOption("RAW").Context(ctx).Do()

	return err
}

// insertRowDataToSheet appends row data list.
// Returns error for failure cases of API calls otherwise returns nil
func insertRowDataToSheet(client *Client, spreadSheetId, spreadSheetTab string, dataList [][]interface{}) error {
	// Creating value range for inserting row into sheet
	vr := sheets.ValueRange{
		MajorDimension: "ROWS",
		Range:          spreadSheetTab + "!A1",
		Values:         dataList,
	}
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), client.opts.Timeout)
	defer cancel()

	_, err = client.service.Spreadsheets.Values.Append(spreadSheetId, spreadSheetTab+"!A1", &vr).ValueInputOption("RAW").Context(ctx).Do()

	return err
}

// parseTransformedData returns array of values from a json.
// source is the json object from transformer and we are iterating the json as a map
// and we are storing the data into designated position in array based on transformer
// mappings.
// Example payload we have from transformer without batching:
//
//	{
//			message:{
//				1: { attributeKey: "Product Purchased", attributeValue: "Realme C3" }
//				2: { attributeKey: "Product Value, attributeValue: "5900"}
//				..
//			}
//	}
//
// Example Payload we have from transformer with batching:
//
//	{
//			batch:[
//				{
//					message: {
//						1: { attributeKey: "Product Purchased", attributeValue: "Realme C3" }
//						2: { attributeKey: "Product Value, attributeValue: "5900"}
//						..
//					}
//				},
//				{
//					message: {
//						1: { attributeKey: "Product Purchased", attributeValue: "Realme C3" }
//						2: { attributeKey: "Product Value, attributeValue: "5900"}
//						..
//					}
//				}
//			]
//	}
func parseTransformedData(source gjson.Result) ([][]interface{}, error) {
	batch := source.Get("batch")
	messages := batch.Array()
	if len(messages) == 0 {
		messages = append(messages, source)
	}
	var valueList [][]interface{}
	for _, messageElement := range messages {
		messagefields := messageElement.Get("message")
		values := make([]interface{}, len(messagefields.Map()))
		var pos int
		var err error
		if messagefields.IsObject() {
			for k, v := range messagefields.Map() {
				pos, err = strconv.Atoi(k)
				if err != nil {
					return nil, err
				}
				// Adding support for numeric type data
				attrValue := v.Get("attributeValue")
				switch attrValue.Type {
				case gjson.Number:
					values[pos] = attrValue.Float()
				default:
					values[pos] = attrValue.String()
				}

			}
		}
		valueList = append(valueList, values)
	}

	return valueList, nil
}

// getSheetsData is used to parse a string array to an interface array for compatibility
// with sheets-api
func getSheetsData(typedata []string) []interface{} {
	data := make([]interface{}, len(typedata))
	for key, value := range typedata {
		data[key] = value
	}
	return data
}

// handleServiceError is created for fail safety, if in any case when err type is not googleapi.Error
// server should not crash with a type error.
func handleServiceError(err error) (statusCode int, responseMessage string) {
	statusCode = 500
	responseMessage = err.Error()

	if err != nil && errors.Is(err, context.DeadlineExceeded) {
		statusCode = 504
	}
	if strings.Contains(err.Error(), "token expired and refresh token is not set") {
		statusCode = 721
	}

	if reflect.TypeOf(err).String() == "*googleapi.Error" {
		serviceErr := err.(*googleapi.Error)
		statusCode = serviceErr.Code
		responseMessage = serviceErr.Message
	}
	return statusCode, responseMessage
}

func clientOptions(config *Config) ([]option.ClientOption, error) {
	var credentials Credentials
	if config.Credentials != "" {
		err := json.Unmarshal([]byte(config.Credentials), &credentials)
		if err != nil {
			return nil, fmt.Errorf("[GoogleSheets] error  :: error in GoogleSheets while unmarshalling credentials json:: %w", err)
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
			"https://www.googleapis.com/auth/spreadsheets",
		},
		TokenURL: tokenURI,
	}
	client, err := generateOAuthClient(jwtconfig)
	if err != nil {
		pkgLogger.Errorf("[Googlesheets] error  :: %v", err)
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
		return nil, fmt.Errorf("[GoogleSheets] error  :: error in GoogleSheets while Retrieving token for service account:: %w", err)
	}
	// Once the token is received we are generating the oauth-config client which are using for generating the google-sheets service
	client := oauthconfig.Client(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("[GoogleSheets] error  :: Unable to create oauth client :: %w", err)
	}
	return client, err
}

func testClientOptions(config *Config) []option.ClientOption {
	token := &oauth2.Token{
		AccessToken:  config.TestConfig.AccessToken,
		RefreshToken: config.TestConfig.RefreshToken,
	}
	// skipcq: GO-S1020
	tlsConfig := &tls.Config{
		// skipcq: GSC-G402
		InsecureSkipVerify: true,
	}
	client := oauth2.NewClient(context.Background(), oauth2.StaticTokenSource(token))
	trans := client.Transport.(*oauth2.Transport)
	trans.Base = &http.Transport{TLSClientConfig: tlsConfig}
	return []option.ClientOption{option.WithEndpoint(config.TestConfig.Endpoint), option.WithHTTPClient(client)}
}

func (*GoogleSheetsProducer) Close() error {
	// no-op
	return nil
}

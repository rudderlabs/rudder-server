package googlesheets

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/go-viper/mapstructure/v2"
	"github.com/tidwall/gjson"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

type Config struct {
	Credentials string              `mapstructure:"credentials"`
	SheetId     string              `mapstructure:"sheetId"`
	SheetName   string              `mapstructure:"sheetName"`
	EventKeyMap []map[string]string `mapstructure:"eventKeyMap"`
	TestConfig  TestConfig          `mapstructure:"testConfig"`
}

type TestConfig struct {
	Endpoint     string `mapstructure:"endpoint"`
	AccessToken  string `mapstructure:"accessToken"`
	RefreshToken string `mapstructure:"refreshToken"`
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
	config          Config
	client          *Client
	lock            sync.RWMutex
	isHeaderUpdated bool
}

// NewProducer creates a producer based on destination config
func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (*GoogleSheetsProducer, error) {
	var config Config
	if err := mapstructure.Decode(destination.Config, &config); err != nil {
		return nil, fmt.Errorf("[GoogleSheets] error  :: error in GoogleSheets while parsing destination config:: %w", err)
	}

	opts, err := prepareClientOptions(&config)
	if err != nil {
		return nil, fmt.Errorf("[GoogleSheets] error :: preparing client options :: %w", err)
	}

	service, err := sheets.NewService(context.Background(), opts...)
	// If err is not nil then retrun
	if err != nil {
		pkgLogger.Errorn("[Googlesheets] error :: creating service client", obskit.Error(err))
		return nil, fmt.Errorf("[GoogleSheets] error :: creating service client :: %w", err)
	}

	return &GoogleSheetsProducer{
		config:          config,
		client:          &Client{service, o},
		lock:            sync.RWMutex{},
		isHeaderUpdated: false,
	}, err
}

func (p *GoogleSheetsProducer) updateHeader() error {
	p.lock.RLock()
	if p.isHeaderUpdated {
		p.lock.RUnlock()
		return nil
	}
	p.lock.RUnlock()

	p.lock.Lock()
	defer p.lock.Unlock()
	if p.isHeaderUpdated {
		return nil
	}

	var headerRowStr []string
	// ** Preparing the Header Data **
	// Creating the array of string which are then converted in to an array of interface which are to
	// be added as header to each of the above spreadsheets.
	// Example: | First Name | Last Name | Birth Day | Item Purchased | ..
	// Here messageId is by default the first column
	headerRowStr = append(headerRowStr, "messageId")
	for _, eventmap := range p.config.EventKeyMap {
		headerRowStr = append(headerRowStr, eventmap["to"])
	}
	headerRow := getSheetsData(headerRowStr)

	if err := p.insertHeaderDataToSheet(headerRow); err != nil {
		return err
	}
	p.isHeaderUpdated = true
	return nil
}

func (p *GoogleSheetsProducer) Produce(jsonData json.RawMessage, _ any) (statusCode int, respStatus, responseMessage string) {
	if p.client == nil {
		respStatus = "Failure"
		responseMessage = "[GoogleSheets] error  :: Failed to initialize google-sheets client"
		return 400, respStatus, responseMessage
	}
	if err := p.updateHeader(); err != nil {
		statCode, serviceMessage := handleServiceError(err)
		respStatus = "Failure"
		responseMessage = "[GoogleSheets] error :: Failed to update header :: " + serviceMessage
		pkgLogger.Errorn("[Googlesheets] error while updating header", obskit.Error(err))
		return statCode, respStatus, responseMessage
	}

	parsedJSON := gjson.ParseBytes(jsonData)
	valueList, parseErr := parseTransformedData(parsedJSON)

	if parseErr != nil {
		respStatus = "Failure"
		responseMessage = "[GoogleSheets] error :: Failed to parse transformed data ::" + parseErr.Error()
		pkgLogger.Errorn("[Googlesheets] error while parsing transformed data", obskit.Error(parseErr))
		return 400, respStatus, responseMessage

	}

	if err := p.insertRowDataToSheet(valueList); err != nil {
		statCode, serviceMessage := handleServiceError(err)
		respStatus = "Failure"
		responseMessage = "[GoogleSheets] error :: Failed to insert Payload :: " + serviceMessage
		pkgLogger.Errorn("[Googlesheets] error while inserting data to sheet", obskit.Error(err))
		return statCode, respStatus, responseMessage
	}

	respStatus = "Success"
	responseMessage = "[GoogleSheets] :: Message Payload inserted with messageId :: " + parsedJSON.Get("id").String()
	return 200, respStatus, responseMessage
}

// insertHeaderDataToSheet inserts header data.
// Returns error for failure cases of API calls otherwise returns nil
func (p *GoogleSheetsProducer) insertHeaderDataToSheet(data []any) error {
	// Creating value range for inserting row into sheet
	var vr sheets.ValueRange
	vr.MajorDimension = "ROWS"
	vr.Range = p.config.SheetName + "!A1"
	vr.Values = append(vr.Values, data)
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), p.client.opts.Timeout)
	defer cancel()

	_, err = p.client.service.Spreadsheets.Values.Update(p.config.SheetId, p.config.SheetName+"!A1", &vr).ValueInputOption("RAW").Context(ctx).Do()

	return err
}

// insertRowDataToSheet appends row data list.
// Returns error for failure cases of API calls otherwise returns nil
func (p *GoogleSheetsProducer) insertRowDataToSheet(dataList [][]any) error {
	// Creating value range for inserting row into sheet
	vr := sheets.ValueRange{
		MajorDimension: "ROWS",
		Range:          p.config.SheetName + "!A1",
		Values:         dataList,
	}
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), p.client.opts.Timeout)
	defer cancel()

	_, err = p.client.service.Spreadsheets.Values.Append(p.config.SheetId, p.config.SheetName+"!A1", &vr).ValueInputOption("RAW").Context(ctx).Do()

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
func parseTransformedData(source gjson.Result) ([][]any, error) {
	batch := source.Get("batch")
	messages := batch.Array()
	if len(messages) == 0 {
		messages = append(messages, source)
	}
	var valueList [][]any
	for _, messageElement := range messages {
		messagefields := messageElement.Get("message")
		values := make([]any, len(messagefields.Map()))
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
func getSheetsData(typedata []string) []any {
	data := make([]any, len(typedata))
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

	if errors.Is(err, context.DeadlineExceeded) {
		return 504, responseMessage
	}

	var serviceErr *googleapi.Error
	if errors.As(err, &serviceErr) {
		return serviceErr.Code, serviceErr.Message
	}
	return statusCode, responseMessage
}

func newOAuth2Client(config *Config) (*http.Client, error) {
	ctx := context.Background()
	jwtConfig, err := google.JWTConfigFromJSON([]byte(config.Credentials), sheets.SpreadsheetsScope)
	if err != nil {
		return nil, fmt.Errorf("[GoogleSheets] error :: parsing credentials :: %w", err)
	}
	oauth2Client := oauth2.NewClient(ctx, jwtConfig.TokenSource(ctx))
	return oauth2Client, nil
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

func realClientOptions(config *Config) ([]option.ClientOption, error) {
	oauth2Client, err := newOAuth2Client(config)
	if err != nil {
		return nil, err
	}
	return []option.ClientOption{option.WithHTTPClient(oauth2Client)}, nil
}

func prepareClientOptions(config *Config) ([]option.ClientOption, error) {
	if config.TestConfig.Endpoint != "" { // test configuration
		return testClientOptions(config), nil
	}
	return realClientOptions(config)
}

func (*GoogleSheetsProducer) Close() error {
	// no-op
	return nil
}

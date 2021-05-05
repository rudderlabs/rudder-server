package googlesheets

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/sheets/v4"
)

type Config struct {
	Credentials   string              `json:"credentials"`
	SheetId       string              `json:"sheetId"`
	EventSheetMap []map[string]string `json:"eventSheetMapping"`
	EventKeyMap   []map[string]string `json:"eventKeyMap"`
}

type Credentials struct {
	Email      string `json:"client_email"`
	PrivateKey string `json:"private_key"`
	TokenUrl   string `json:"token_uri"`
}

type GoogleAPIService struct {
	Jwt     *jwt.Config
	Service *sheets.Service
}

var pkgLogger logger.LoggerI
var googleAPIService GoogleAPIService

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("googlesheets")
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (*GoogleAPIService, error) {
	var config Config
	var credentialsFile Credentials
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf("[GoogleSheets] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return nil, fmt.Errorf("[GoogleSheets] error  :: error in GoogleSheets while unmarshalling destination config:: %w", err)
	}
	if config.Credentials != "" {
		err = json.Unmarshal([]byte(config.Credentials), &credentialsFile)
		if err != nil {
			return nil, fmt.Errorf("[GoogleSheets] error  :: error in GoogleSheets while unmarshalling credentials json:: %w", err)
		}

	}
	// Creating token URL from Credentials file if not using constant from google.JWTTOkenURL
	tokenURI := google.JWTTokenURL
	if len(credentialsFile.TokenUrl) != 0 {
		tokenURI = credentialsFile.TokenUrl
	}
	// Creating JWT Config which we are using for getting the oauth token
	jwtconfig := &jwt.Config{
		Email:      credentialsFile.Email,
		PrivateKey: []byte(credentialsFile.PrivateKey),
		Scopes: []string{
			"https://www.googleapis.com/auth/spreadsheets",
		},
		TokenURL: tokenURI,
	}
	googleAPIService.Jwt = jwtconfig
	service, err := generateServiceWithRefreshToken(*jwtconfig)
	googleAPIService.Service = service
	// If err is not nil then retrun
	if err != nil {
		return &googleAPIService, err
	}

	// Storing All the Spread-Sheets (Pages withing a single Google-sheets) where the events are
	// required to be mapped
	// Example: | Sheet1 | Sheet2 | Sheet3 | ..
	var sheets []string
	for _, eventtosheetmap := range config.EventSheetMap {
		sheets = append(sheets, eventtosheetmap["to"])
	}

	// Creating the array of string which are then coverted in to an array of interface which are to
	// be added as header to each of the above spreadsheets.
	// Example: | First Name | Last Name | Birth Day | Item Purchased | ..
	var headerRowStr []string
	for _, eventmap := range config.EventKeyMap {
		headerRowStr = append(headerRowStr, eventmap["to"])
	}
	headerRow := getSheetsData(headerRowStr)

	// *** Adding the header ***
	// Iterating each sheets we adding the header for them if any error occurs here we throw it causing an error in creation of destination, else if all operation occurs successfuly we complete the NewProducer processes successfully
	for _, sheet := range sheets {
		// Inserting header to the sheet
		err = insertDataToSheet(config.SheetId, sheet, headerRow, true)
	}
	return &googleAPIService, err
}

func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (statusCode int, respStatus string, responseMessage string) {

	// Here we recieve the transformed json with all the required mappings
	parsedJSON := gjson.ParseBytes(jsonData)
	// Storing sheet_id and sheet_name for the specific message
	spreadSheetId := parsedJSON.Get("spreadSheetId").String()
	spreadSheet := parsedJSON.Get("spreadSheet").String()

	// Here we extract the values of the transformed json into a string array
	// because google sheets-api works with array of data if any error occurs here we
	// log it as parse error for that event
	values, parseErr := parseTransformedData(parsedJSON)

	if parseErr != nil {
		respStatus = "Failure"
		responseMessage = "[GoogleSheets] error :: Failed to parse transformed data ::" + parseErr.Error()
		return 400, respStatus, responseMessage

	}

	message := getSheetsData(values)

	// *** Appending actual Rudder event as Row ***
	// In this section we are appending the actual messages into the sheet
	// After parsing the parsedJSON as a row specific data which we are storing in message,
	// and appending event (message) to the sheet
	err := insertDataToSheet(spreadSheetId, spreadSheet, message, false)
	if err != nil {
		statCode, serviceMessage := handleServiceError(err)
		respStatus = "Failure"
		responseMessage = "[GoogleSheets] error :: Failed to insert Payload :: " + serviceMessage
		return statCode, respStatus, responseMessage
	}

	respStatus = "Success"
	responseMessage = "[GoogleSheets] :: Message Payload inserted with messageId :: " + parsedJSON.Get("id").String()
	return 200, respStatus, responseMessage
}

// This method produces a google-sheets client from a jwt.Config client by
// retrieveing access token
func generateServiceWithRefreshToken(jwtconfig jwt.Config) (*sheets.Service, error) {
	ctx := context.Background()
	var oauthconfig *oauth2.Config
	token, err := jwtconfig.TokenSource(ctx).Token()
	if err != nil {
		return nil, fmt.Errorf("[GoogleSheets] error  :: error in GoogleSheets while Retrieving token for service account:: %w", err)
	}
	// Once the token is received we are generating the oauth-config client which are using for
	// generating the google-sheets service
	client := oauthconfig.Client(ctx, token)
	sheetService, err := sheets.New(client)
	if err != nil {
		return nil, fmt.Errorf("[GoogleSheets] error  :: Unable to create sheet service :: %w", err)
	}
	return sheetService, err
}

// Wrapper func to insert headerData or rowData based on boolean flag.
// This method uses global googleApiService Client for making API Calls to google-sheets.
// Returns error for failure cases of API calls otherwise returns nil
// In case of Access Token expiry it handles by creating a new client
// with new access token and recursively calling the function in order to prevent event loss
func insertDataToSheet(spreadSheetId string, spreadSheetTab string, data []interface{}, isHeader bool) error {
	// Creating value range for inserting row into sheet
	var vr sheets.ValueRange
	vr.MajorDimension = "ROWS"
	vr.Range = spreadSheetTab + "!A1"
	vr.Values = append(vr.Values, data)
	var err error
	if isHeader {
		// In case we have a header Row we want to update it always at the the top of sheet
		// hence we are using update.
		_, err = googleAPIService.Service.Spreadsheets.Values.Update(spreadSheetId, spreadSheetTab+"!A1", &vr).ValueInputOption("RAW").Do()

	} else {
		// In case of a event Row we want to append it sequentially below the header hence we
		// are using append
		_, err = googleAPIService.Service.Spreadsheets.Values.Append(spreadSheetId, spreadSheetTab+"!A1", &vr).ValueInputOption("RAW").Do()
	}
	if err != nil && strings.Contains(err.Error(), "token expired and refresh token is not set") {
		var serviceErr error
		pkgLogger.Debug("[Google Sheets]Token Expired :: Generating New Client")
		// Here we are updating the client with new generated client
		googleAPIService.Service, serviceErr = generateServiceWithRefreshToken(*googleAPIService.Jwt)
		if serviceErr != nil {
			pkgLogger.Error("[Google Sheets]Token Expired :: Failed to Generate New Client", serviceErr.Error())
			return serviceErr
		}
		pkgLogger.Debug("[Google Sheets]Token Expired :: Generated New Client")
		return insertDataToSheet(spreadSheetId, spreadSheetTab, data, isHeader)
	} else if err != nil {
		return err
	}
	return nil
}

// Method to return array of values from a json.
// source is the json object from transformer and we are iterating the json as a map
// and we are storing the data into designated position in array based on transformer
// mappings.
// Example payload we have from transformer:
// {
//		message:{
//			1: { attributeKey: "Product Purchased", attributeValue: "Realme C3" }
//			2: { attributeKey: "Product Value, attributeValue: "5900"}
//			..
// 		}
// }
func parseTransformedData(source gjson.Result) ([]string, error) {
	//var sourcemap map[string]gjson.Result
	messagefields := source.Get("message")
	values := make([]string, len(messagefields.Map()))
	var pos int
	var err error
	if messagefields.IsObject() {
		for k, v := range messagefields.Map() {
			pos, err = strconv.Atoi(k)
			if err != nil {
				return values, err
			}
			values[pos] = v.Get("attributeValue").String()
		}
	}
	return values, err
}

// Func used to parse a string array to an interface array for compatibility
// with sheets-api
func getSheetsData(typedata []string) []interface{} {
	data := make([]interface{}, len(typedata))
	for key, value := range typedata {
		data[key] = value
	}
	return data
}

// This method is created for fail safety, if in any case when err type is not googleapi.Error
// we should not crash with a type error.
func handleServiceError(err error) (statusCode int, responseMessage string) {
	statusCode = 500
	responseMessage = err.Error()
	if reflect.TypeOf(err).String() == "*googleapi.Error" {
		serviceErr := err.(*googleapi.Error)
		statusCode = serviceErr.Code
		responseMessage = serviceErr.Message
	}
	return statusCode, responseMessage
}

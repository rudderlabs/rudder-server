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
	Credentials     string              `json:"credentials"`
	EventToSheetMap []map[string]string `json:"eventToSpreadSheetIdMap"`
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
	return &googleAPIService, err
}

func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (statusCode int, respStatus string, responseMessage string) {

	// Here we recieve the transformed json with all the required mappings
	parsedJSON := gjson.ParseBytes(jsonData)
	// Storing sheet_id and sheet_tab for the specific message
	spreadSheetId := parsedJSON.Get("spreadSheetId").String()
	spreadSheetTab := parsedJSON.Get("spreadSheetTab").String()

	// Here we extract the keys, values of the transformed json into two string arrays
	// because google sheets-api works with array of data if any error occurs here we
	// log it as parse error for that event
	keys, values, parseErr := parseTransformedData(parsedJSON)

	if parseErr != nil {
		respStatus = "Failure"
		responseMessage = "[GoogleSheets] error :: Failed to parse transformed data ::" + parseErr.Error()
		return 400, respStatus, responseMessage

	}

	// *** Adding the header ***
	// Converting the string array of keys to an array of interface compatibale with sheets-api (Keys relates to sheet header)
	headerRow := getSheetsData(keys)

	// Inserting header to the sheet
	err := insertDataToSheet(spreadSheetId, spreadSheetTab, headerRow, true)
	if err != nil {
		statCode, serviceMessage := handleServiceError(err)
		respStatus = "Failure"
		responseMessage = "[GoogleSheets] error :: Failed to insert Header :: " + serviceMessage
		return statCode, respStatus, responseMessage
	}

	// *** Appending actual Rudder event as Row ***
	// In this section we are appending the actual messages into the sheet
	// After parsing the parsedJSON as a row specific data which we are storing in message
	message := getSheetsData(values)

	// Inserting event (message) to the sheet
	err = insertDataToSheet(spreadSheetId, spreadSheetTab, message, false)
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

// This method is created for fail safety, if in any case when err type is not googleapi.Error
// we should not crash with a type error.
func handleServiceError(err error) (statusCode int, responseMessage string) {
	statusCode = 400
	responseMessage = err.Error()
	if reflect.TypeOf(err).String() == "*googleapi.Error" {
		serviceErr := err.(*googleapi.Error)
		statusCode = serviceErr.Code
		responseMessage = serviceErr.Message
	}
	return statusCode, responseMessage
}

// Method to return array of keys, values from a json.
// source is the json object from transformer and we are iterating the json as a map
// and we are storing the data into designated position in array based on transformer
// mappings.
// Example payload we have from transformer:
// { message:
// 	 { 	'0': { key: 'anonymous_id', value: '' },
// 	 	'1': { key: 'user_id', value: 'userTest004' },
// 	 	'2': { key: 'event', value: 'Page Call' },
// 	 	..}
// }
func parseTransformedData(source gjson.Result) ([]string, []string, error) {
	//var sourcemap map[string]gjson.Result
	messagefields := source.Get("message")
	keys := make([]string, len(messagefields.Map()))
	values := make([]string, len(messagefields.Map()))
	var pos int
	var err error
	if messagefields.IsObject() {
		for k, v := range messagefields.Map() {
			pos, err = strconv.Atoi(k)
			if err != nil {
				return keys, values, err
			}
			keys[pos] = v.Get("key").String()
			values[pos] = v.Get("value").String()
		}
	}
	return keys, values, err
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

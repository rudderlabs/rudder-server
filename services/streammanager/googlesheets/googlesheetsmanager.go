package googlesheets

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

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

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("googlesheets")
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (*sheets.Service, error) {
	var config Config
	var credentialsFile Credentials
	var oauthconfig *oauth2.Config
	ctx := context.Background()
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf("[GoogleSheets] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return nil, fmt.Errorf("[GoogleSheets] error  :: error in GoogleSheets while unmarshelling destination config:: %w", err)
	}
	if config.Credentials != "" {
		err = json.Unmarshal([]byte(config.Credentials), &credentialsFile)
		if err != nil {
			return nil, fmt.Errorf("[GoogleSheets] error  :: error in GoogleSheets while unmarshelling credentials json:: %w", err)
		}

	}
	// Creating token URL from Credentials file if not using constant from google.JWTTOkenURL
	tokenURI := google.JWTTokenURL
	if len(credentialsFile.TokenUrl) != 0 {
		tokenURI = credentialsFile.TokenUrl
	}
	// Creating HWT Config which we are using for getting the oauth token
	jwtconfig := &jwt.Config{
		Email:      credentialsFile.Email,
		PrivateKey: []byte(credentialsFile.PrivateKey),
		Scopes: []string{
			"https://www.googleapis.com/auth/spreadsheets",
		},
		TokenURL: tokenURI,
	}
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

func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (statusCode int, respStatus string, responseMessage string) {
	service := producer.(*sheets.Service)
	// Here we recieve the transformed json with all the required mappings
	parsedJSON := gjson.ParseBytes(jsonData)
	// Storing sheet_id and sheet_tab for the specific message
	spreadSheetId := parsedJSON.Get("spreadSheetId").String()
	spreadSheetTab := parsedJSON.Get("spreadSheetTab").String()
	// Here we extract the keys, values of the transformed json into two string arrays
	// because google sheets-api works with array of data if any error occurs here we
	// log it as parse error for that event
	keys, values, parseErr := ParseTransformedData(parsedJSON)

	if parseErr != nil {
		respStatus = "Faliure"
		responseMessage = "[GoogleSheets] error :: Failed to parse transformed data ::" + parseErr.Error()
		return 400, respStatus, responseMessage

	}
	// Converting the string array of keys to an array of interface compatibale with sheets-api (Keys relates to sheet header)
	headerRow := GetSheetsData(keys)
	// Creating value range for inserting row into sheet
	var vr sheets.ValueRange
	vr.MajorDimension = "ROWS"
	vr.Range = spreadSheetTab + "!A1"
	// This is the header row for creating the header in the sheet
	vr.Values = append(vr.Values, headerRow)
	_, err := service.Spreadsheets.Values.Update(spreadSheetId, spreadSheetTab+"!A1", &vr).ValueInputOption("RAW").Do()

	if err != nil {
		statCode, serviceMessage := HandleServiceError(err)
		respStatus = "Faliure"
		responseMessage = "[GoogleSheets] error :: Failed to add Header :: " + serviceMessage
		return statCode, respStatus, responseMessage
	}

	// In this section we are appending the actual messages into the sheet
	messageValues := [][]interface{}{}
	// After parsing the parsedJSON as a row specific data which we are storing in message
	message := GetSheetsData(values)
	vr.Values = append(messageValues, message)
	// Appending the actual message into the specific sheet
	_, err = service.Spreadsheets.Values.Append(spreadSheetId, spreadSheetTab+"!A1", &vr).ValueInputOption("RAW").Do()
	if err != nil {
		statCode, serviceMessage := HandleServiceError(err)
		respStatus = "Faliure"
		responseMessage = "[GoogleSheets] error :: Failed to insert Payload :: " + serviceMessage
		return statCode, respStatus, responseMessage
	}

	respStatus = "Success"
	responseMessage = "[GoogleSheets] :: Message Payload inserted with messageId :: " + parsedJSON.Get("id").String()
	return 200, respStatus, responseMessage
}

// This method is created for fail safety, if in any case when err type is not googleapi.Error
// we should not crash with a type error.
func HandleServiceError(err error) (statusCode int, responseMessage string) {
	statusCode = 400
	responseMessage = err.Error()
	if reflect.TypeOf(err).String() == "*googleapi.Error" {
		serviceErr := err.(*googleapi.Error)
		statusCode = serviceErr.Code
		responseMessage = serviceErr.Message
	}
	return statusCode, responseMessage
}

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
func ParseTransformedData(source gjson.Result) ([]string, []string, error) {
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

//used to parse a string array to an interface array for compatibility
// with sheets-api
func GetSheetsData(typedata []string) []interface{} {
	data := make([]interface{}, len(typedata))
	for key, value := range typedata {
		data[key] = value
	}
	return data
}

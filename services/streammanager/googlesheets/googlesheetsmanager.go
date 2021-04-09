package googlesheets

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/services/streammanager/googlesheets/schemas"
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
	parsedJSON := gjson.ParseBytes(jsonData)
	service := producer.(*sheets.Service)
	// Storing sheet_id and sheet_tab for the specific message
	spreadSheetId := parsedJSON.Get("spreadSheetId").String()
	spreadSheetTab := parsedJSON.Get("spreadSheetTab").String()
	// Creating value range for inserting row into sheet
	var vr sheets.ValueRange
	vr.MajorDimension = "ROWS"
	vr.Range = spreadSheetTab + "!A1"
	// This is the header row for creating the header in the sheet
	headerRow := schemas.GetSheetsHeaderSchema()
	vr.Values = append(vr.Values, headerRow)
	_, err := service.Spreadsheets.Values.Update(spreadSheetId, spreadSheetTab+"!A1", &vr).ValueInputOption("RAW").Do()

	if err != nil {
		serviceError := err.(*googleapi.Error)
		respStatus = "Faliure"
		responseMessage = "[GoogleSheets] error :: Failed to add Header :: " + serviceError.Message
		return serviceError.Code, respStatus, responseMessage
	}

	// In this section we are appending the actual messages into the sheet
	messageValues := [][]interface{}{}
	// After parsing the parsedJSON as a row specific message we are storing it message
	message := schemas.GetSheetsEvent(parsedJSON)
	vr.Values = append(messageValues, message)
	// Appending the actual message into the specific sheet
	_, err = service.Spreadsheets.Values.Append(spreadSheetId, spreadSheetTab+"!A1", &vr).ValueInputOption("RAW").Do()
	if err != nil {
		serviceError := err.(*googleapi.Error)
		respStatus = "Faliure"
		responseMessage = "[GoogleSheets] error :: Failed to insert Payload :: " + serviceError.Message
		return serviceError.Code, respStatus, responseMessage
	}

	respStatus = "Success"
	responseMessage = "[GoogleSheets] :: Message Payload inserted with messageId :: " + parsedJSON.Get("id").String()
	return 200, respStatus, responseMessage
}

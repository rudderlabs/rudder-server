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
	"google.golang.org/api/sheets/v4"
)

type Config struct {
	Credentials     string              `json:"credentials"`
	EventToSheetMap []map[string]string `json:"eventToSpreadSheetIdMap"`
}

type Credentials struct {
	Email      string `json:"client_email"`
	PrivateKey string `json:"private_key"`
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

	jwtconfig := &jwt.Config{
		Email:      credentialsFile.Email,
		PrivateKey: []byte(credentialsFile.PrivateKey),
		Scopes: []string{
			"https://www.googleapis.com/auth/spreadsheets",
		},
		TokenURL: google.JWTTokenURL, // credentialsFile.TokenURL,google.JWTTokenURL,

	}
	token, err := jwtconfig.TokenSource(oauth2.NoContext).Token()
	if err != nil {
		return nil, fmt.Errorf("[GoogleSheets] error  :: error in GoogleSheets while Retrieving token for service account:: %w", err)
	}
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
	spreadSheetId := parsedJSON.Get("spreadSheetTab").String()
	spreadSheetTab := parsedJSON.Get("spreadSheetTab").String()
	var vr sheets.ValueRange
	vr.MajorDimension = "ROWS"
	vr.Range = spreadSheetTab + "!A1"
	headerRow := schemas.GetSheetsHeaderSchema()
	vr.Values = append(vr.Values, headerRow)
	_, err := service.Spreadsheets.Values.Update(spreadSheetId, spreadSheetTab+"!A1", &vr).ValueInputOption("RAW").Do()

	if err != nil {
		fmt.Print("Unable to set data into sheet.", err)
		respStatus = "Failed"
		responseMessage = "Failed Code Message"
		return 500, respStatus, responseMessage
	}

	// Append actual messages
	fmt.Println("json", parsedJSON)
	messageValues := [][]interface{}{}
	message := schemas.GetSheetsEvent(parsedJSON)
	vr.Values = append(messageValues, message)
	_, err = service.Spreadsheets.Values.Append(spreadSheetId, spreadSheetTab+"!A1", &vr).ValueInputOption("RAW").Do()
	if err != nil {
		fmt.Print("Unable to retrieve data from sheet.", err)
		respStatus = "Failed"
		responseMessage = "Failed OMG"
		return 500, respStatus, responseMessage
	}

	fmt.Println("success")
	respStatus = "Success"
	responseMessage = "Done"
	return 200, respStatus, responseMessage
}

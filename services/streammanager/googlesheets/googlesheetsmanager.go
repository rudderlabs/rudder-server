package googlesheets

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

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
	var vr sheets.ValueRange
	vr.MajorDimension = "ROWS"
	vr.Range = strings.Join([]string{parsedJSON.Get("spreadSheetTab").String(), "!A1"}, "")
	headerRow := []interface{}{
		"id",
		"anonymous_id",
		"context_app_build",
		"context_app_name",
		"context_app_namespace",
		"context_app_version",
		"context_device_id",
		"context_device_manufacturer",
		"context_device_model",
		"context_device_name",
		"context_device_type",
		"context_library_name",
		"context_library_version",
		"context_locale",
		"context_network_carrier",
		"context_network_bluetooth",
		"context_network_cellular",
		"context_network_wifi",
		"context_os_name",
		"context_os_version",
		"context_screen",
		"context_timezone",
		"context_traits",
		"context_userAgent",
		"event_text",
		"event_name",
		"sent_at",
		"received_at",
		"timestamp",
		"original_timestamp",
		"properties"}
	vr.Values = append(vr.Values, headerRow)
	_, err := service.Spreadsheets.Values.Update(parsedJSON.Get("spreadSheetId").String(), parsedJSON.Get("spreadSheetTab").String()+"!A1", &vr).ValueInputOption("RAW").Do()

	if err != nil {
		fmt.Print("Unable to set data into sheet.", err)
		respStatus = "Failed"
		responseMessage = "Failed Code Message"
		return 500, respStatus, responseMessage
	}

	// Append actual messages
	fmt.Println("json", parsedJSON)
	messageValues := [][]interface{}{}
	message := []interface{}{
		parsedJSON.Get("id").String(),
		parsedJSON.Get("anonymous_id").String(),
		parsedJSON.Get("context_app_build").String(),
		parsedJSON.Get("context_app_name").String(),
		parsedJSON.Get("context_app_namespace").String(),
		parsedJSON.Get("context_app_version").String(),
		parsedJSON.Get("context_device_id").String(),
		parsedJSON.Get("context_device_manufacturer").String(),
		parsedJSON.Get("context_device_model").String(),
		parsedJSON.Get("context_device_name").String(),
		parsedJSON.Get("context_device_type").String(),
		parsedJSON.Get("context_library_name").String(),
		parsedJSON.Get("context_library_version").String(),
		parsedJSON.Get("context_locale").String(),
		parsedJSON.Get("context_network_carrier").String(),
		parsedJSON.Get("context_network_bluetooth").String(),
		parsedJSON.Get("context_network_cellular").String(),
		parsedJSON.Get("context_network_wifi").String(),
		parsedJSON.Get("context_os_name").String(),
		parsedJSON.Get("context_os_version").String(),
		parsedJSON.Get("context_screen").String(),
		parsedJSON.Get("context_timezone").String(),
		parsedJSON.Get("context_traits").String(),
		parsedJSON.Get("context_userAgent").String(),
		parsedJSON.Get("event_text").String(),
		parsedJSON.Get("event_name").String(),
		parsedJSON.Get("sent_at").String(),
		parsedJSON.Get("received_at").String(),
		parsedJSON.Get("timestamp").String(),
		parsedJSON.Get("original_timestamp").String(),
		parsedJSON.Get("properties").String()}
	vr.Values = append(messageValues, message)
	_, err = service.Spreadsheets.Values.Append(parsedJSON.Get("spreadSheetId").String(), parsedJSON.Get("spreadSheetTab").String()+"!A1", &vr).ValueInputOption("RAW").Do()
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

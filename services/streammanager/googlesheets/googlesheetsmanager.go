package googlesheets

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/sheets/v4"
)

type Config struct {
	Credentials     string              `json:"credentials"`
	EventToSheetMap []map[string]string `json:"eventToSpreadSheetIdMap"`
}

type SheetsClient struct {
	sheetsclient *sheets.Service
}

type Message struct {
	anonymous_id       string
	context_app        string
	context_device     string
	context_library    string
	context_locale     string
	context_network    string
	context_os         string
	context_screen     string
	context_timezone   string
	context_traits     string
	contex_userAgent   string
	id                 string
	sent_at            string
	received_at        string
	timestamp          string
	original_timestamp string
	event_text         string
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("googlesheets")
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (*sheets.Service, error) {
	var config Config
	var credentialsFile *jwt.Config
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
		marshalledCredentials, err := json.Marshal(config.Credentials)
		if err != nil {
			return nil, fmt.Errorf("[GoogleSheets] error  :: error in GoogleSheets while marshelling Credentials from config:: %w", err)
		}
		json.Unmarshal(marshalledCredentials, &credentialsFile)

	}
	jwtconfig := &jwt.Config{
		Email:      credentialsFile.Email,
		PrivateKey: []byte(credentialsFile.PrivateKey),
		Scopes: []string{
			"https://www.googleapis.com/auth/spreadsheets",
		},
		TokenURL: credentialsFile.TokenURL, //google.JWTTokenURL,

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

	for _, s := range config.EventToSheetMap {
		headerRow := []string{
			"anonymous_id",
			"context_app",
			"context_device",
			"context_library",
			"context_locale",
			"context_network",
			"context_os",
			"context_screen",
			"context_timezone",
			"context_traits",
			"contex_userAgent",
			"id",
			"sent_at",
			"received_at",
			"timestamp",
			"original_timestamp",
			"event_text",
		}
		insertData(s["to"], headerRow, sheetService)
	}

	return sheetService, err
}

func insertData(spreadsheetId string, data []interface{}, sheetService *sheets.Service) error {
	var vr sheets.ValueRange
	vr.MajorDimension = "ROWS"
	vr.Range = "Sheet1!A1"
	values := []interface{}{data}
	vr.Values = append(vr.Values, values)
	_, err := sheetService.Spreadsheets.Values.Append(spreadsheetId, "Sheet1!A1", &vr).ValueInputOption("RAW").Do()
	return err
}

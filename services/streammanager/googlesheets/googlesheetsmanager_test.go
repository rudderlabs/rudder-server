package googlesheets

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"

	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

const (
	sheetId       = "sheetId"
	sheetName     = "sheetName"
	destinationId = "destinationId"
	header1       = "Product Purchased"
	header2       = "Product Value"
)

func Test_Timeout(t *testing.T) {
	pool, err := dockertest.NewPool("")
	pool.MaxWait = 2 * time.Minute
	require.NoError(t, err)

	testConfig, err := SetupTestGoogleSheets(pool, t)
	require.NoError(t, err)

	mockCtrl := gomock.NewController(t)
	mockLogger := mock_logger.NewMockLogger(mockCtrl)
	mockLogger.EXPECT().Errorf(gomock.Any(), gomock.Any()).AnyTimes()
	pkgLogger = mockLogger

	config := map[string]interface{}{
		"SheetId":     sheetId,
		"SheetName":   sheetName,
		"DestID":      destinationId,
		"Credentials": "",
		"EventKeyMap": []map[string]string{
			{"to": header1},
			{"to": header2},
		},
		"TestConfig": testConfig,
	}
	destination := backendconfig.DestinationT{Config: config}
	producer, err := NewProducer(&destination, common.Opts{Timeout: 10 * time.Second})
	if err != nil {
		t.Fatalf(" %+v", err)
	}
	producer.client.opts = common.Opts{Timeout: 1 * time.Microsecond}
	json := fmt.Sprintf(`{
		"spreadSheetId": "%s",
		"spreadSheet": "%s",
		"message":{
			"0": { "attributeKey": "%s", "attributeValue": "Realme C3" }
			"1": { "attributeKey": "%s", "attributeValue": "5900"}
		}
	}`, sheetId, sheetName, header1, header2)
	statusCode, respStatus, responseMessage := producer.Produce([]byte(json), nil)
	const expectedStatusCode = 504
	if statusCode != expectedStatusCode {
		t.Errorf("Expected status code %d, got %d.", expectedStatusCode, statusCode)
	}

	const expectedRespStatus = "Failure"
	if respStatus != expectedRespStatus {
		t.Errorf("Expected response status %s, got %s.", expectedRespStatus, respStatus)
	}

	const expectedResponseMessage = "[GoogleSheets] error :: Failed to insert Payload :: context deadline exceeded"
	if responseMessage != expectedResponseMessage {
		t.Errorf("Expected response message %s, got %s.", expectedResponseMessage, responseMessage)
	}
}

type cleaner interface {
	Cleanup(func())
	Log(...interface{})
}

func SetupTestGoogleSheets(pool *dockertest.Pool, cln cleaner) (*TestConfig, error) {
	var config TestConfig
	dockerContainer, err := pool.Run("atzoum/simulator-google-sheets", "latest", []string{})
	if err != nil {
		return nil, fmt.Errorf("Could not start resource: %s", err)
	}
	cln.Cleanup(func() {
		if err := pool.Purge(dockerContainer); err != nil {
			cln.Log(fmt.Errorf("could not purge resource: %v", err))
		}
	})
	config.Endpoint = fmt.Sprintf("https://127.0.0.1:%s/", dockerContainer.GetPort("8443/tcp"))
	config.AccessToken = "cd887efc-7c7d-4e8e-9580-f7502123badf"
	config.RefreshToken = "bdbbe5ec-6081-4c6c-8974-9c4abfc0fdcc"

	token := &oauth2.Token{
		AccessToken:  config.AccessToken,
		RefreshToken: config.RefreshToken,
	}
	// skipcq: GO-S1020
	tlsConfig := &tls.Config{
		// skipcq: GSC-G402
		InsecureSkipVerify: true,
	}
	client := oauth2.NewClient(context.Background(), oauth2.StaticTokenSource(token))
	trans := client.Transport.(*oauth2.Transport)
	trans.Base = &http.Transport{TLSClientConfig: tlsConfig}
	sheetService, err := sheets.NewService(context.Background(), option.WithEndpoint(config.Endpoint), option.WithHTTPClient(client))

	if err := pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, err = sheetService.Spreadsheets.Get("sheetId").Context(ctx).Do()
		return err
	}); err != nil {
		return nil, fmt.Errorf("Could not connect to Google sheets service")
	}
	return &config, nil
}

package bqstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/googleutils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
	gbq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"
)

type Config struct {
	Credentials string `json:"credentials"`
	ProjectId   string `json:"projectId"`
	DatasetId   string `json:"datasetId"`
	TableId     string `json:"tableId"`
}

type Client struct {
	bqClient *bigquery.Client
	opts     common.Opts
}

// https://stackoverflow.com/questions/55951812/insert-into-bigquery-without-a-well-defined-struct
type genericRecord map[string]bigquery.Value

func (rec genericRecord) Save() (map[string]bigquery.Value, string, error) {
	var insertID string
	if columnVal, isInsertIdPresent := rec["insertId"]; isInsertIdPresent {
		insertID = columnVal.(string)
		delete(rec, "insertId")
	}
	return rec, insertID, nil
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("bqstream")
}

type BQStreamProducer struct {
	client *Client
}

func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (*BQStreamProducer, error) {
	var config Config
	jsonConfig, err := json.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("[BQStream] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return nil, createErr(err, "error in BQStream while unmarshalling destination config")
	}
	opts := []option.ClientOption{
		option.WithScopes([]string{
			gbq.BigqueryInsertdataScope,
		}...),
	}
	if !googleutils.ShouldSkipCredentialsInit(config.Credentials) {
		confCreds := []byte(config.Credentials)
		if err = googleutils.CompatibleGoogleCredentialsJSON(confCreds); err != nil {
			return nil, createErr(err, "incompatible credentials")
		}
		opts = append(opts, option.WithCredentialsJSON(confCreds))
	}
	bqClient, err := bigquery.NewClient(context.Background(), config.ProjectId, opts...)
	if err != nil {
		return nil, err
	}
	return &BQStreamProducer{client: &Client{bqClient: bqClient, opts: o}}, nil
}

func (producer *BQStreamProducer) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessage string) {
	client := producer.client
	bqClient := client.bqClient
	o := client.opts
	parsedJSON := gjson.ParseBytes(jsonData)
	dsId := parsedJSON.Get("datasetId").String()
	tblId := parsedJSON.Get("tableId").String()
	props := parsedJSON.Get("properties").String()

	var genericRec *genericRecord
	err := json.Unmarshal([]byte(props), &genericRec)
	if err != nil {
		return http.StatusBadRequest, "Failure", createErr(err, "error in unmarshalling data").Error()
	}
	bqInserter := bqClient.Dataset(dsId).Table(tblId).Inserter()
	ctx, cancel := context.WithTimeout(context.Background(), o.Timeout)
	defer cancel()
	err = bqInserter.Put(ctx, genericRec)

	if err != nil {
		if ctx.Err() != nil && errors.Is(err, context.DeadlineExceeded) {
			return http.StatusGatewayTimeout, "Failure", createErr(err, "timeout in data insertion").Error()
		}
		return http.StatusBadRequest, "Failure", createErr(err, "error in data insertion").Error()
	}

	return http.StatusOK, "Success", `[BQStream] Successful insertion of data`
}

func (producer *BQStreamProducer) Close() error {
	client := producer.client
	if client == nil {
		return createErr(nil, "error while trying to close the client")
	}
	bqClient := client.bqClient

	err := bqClient.Close()
	if err != nil {
		return createErr(err, "error while closing the client")
	}
	return err
}

func createErr(err error, msg string) error {
	fmtMsg := fmt.Errorf("[BQStream] error :: %v:: %w", msg, err).Error()
	pkgLogger.Errorf(fmtMsg)
	return fmt.Errorf(fmtMsg)
}

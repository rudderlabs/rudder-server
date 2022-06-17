package bqstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/bigquery"
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

type Credentials struct {
	Email      string `json:"client_email"`
	PrivateKey string `json:"private_key"`
	TokenUrl   string `json:"token_uri"`
}

type Client struct {
	bqClient *bigquery.Client
	opts     Opts
}
type Opts struct {
	Timeout time.Duration
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

func NewProducer(destinationConfig interface{}, o Opts) (*Client, error) {
	var config Config
	var credentialsFile Credentials
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf("[BQStream] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return nil, createErr(err, "error in BQStream while unmarshalling destination config")
	}
	var confCreds []byte
	if config.Credentials == "" {
		return nil, createErr(err, "credentials not being sent")
	}
	if err = googleutils.CompatibleGoogleCredentialsJSON([]byte(config.Credentials)); err != nil {
		return nil, createErr(err, "incompatible credentials")
	}
	confCreds = []byte(config.Credentials)
	err = json.Unmarshal(confCreds, &credentialsFile)
	if err != nil {
		return nil, createErr(err, "error in BQStream while unmarshalling credentials json")
	}
	opts := []option.ClientOption{
		option.WithCredentialsJSON([]byte(config.Credentials)),
		option.WithScopes([]string{
			gbq.BigqueryInsertdataScope,
		}...),
	}
	bqClient, err := bigquery.NewClient(context.Background(), config.ProjectId, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{bqClient: bqClient, opts: o}, nil
}

func Produce(jsonData json.RawMessage, producer, destConfig interface{}) (statusCode int, respStatus, responseMessage string) {
	client := producer.(*Client)
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

func CloseProducer(producer interface{}) error {
	client, ok := producer.(*Client)
	if !ok {
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

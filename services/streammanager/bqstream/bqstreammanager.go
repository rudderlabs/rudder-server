package bqstream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"
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

func NewProducer(destinationConfig interface{}) (*bigquery.Client, error) {
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
		return nil, createErr(err, "Credentials not being sent")
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
	return bigquery.NewClient(context.Background(), config.ProjectId, opts...)
}

func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (statusCode int, respStatus string, responseMessage string) {

	bqClient := producer.(*bigquery.Client)
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

	err = bqInserter.Put(context.Background(), genericRec)
	if err != nil {
		return http.StatusBadRequest, "Failure", createErr(err, "error in data insertion").Error()
	}

	return http.StatusOK, "Success", `[BQStream] Successful insertion of data`
}

func CloseProducer(producer interface{}) error {
	bqClient, ok := producer.(*bigquery.Client)
	if !ok {
		return createErr(nil, "error while trying to close the client")
	}
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

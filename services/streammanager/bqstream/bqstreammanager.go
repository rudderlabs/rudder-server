//go:generate mockgen -destination=../../../mocks/services/streammanager/bqstream/mock_bqstream.go -package mock_bqstream github.com/rudderlabs/rudder-server/services/streammanager/bqstream BQClient

package bqstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"cloud.google.com/go/bigquery"

	"github.com/tidwall/gjson"
	gbq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/option"

	"github.com/rudderlabs/rudder-go-kit/googleutil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

type Config struct {
	Credentials string `json:"credentials"`
	ProjectId   string `json:"projectId"`
	DatasetId   string `json:"datasetId"`
	TableId     string `json:"tableId"`
}

// https://stackoverflow.com/questions/55951812/insert-into-bigquery-without-a-well-defined-struct
type GenericRecord map[string]bigquery.Value

type BQClient interface {
	Put(ctx context.Context, datasetID, tableID string, records []*GenericRecord) error
	Close() error
}

type BQStreamProducer struct {
	Opts   common.Opts
	Client BQClient
}

type Client struct {
	bqClient *bigquery.Client
}

func (c *Client) Put(ctx context.Context, datasetID, tableID string, records []*GenericRecord) error {
	bqInserter := c.bqClient.Dataset(datasetID).Table(tableID).Inserter()
	return bqInserter.Put(ctx, records)
}

func (c *Client) Close() error {
	return c.bqClient.Close()
}

func (rec GenericRecord) Save() (map[string]bigquery.Value, string, error) {
	var insertID string
	if columnVal, isInsertIdPresent := rec["insertId"]; isInsertIdPresent {
		insertID = columnVal.(string)
		delete(rec, "insertId")
	}
	return rec, insertID, nil
}

var pkgLogger logger.Logger

func Init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("bqstream")
}

func init() {
	Init()
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
	if !googleutil.ShouldSkipCredentialsInit(config.Credentials) {
		confCreds := []byte(config.Credentials)
		if err = googleutil.CompatibleGoogleCredentialsJSON(confCreds); err != nil {
			return nil, createErr(err, "incompatible credentials")
		}
		opts = append(opts, option.WithCredentialsJSON(confCreds))
	}
	bqClient, err := bigquery.NewClient(context.Background(), config.ProjectId, opts...)
	if err != nil {
		return nil, err
	}
	return &BQStreamProducer{Client: &Client{bqClient: bqClient}, Opts: o}, nil
}

func (producer *BQStreamProducer) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessage string) {
	client := producer.Client
	if client == nil {
		return http.StatusBadRequest, "Failure", "[BQStream] error :: invalid client"
	}
	parsedJSON := gjson.ParseBytes(jsonData)
	dsId := parsedJSON.Get("datasetId").String()
	tblId := parsedJSON.Get("tableId").String()
	props := parsedJSON.Get("properties")

	var genericRecs []*GenericRecord
	if props.IsArray() {
		err := json.Unmarshal([]byte(props.String()), &genericRecs)
		if err != nil {
			return http.StatusBadRequest, "Failure", createErr(err, "error in unmarshalling data").Error()
		}
	} else {
		var genericRec *GenericRecord
		err := json.Unmarshal([]byte(props.String()), &genericRec)
		if err != nil {
			return http.StatusBadRequest, "Failure", createErr(err, "error in unmarshalling data").Error()
		}
		genericRecs = append(genericRecs, genericRec)
	}

	ctx, cancel := context.WithTimeout(context.Background(), producer.Opts.Timeout)
	defer cancel()
	err := client.Put(ctx, dsId, tblId, genericRecs)
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, context.DeadlineExceeded) {
			return http.StatusGatewayTimeout, "Failure", createErr(err, "timeout in data insertion").Error()
		}
		return http.StatusBadRequest, "Failure", createErr(err, "error in data insertion").Error()
	}

	return http.StatusOK, "Success", `[BQStream] Successful insertion of data`
}

func (producer *BQStreamProducer) Close() error {
	client := producer.Client
	if client == nil {
		return createErr(nil, "error while trying to close the client")
	}

	err := client.Close()
	if err != nil {
		return createErr(err, "error while closing the client")
	}
	return err
}

func createErr(err error, msg string) error {
	fmtMsg := fmt.Errorf("[BQStream] error :: %v:: %w", msg, err).Error()
	pkgLogger.Errorf(fmtMsg)
	return errors.New(fmtMsg)
}

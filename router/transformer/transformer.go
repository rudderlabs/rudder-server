package transformer

//go:generate mockgen -destination=../../mocks/router/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/router/transformer Transformer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/router/types"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/tidwall/gjson"
)

const (
	BATCH            = "BATCH"
	ROUTER_TRANSFORM = "ROUTER_TRANSFORM"
)

//HandleT is the handle for this class
type HandleT struct {
	tr                        *http.Transport
	client                    *http.Client
	transformRequestTimerStat stats.RudderStats
	logger                    logger.LoggerI
	features                  features
}

//Transformer provides methods to transform events
type Transformer interface {
	Setup()
	Transform(transformType string, transformMessage *types.TransformMessageT) []types.DestinationJobT
}

type features interface {
	Get(string) string
}

//NewTransformer creates a new transformer
func NewTransformer() *HandleT {
	return &HandleT{}
}

var (
	maxRetry   int
	retrySleep time.Duration
	pkgLogger  logger.LoggerI
)

func loadConfig() {
	config.RegisterIntConfigVariable(30, &maxRetry, true, 1, "Processor.maxRetry")
	config.RegisterDurationConfigVariable(time.Duration(100), &retrySleep, true, time.Millisecond, []string{"Processor.retrySleep", "Processor.retrySleepInMS"}...)

}

func Init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("router").Child("transformer")
}

//GetDestinationURL returns node URL
func GetDestinationURL(destType string) string {
	destTransformURL := "" // TODO
	destinationEndPoint := fmt.Sprintf("%s/v0/%s", destTransformURL, strings.ToLower(destType))
	if misc.Contains(warehouse.WarehouseDestinations, destType) {
		whSchemaVersionQueryParam := fmt.Sprintf("whSchemaVersion=%s&whIDResolve=%v", config.GetWHSchemaVersion(), warehouseutils.IDResolutionEnabled())
		if destType == "RS" {
			rsAlterStringToTextQueryParam := fmt.Sprintf("rsAlterStringToText=%s", fmt.Sprintf("%v", config.GetVarCharMaxForRS()))
			return destinationEndPoint + "?" + whSchemaVersionQueryParam + "&" + rsAlterStringToTextQueryParam
		}
		if destType == "CLICKHOUSE" {
			enableArraySupport := fmt.Sprintf("chEnableArraySupport=%s", fmt.Sprintf("%v", config.GetArraySupportForCH()))
			return destinationEndPoint + "?" + whSchemaVersionQueryParam + "&" + enableArraySupport
		}
		return destinationEndPoint + "?" + whSchemaVersionQueryParam
	}
	return destinationEndPoint
}

//Transform transforms router jobs to destination jobs
func (trans *HandleT) Transform(transformType string, transformMessage *types.TransformMessageT) []types.DestinationJobT {
	//Call remote transformation
	rawJSON, err := json.Marshal(transformMessage)
	if err != nil {
		panic(err)
	}
	trans.logger.Debugf("[Router Transfomrer] :: input payload : %s", string(rawJSON))

	retryCount := 0
	var resp *http.Response
	var respData []byte
	//We should rarely have error communicating with our JS
	reqFailed := false

	var url string
	if transformType == BATCH {
		url = getBatchURL()
	} else if transformType == ROUTER_TRANSFORM {
		url = getRouterTransformURL()
	} else {
		//Unexpected transformType returning empty
		return []types.DestinationJobT{}
	}

	for {
		trans.transformRequestTimerStat.Start()
		resp, err = trans.client.Post(url, "application/json; charset=utf-8",
			bytes.NewBuffer(rawJSON))

		if err == nil {
			//If no err returned by client.Post, reading body.
			//If reading body fails, retrying.
			respData, err = io.ReadAll(resp.Body)
		}

		if err != nil {
			trans.transformRequestTimerStat.End()
			reqFailed = true
			trans.logger.Errorf("JS HTTP connection error: URL: %v Error: %+v", url, err)
			if retryCount > maxRetry {
				panic(fmt.Errorf("JS HTTP connection error: URL: %v Error: %+v", url, err))
			}
			retryCount++
			time.Sleep(retrySleep)
			//Refresh the connection
			continue
		}
		if reqFailed {
			trans.logger.Errorf("Failed request succeeded after %v retries, URL: %v", retryCount, url)
		}

		trans.transformRequestTimerStat.End()
		break
	}

	// Remove Assertion?
	if resp.StatusCode != http.StatusOK {
		trans.logger.Errorf("[Router Transfomrer] :: Transformer returned status code: %v reason: %v", resp.StatusCode, resp.Status)
	}

	var destinationJobs []types.DestinationJobT
	if resp.StatusCode == http.StatusOK {
		trans.logger.Debugf("[Router Transfomrer] :: output payload : %s", string(respData))

		if transformType == BATCH {
			err = json.Unmarshal(respData, &destinationJobs)
		} else if transformType == ROUTER_TRANSFORM {
			err = json.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &destinationJobs)
		}
		//This is returned by our JS engine so should  be parsable
		//but still handling it
		if err != nil {
			panic(err)
		}
	} else {
		statusCode := 500
		if resp.StatusCode == http.StatusNotFound {
			statusCode = 404
		}
		for _, routerJob := range transformMessage.Data {
			resp := types.DestinationJobT{Message: routerJob.Message, JobMetadataArray: []types.JobMetadataT{routerJob.JobMetadata}, Destination: routerJob.Destination, Batched: false, StatusCode: statusCode, Error: string(respData)}
			destinationJobs = append(destinationJobs, resp)
		}
	}
	resp.Body.Close()

	return destinationJobs
}

func (trans *HandleT) Setup() {
	trans.logger = pkgLogger
	trans.tr = &http.Transport{}
	trans.client = &http.Client{Transport: trans.tr}
	trans.transformRequestTimerStat = stats.NewStat("router.processor.transformer_request_time", stats.TimerType)
}

func getBatchURL() string {
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/batch"
}

func getRouterTransformURL() string {
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/routerTransform"
}

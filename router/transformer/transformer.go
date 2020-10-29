package transformer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/router/types"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

//HandleT is the handle for this class
type HandleT struct {
	tr                        *http.Transport
	client                    *http.Client
	transformRequestTimerStat stats.RudderStats
	logger                    logger.LoggerI
}

//Transformer provides methods to transform events
type Transformer interface {
	Setup()
	Transform(transformMessage *types.TransformMessageT) []types.DestinationJobT
}

//NewTransformer creates a new transformer
func NewTransformer() *HandleT {
	return &HandleT{}
}

var (
	maxChanSize, numTransformWorker, maxRetry int
	retrySleep                                time.Duration
	pkgLogger                                 logger.LoggerI
)

func loadConfig() {
	maxChanSize = config.GetInt("Processor.maxChanSize", 2048)
	numTransformWorker = config.GetInt("Processor.numTransformWorker", 8)
	maxRetry = config.GetInt("Processor.maxRetry", 30)
	retrySleep = config.GetDuration("Processor.retrySleepInMS", time.Duration(100)) * time.Millisecond
}

func init() {
	loadConfig()
	pkgLogger = logger.NewLogger().Child("routerTransformer")

}

//Transform transforms router jobs to destination jobs
func (trans *HandleT) Transform(transformMessage *types.TransformMessageT) []types.DestinationJobT {
	//Call remote transformation
	rawJSON, err := json.Marshal(transformMessage)
	if err != nil {
		panic(err)
	}
	logger.Debugf("[Router Transfomrer] :: input payload : %s", string(rawJSON))

	retryCount := 0
	var resp *http.Response
	//We should rarely have error communicating with our JS
	reqFailed := false

	for {
		url := getBatchURL("AM")
		trans.transformRequestTimerStat.Start()
		resp, err = trans.client.Post(url, "application/json; charset=utf-8",
			bytes.NewBuffer(rawJSON))
		if err != nil {
			trans.transformRequestTimerStat.End()
			reqFailed = true
			logger.Errorf("JS HTTP connection error: URL: %v Error: %+v", url, err)
			if retryCount > maxRetry {
				panic(fmt.Errorf("JS HTTP connection error: URL: %v Error: %+v", url, err))
			}
			retryCount++
			time.Sleep(retrySleep)
			//Refresh the connection
			continue
		}
		if reqFailed {
			logger.Errorf("Failed request succeeded after %v retries, URL: %v", retryCount, url)
		}

		trans.transformRequestTimerStat.End()
		break
	}

	// Remove Assertion?
	if resp.StatusCode != http.StatusOK {
		logger.Errorf("[Router Transfomrer] :: Transformer returned status code: %v reason: %v", resp.StatusCode, resp.Status)
	}

	var destinationJobs []types.DestinationJobT
	if resp.StatusCode == http.StatusOK {
		respData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		logger.Debugf("[Router Transfomrer] :: output payload : %s", string(rawJSON))
		err = json.Unmarshal(respData, &destinationJobs)
		//This is returned by our JS engine so should  be parsable
		//but still handling it
		if err != nil {
			panic(err)
		}
	} else {
		io.Copy(ioutil.Discard, resp.Body)
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

func getBatchURL(destType string) string {
	return strings.TrimSuffix(config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090"), "/") + "/batch"
}

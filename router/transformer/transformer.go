package transformer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type DestinationTransformerJobT struct {
	Message     json.RawMessage            `json:"message"`
	JobMetadata DestinationJobMetadataT    `json:"metadata"`
	Destination backendconfig.DestinationT `json:"destination"`
}

type DestinationJobT struct {
	Message          json.RawMessage            `json:"batchedRequest"`
	JobMetadataArray []DestinationJobMetadataT  `json:"metadata"`
	Destination      backendconfig.DestinationT `json:"destination"`
}

type DestinationJobMetadataT struct {
	UserID        string `json:"userId"`
	JobID         int64  `json:"jobId"`
	SourceID      string `json:"sourceId"`
	DestinationID string `json:"destinationId"`
	AttemptNum    int    `json:"attemptNum"`
	ReceivedAt    string `json:"receivedAt"`
	CreatedAt     string `json:"createdAt"`
}

//TransformMessageT is used to pass message to the transformer workers
type TransformMessageT struct {
	Data     []DestinationTransformerJobT `json:"input"`
	DestType string                       `json:"destType"`
}

//HandleT is the handle for this class
type HandleT struct {
	tr                        *http.Transport
	client                    *http.Client
	transformRequestTimerStat stats.RudderStats
}

//Transformer provides methods to transform events
type Transformer interface {
	Setup()
	Transform(transformMessage *TransformMessageT) []DestinationJobT
}

//NewTransformer creates a new transformer
func NewTransformer() *HandleT {
	return &HandleT{}
}

var (
	maxChanSize, numTransformWorker, maxRetry int
	retrySleep                                time.Duration
)

func loadConfig() {
	maxChanSize = config.GetInt("Processor.maxChanSize", 2048)
	numTransformWorker = config.GetInt("Processor.numTransformWorker", 8)
	maxRetry = config.GetInt("Processor.maxRetry", 30)
	retrySleep = config.GetDuration("Processor.retrySleepInMS", time.Duration(100)) * time.Millisecond
}

func init() {
	loadConfig()
}

func (trans *HandleT) Transform(transformMessage *TransformMessageT) []DestinationJobT {
	//Call remote transformation
	rawJSON, err := json.Marshal(transformMessage)
	if err != nil {
		panic(err)
	}

	//TODO remove
	fmt.Println("input rawjson print")
	fmt.Println(string(rawJSON))

	retryCount := 0
	var resp *http.Response
	//We should rarely have error communicating with our JS
	reqFailed := false

	for {
		url := GetBatchURL("AM")
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
	if !(resp.StatusCode == http.StatusOK ||
		resp.StatusCode == http.StatusBadRequest ||
		resp.StatusCode == http.StatusNotFound ||
		resp.StatusCode == http.StatusRequestEntityTooLarge) {
		logger.Errorf("Transformer returned status code: %v", resp.StatusCode)
	}

	var destinationJobs []DestinationJobT
	if resp.StatusCode == http.StatusOK {
		respData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(respData, &destinationJobs)
		//This is returned by our JS engine so should  be parsable
		//but still handling it
		if err != nil {
			panic(err)
		}

		//TODO remove
		fmt.Println("output jobs")
		fmt.Println(string(respData))
	} else {
		io.Copy(ioutil.Discard, resp.Body)
	}
	resp.Body.Close()

	return destinationJobs
}

func (trans *HandleT) Setup() {
	trans.tr = &http.Transport{}
	trans.client = &http.Client{Transport: trans.tr}
	trans.transformRequestTimerStat = stats.NewStat("router.processor.transformer_request_time", stats.TimerType)
}

//TODO complete this
//GetBatchURL returns node URL
func GetBatchURL(destType string) string {
	return "http://localhost:9090/batch"
	//config.GetEnv("DEST_TRANSFORM_URL", "http://localhost:9090")
}

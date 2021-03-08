package transformer

//go:generate mockgen -destination=../../mocks/processor/transformer/mock_transformer.go -package=mocks_transformer github.com/rudderlabs/rudder-server/processor/transformer Transformer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/processor/integrations"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types"
)

const (
	UserTransformerStage = "user_transformer"
	DestTransformerStage = "dest_transformer"
)
const supportedTransformerAPIVersion = 1

type MetadataT struct {
	SourceID        string `json:"sourceId"`
	DestinationID   string `json:"destinationId"`
	JobID           int64  `json:"jobId"`
	DestinationType string `json:"destinationType"`
	MessageID       string `json:"messageId"`
	// set by user_transformer to indicate transformed event is part of group indicated by messageIDs
	MessageIDs []string `json:"messageIds"`
	RudderID   string   `json:"rudderId"`
	SessionID  string   `json:"sessionId,omitempty"`
	ReceivedAt string   `json:"receivedAt"`
}

type TransformerEventT struct {
	Message     types.SingularEventT       `json:"message"`
	Metadata    MetadataT                  `json:"metadata"`
	Destination backendconfig.DestinationT `json:"destination"`
	SessionID   string                     `json:"session_id,omitempty"`
	Libraries   []backendconfig.LibraryT   `json:"libraries"`
}

//transformMessageT is used to pass message to the transformer workers
type transformMessageT struct {
	index int
	data  []TransformerEventT
	url   string
}

type transformedMessageT struct {
	index int
	data  []TransformerResponseT
}

//HandleT is the handle for this class
type HandleT struct {
	requestQ           chan *transformMessageT
	responseQ          chan *transformedMessageT
	accessLock         sync.Mutex
	perfStats          *misc.PerfStats
	sentStat           stats.RudderStats
	receivedStat       stats.RudderStats
	failedStat         stats.RudderStats
	transformTimerStat stats.RudderStats
	logger             logger.LoggerI
}

//Transformer provides methods to transform events
type Transformer interface {
	Setup()
	Transform(clientEvents []TransformerEventT, url string, batchSize int, breakIntoBatchWhenUserChanges bool) ResponseT
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
	pkgLogger = logger.NewLogger().Child("processor").Child("transformer")
}

type TransformerResponseT struct {
	// Not marking this Singular Event, since this not a RudderEvent
	Output     map[string]interface{} `json:"output"`
	Metadata   MetadataT              `json:"metadata"`
	StatusCode int                    `json:"statusCode"`
	Error      string                 `json:"error"`
}

func (trans *HandleT) transformWorker() {
	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	transformRequestTimerStat := stats.NewStat("processor.transformer_request_time", stats.TimerType)

	for job := range trans.requestQ {
		//Call remote transformation
		rawJSON, err := json.Marshal(job.data)
		if err != nil {
			panic(err)
		}
		retryCount := 0
		var resp *http.Response
		//We should rarely have error communicating with our JS
		reqFailed := false

		for {
			transformRequestTimerStat.Start()
			resp, err = client.Post(job.url, "application/json; charset=utf-8",
				bytes.NewBuffer(rawJSON))
			if err != nil {
				transformRequestTimerStat.End()
				reqFailed = true
				trans.logger.Errorf("JS HTTP connection error: URL: %v Error: %+v", job.url, err)
				if retryCount > maxRetry {
					panic(fmt.Errorf("JS HTTP connection error: URL: %v Error: %+v", job.url, err))
				}
				retryCount++
				time.Sleep(retrySleep)
				//Refresh the connection
				continue
			}
			if reqFailed {
				trans.logger.Errorf("Failed request succeeded after %v retries, URL: %v", retryCount, job.url)
			}

			// perform version compatability check only on success
			if resp.StatusCode == http.StatusOK {
				transformerAPIVersion, convErr := strconv.Atoi(resp.Header.Get("apiVersion"))
				if convErr != nil {
					transformerAPIVersion = 0
				}
				if supportedTransformerAPIVersion != transformerAPIVersion {
					trans.logger.Errorf("Incompatible transformer version: Expected: %d Received: %d, URL: %v", supportedTransformerAPIVersion, transformerAPIVersion, job.url)
					panic(fmt.Errorf("Incompatible transformer version: Expected: %d Received: %d, URL: %v", supportedTransformerAPIVersion, transformerAPIVersion, job.url))
				}
			}

			transformRequestTimerStat.End()
			break
		}

		// Remove Assertion?
		if !(resp.StatusCode == http.StatusOK ||
			resp.StatusCode == http.StatusBadRequest ||
			resp.StatusCode == http.StatusNotFound ||
			resp.StatusCode == http.StatusRequestEntityTooLarge) {
			trans.logger.Errorf("Transformer returned status code: %v", resp.StatusCode)
		}

		var transformerResponses []TransformerResponseT
		respData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		if resp.StatusCode == http.StatusOK {
			err = json.Unmarshal(respData, &transformerResponses)
			//This is returned by our JS engine so should  be parsable
			//but still handling it
			if err != nil {
				panic(err)
			}
		} else {
			for _, transformEvent := range job.data {
				resp := TransformerResponseT{StatusCode: resp.StatusCode, Error: string(respData), Metadata: transformEvent.Metadata}
				transformerResponses = append(transformerResponses, resp)
			}
		}
		resp.Body.Close()

		trans.responseQ <- &transformedMessageT{data: transformerResponses, index: job.index}
	}
}

//Setup initializes this class
func (trans *HandleT) Setup() {
	trans.logger = pkgLogger
	trans.requestQ = make(chan *transformMessageT, maxChanSize)
	trans.responseQ = make(chan *transformedMessageT, maxChanSize)
	trans.sentStat = stats.NewStat("processor.transformer_sent", stats.CountType)
	trans.receivedStat = stats.NewStat("processor.transformer_received", stats.CountType)
	trans.failedStat = stats.NewStat("processor.transformer_failed", stats.CountType)
	trans.transformTimerStat = stats.NewStat("processor.transformation_time", stats.TimerType)
	trans.perfStats = &misc.PerfStats{}
	trans.perfStats.Setup("JS Call")
	for i := 0; i < numTransformWorker; i++ {
		trans.logger.Info("Starting transformer worker", i)
		rruntime.Go(func() {
			trans.transformWorker()
		})
	}
}

//ResponseT represents a Transformer response
type ResponseT struct {
	Events       []TransformerResponseT
	FailedEvents []TransformerResponseT
}

//GetVersion gets the transformer version by asking it on /transfomerBuildVersion. if there is any error it returns empty string
func GetVersion() (transformerBuildVersion string) {
	transformerBuildVersion = "Not an official release. Get the latest release from dockerhub."
	url := integrations.GetTransformerURL() + "/transformerBuildVersion"
	resp, err := http.Get(url)
	if err != nil {
		pkgLogger.Errorf("Unable to make a transfomer build version call with error : %s", err.Error())
		return

	}
	if resp == nil {
		transformerBuildVersion = fmt.Sprintf("No response from transformer. %s", transformerBuildVersion)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			pkgLogger.Errorf("Unable to read response into bytes with error : %s", err.Error())
			transformerBuildVersion = "Unable to read response from transformer."
			return
		}
		transformerBuildVersion = string(bodyBytes)
	}
	return
}

//Transform function is used to invoke transformer API
//Transformer is not thread safe. If performance becomes
//an issue we can create multiple transformer instances
//but given that they are hitting the same NodeJS
//process it may not be an issue if batch sizes (len clientEvents)
//are big enough to saturate NodeJS. Right now the transformer
//instance is shared between both user specific transformation
//code and destination transformation code.
func (trans *HandleT) Transform(clientEvents []TransformerEventT,
	url string, batchSize int, breakIntoBatchWhenUserChanges bool) ResponseT {

	trans.accessLock.Lock()
	defer trans.accessLock.Unlock()

	trans.transformTimerStat.Start()

	var transformResponse = make([]*transformedMessageT, 0)
	//Enqueue all the jobs
	inputIdx := 0
	outputIdx := 0
	totalSent := 0
	reqQ := trans.requestQ
	resQ := trans.responseQ

	trans.perfStats.Start()
	var toSendData []TransformerEventT

	for {
		//The channel is still live and the last batch has been sent
		//Construct the next batch
		if reqQ != nil && toSendData == nil {
			clientBatch := make([]TransformerEventT, 0)
			batchCount := 0
			for {
				if (batchCount >= batchSize || inputIdx >= len(clientEvents)) && inputIdx != 0 {
					// If processSessions is false or if dest transformer is being called, break using just the batchSize.
					// Otherwise break when userId changes. This makes sure all events of a session go together as a batch
					if !breakIntoBatchWhenUserChanges || inputIdx >= len(clientEvents) {
						break
					}
					prevUserID, ok := misc.GetRudderID(clientEvents[inputIdx-1].Message)
					if !ok {
						panic(fmt.Errorf("GetRudderID failed"))
					}
					currentUserID, ok := misc.GetRudderID(clientEvents[inputIdx].Message)
					if !ok {
						panic(fmt.Errorf("GetRudderID failed"))
					}
					if currentUserID != prevUserID {
						trans.logger.Debug("Breaking batch at", inputIdx, prevUserID, currentUserID)
						break
					}
				}
				if inputIdx >= len(clientEvents) {
					break
				}
				clientBatch = append(clientBatch, clientEvents[inputIdx])
				batchCount++
				inputIdx++
			}
			toSendData = clientBatch
			trans.sentStat.Count(len(clientBatch))
		}

		select {
		//In case of batch event, index is the next Index
		case reqQ <- &transformMessageT{index: inputIdx, data: toSendData, url: url}:
			totalSent++
			toSendData = nil
			if inputIdx == len(clientEvents) {
				reqQ = nil
			}
		case data := <-resQ:
			transformResponse = append(transformResponse, data)
			outputIdx++
			//If all was sent and all was received we are done
			if reqQ == nil && outputIdx == totalSent {
				resQ = nil
			}
		}
		if reqQ == nil && resQ == nil {
			break
		}
	}
	if !(inputIdx == len(clientEvents) && outputIdx == totalSent) {
		panic(fmt.Errorf("inputIdx:%d != len(clientEvents):%d or outputIdx:%d != totalSent:%d", inputIdx, len(clientEvents), outputIdx, totalSent))
	}

	//Sort the responses in the same order as input
	sort.Slice(transformResponse, func(i, j int) bool {
		return transformResponse[i].index < transformResponse[j].index
	})

	//Some sanity checks
	if !(batchSize > 0 || transformResponse[0].index == 1) {
		panic(fmt.Errorf("batchSize:%d <= 0 and transformResponse[0].index:%d != 1", batchSize, transformResponse[0].index))
	}
	if transformResponse[len(transformResponse)-1].index != len(clientEvents) {
		panic(fmt.Errorf("transformResponse[len(transformResponse)-1].index:%d != len(clientEvents):%d", transformResponse[len(transformResponse)-1].index, len(clientEvents)))
	}

	var outClientEvents []TransformerResponseT
	var failedEvents []TransformerResponseT

	for _, resp := range transformResponse {
		if resp.data == nil {
			continue
		}
		respArray := resp.data

		//Transform is one to many mapping so returned
		//response for each is an array. We flatten it out
		for _, transformerResponse := range respArray {
			if transformerResponse.StatusCode != 200 {
				failedEvents = append(failedEvents, transformerResponse)
				continue
			}
			outClientEvents = append(outClientEvents, transformerResponse)
		}

	}

	trans.receivedStat.Count(len(outClientEvents))
	trans.failedStat.Count(len(failedEvents))
	trans.perfStats.End(len(clientEvents))
	trans.perfStats.Print()

	trans.transformTimerStat.End()

	return ResponseT{
		Events:       outClientEvents,
		FailedEvents: failedEvents,
	}
}

package processor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

//Structure which is used to pass message to the transformer workers
type transformMessageT struct {
	index int
	data  interface{}
	url   string
}

//HandleT is the handle for this class
type transformerHandleT struct {
	requestQ           chan *transformMessageT
	responseQ          chan *transformMessageT
	accessLock         sync.Mutex
	perfStats          *misc.PerfStats
	sentStat           *stats.RudderStats
	receivedStat       *stats.RudderStats
	failedStat         *stats.RudderStats
	transformTimerStat *stats.RudderStats
}

var (
	maxChanSize, numTransformWorker, maxRetry int
	retrySleep                                time.Duration
)

func (trans *transformerHandleT) transformWorker() {
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
				logger.Errorf("JS HTTP connection error: URL: %v Error: %+v", job.url, err)
				if retryCount > maxRetry {
					panic(fmt.Errorf("JS HTTP connection error: URL: %v Error: %+v", job.url, err))
				}
				retryCount++
				time.Sleep(retrySleep)
				//Refresh the connection
				continue
			}
			if reqFailed {
				logger.Errorf("Failed request succeeded after %v retries, URL: %v", retryCount, job.url)
			}
			transformRequestTimerStat.End()
			break
		}

		// Remove Assertion?
		if !(resp.StatusCode == http.StatusOK ||
			resp.StatusCode == http.StatusBadRequest ||
			resp.StatusCode == http.StatusNotFound ||
			resp.StatusCode == http.StatusRequestEntityTooLarge) {
			logger.Errorf("Transformer returned status code: %v", resp.StatusCode)
		}

		var toSendData interface{}
		if resp.StatusCode == http.StatusOK {
			respData, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				panic(err)
			}
			err = json.Unmarshal(respData, &toSendData)
			//This is returned by our JS engine so should  be parsable
			//but still handling it
			if err != nil {
				panic(err)
			}
		} else {
			io.Copy(ioutil.Discard, resp.Body)
		}
		resp.Body.Close()

		trans.responseQ <- &transformMessageT{data: toSendData, index: job.index}
	}
}

//Setup initializes this class
func (trans *transformerHandleT) Setup() {
	trans.requestQ = make(chan *transformMessageT, maxChanSize)
	trans.responseQ = make(chan *transformMessageT, maxChanSize)
	trans.sentStat = stats.NewStat("processor.transformer_sent", stats.CountType)
	trans.receivedStat = stats.NewStat("processor.transformer_received", stats.CountType)
	trans.failedStat = stats.NewStat("processor.transformer_failed", stats.CountType)
	trans.transformTimerStat = stats.NewStat("processor.transformation_time", stats.TimerType)
	trans.perfStats = &misc.PerfStats{}
	trans.perfStats.Setup("JS Call")
	for i := 0; i < numTransformWorker; i++ {
		logger.Info("Starting transformer worker", i)
		rruntime.Go(func() {
			trans.transformWorker()
		})
	}
}

type ResponseT struct {
	Events  []interface{}
	Success bool
}

//Transform function is used to invoke transformer API
//Transformer is not thread safe. If performance becomes
//an issue we can create multiple transformer instances
//but given that they are hitting the same NodeJS
//process it may not be an issue if batch sizes (len clientEvents)
//are big enough to saturate NodeJS. Right now the transformer
//instance is shared between both user specific transformation
//code and destination transformation code.
func (trans *transformerHandleT) Transform(clientEvents []interface{},
	url string, batchSize int, breakIntoBatchWhenUserChanges bool) ResponseT {

	trans.accessLock.Lock()
	defer trans.accessLock.Unlock()

	trans.transformTimerStat.Start()

	var transformResponse = make([]*transformMessageT, 0)
	//Enqueue all the jobs
	inputIdx := 0
	outputIdx := 0
	totalSent := 0
	reqQ := trans.requestQ
	resQ := trans.responseQ

	trans.perfStats.Start()
	var toSendData interface{}

	for {
		//The channel is still live and the last batch has been sent
		//Construct the next batch
		if reqQ != nil && toSendData == nil {
			clientBatch := make([]interface{}, 0)
			batchCount := 0
			for {
				if (batchCount >= batchSize || inputIdx >= len(clientEvents)) && inputIdx != 0 {
					// If processSessions is false or if dest transformer is being called, break using just the batchSize.
					// Otherwise break when userId changes. This makes sure all events of a session go together as a batch
					if !breakIntoBatchWhenUserChanges || inputIdx >= len(clientEvents) {
						break
					}
					prevUserID, ok := misc.GetAnonymousID(clientEvents[inputIdx-1].(map[string]interface{})["message"])
					if !ok {
						panic(fmt.Errorf("GetAnonymousID failed"))
					}
					currentUserID, ok := misc.GetAnonymousID(clientEvents[inputIdx].(map[string]interface{})["message"])
					if !ok {
						panic(fmt.Errorf("GetAnonymousID failed"))
					}
					if currentUserID != prevUserID {
						logger.Debug("Breaking batch at", inputIdx, prevUserID, currentUserID)
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

	outClientEvents := make([]interface{}, 0)

	for _, resp := range transformResponse {
		if resp.data == nil {
			continue
		}
		respArray, ok := resp.data.([]interface{})
		if !ok {
			panic(fmt.Errorf("typecast of resp.data to []interface{} failed"))
		}
		//Transform is one to many mapping so returned
		//response for each is an array. We flatten it out
		for _, respElem := range respArray {
			respElemMap, castOk := respElem.(map[string]interface{})
			if castOk {
				respOutput, ok := respElemMap["output"]
				if !ok {
					trans.failedStat.Increment()
					continue
				}
				if output, castOk := respOutput.(map[string]interface{}); castOk {
					if statusCode, ok := output["statusCode"]; ok && fmt.Sprintf("%v", statusCode) == "400" {
						// TODO: Log errored resposnes to file
						trans.failedStat.Increment()
						continue
					}
				}
			}
			outClientEvents = append(outClientEvents, respElem)
		}

	}

	trans.receivedStat.Count(len(outClientEvents))
	trans.perfStats.End(len(clientEvents))
	trans.perfStats.Print()

	trans.transformTimerStat.End()

	return ResponseT{
		Events:  outClientEvents,
		Success: true,
	}
}

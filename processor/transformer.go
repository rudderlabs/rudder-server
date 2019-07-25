package processor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"time"
	"sync"
	"github.com/rudderlabs/rudder-server/misc"
)

//Structure which is used to pass message to the transformer workers
type transformMessageT struct {
	index int
	data  json.RawMessage
	url   string
}

//HandleT is the handle for this class
type transformerHandleT struct {
	requestQ  chan *transformMessageT
	responseQ chan *transformMessageT
	accessLock sync.Mutex	
	perfStats *misc.PerfStats
}



var (
	maxChanSize, numTransformWorker, maxRetry int
	retrySleep                                time.Duration
)


func (trans *transformerHandleT) transformWorker() {
	for job := range trans.requestQ {
		//Call remote transformation
		postData := new(bytes.Buffer)
		json.NewEncoder(postData).Encode(job.data)

		retryCount := 0
		var resp *http.Response
		var err error
		//We should rarely have error communicating with our JS
		for {
			resp, err = http.Post(job.url, "application/json; charset=utf-8",
				postData)
			if err != nil {
				log.Println("JS HTTP connection error", err)
				fmt.Println("JS HTTP connection error", err)
				if retryCount > maxRetry {
					misc.Assert(false)
				}
				retryCount++
				time.Sleep(retrySleep)
				continue
			}
			break
		}
		defer resp.Body.Close()

		misc.Assert(resp.StatusCode == http.StatusOK ||
			resp.StatusCode == http.StatusBadRequest)

		var respData json.RawMessage

		if resp.StatusCode == http.StatusOK {
			respData, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				respData = nil
			}
		}

		trans.responseQ <- &transformMessageT{data: respData, index: job.index}
	}
}

//Setup initializes this class
func (trans *transformerHandleT) Setup() {
	loadConfig()
	trans.requestQ = make(chan *transformMessageT, maxChanSize)
	trans.responseQ = make(chan *transformMessageT, maxChanSize)
	trans.perfStats = &misc.PerfStats{}
	trans.perfStats.Setup("JS Call")
	for i := 0; i < numTransformWorker; i++ {
		fmt.Println("Starting transformer worker", i)
		go trans.transformWorker()
	}
}

//Transform function is used to invoke transformer API
//Transformer is not thread. If performance becomes
//an issue we can create multiple transformer instances
//but given that they are hitting the same NodeJS
//process it may not be an issue if batch sizes (len clientEvents)
//are big enough to saturate NodeJS. Right now the transformer
//instance is shared between both user specific transformation
//code and destination transformation code. 
func (trans *transformerHandleT) Transform(clientEvents []interface{},
	url string, oneToMany bool) ([]interface{}, bool) {


	trans.accessLock.Lock()
	defer trans.accessLock.Unlock()
	
	var transformResponse = make([]*transformMessageT, 0)
	//Enqueue all the jobs
	inputIdx := 0
	outputIdx := 0
	reqQ := trans.requestQ
	resQ := trans.responseQ

	trans.perfStats.Start()

	for {
		var rawJSON json.RawMessage
		if reqQ != nil {
			rawJSON, _ = json.Marshal(clientEvents[inputIdx])
		}
		select {
		case reqQ <- &transformMessageT{index: inputIdx, data: rawJSON, url: url}:
			inputIdx++
			if inputIdx == len(clientEvents) {
				reqQ = nil
			}
		case data := <-resQ:
			transformResponse = append(transformResponse, data)
			outputIdx++
			if outputIdx == len(clientEvents) {
				resQ = nil
			}
		}
		if reqQ == nil && resQ == nil {
			break
		}
	}
	misc.Assert(inputIdx == len(clientEvents) && outputIdx == len(clientEvents))

	//Sort the responses in the same order as input
	sort.Slice(transformResponse, func(i, j int) bool {
		return transformResponse[i].index < transformResponse[j].index
	})

	//Some sanity checks
	misc.Assert(transformResponse[0].index == 0)
	misc.Assert(transformResponse[len(transformResponse)-1].index == len(clientEvents)-1)

	outClientEvents := make([]interface{}, 0)
	
	for _, resp := range transformResponse {
		var respObj interface{}
		//Bad JSON
		if resp.data == nil {
			continue
		}
		err := json.Unmarshal(resp.data, &respObj)
		//This is returned by our JS engine so should  be parsable
		//but still handling it
		if err != nil {
			continue
		}
		if oneToMany {
			respArray := respObj.([]interface{})
			//Transform is one to many mapping so returned
			//response for each is an array. We flatten it out
			for _, respElem := range respArray {
				outClientEvents = append(outClientEvents, respElem)
			}
		} else {
			//One to one mapping so no flattening is
			//required
			outClientEvents = append(outClientEvents, respObj)
		}
	}
	trans.perfStats.End(len(clientEvents))
	trans.perfStats.Print()

	return outClientEvents, true
}

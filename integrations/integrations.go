package integrations

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/spf13/viper"

	"github.com/rudderlabs/rudder-server/integrations/google"
	"github.com/rudderlabs/rudder-server/misc"
)

//Structure which is used to pass message to the transformer workers
type transformMessageT struct {
	index int
	data  json.RawMessage
	dest  string
}

//HandleT is the handle for this class
type HandleT struct {
	requestQ  chan *transformMessageT
	responseQ chan *transformMessageT
	perfStats *misc.PerfStats
}

//DestTransformer interface provides a generic interface which
//every destination implements to transform the JSON
type DestTransformer interface {
	Transform(clientEvent *interface{}) (interface{}, bool)
}

//destNameIDMap is the mapping between the names provided in integration field
//our internal ID for that destination. We save this ID in the customval field
//in JobsDB
var destNameIDMap = map[string]string{
	"google_analytics": "GA",
	"rudderlabs":       "GA",
}

//destTransformerMap keeps a mapping between the destinationID and the corresponding
//transformation function. The functions are in integration folder
var destTransformerMap = map[string]DestTransformer{
	"GA": google.HandleT{},
}

//destJSTransformerMap keeps a mapping between the destinationID and
//the NodeJS URL end point where the transformation function is hosted
var destJSTransformerMap = map[string]string{
	"GA": "http://localhost:9090",
}

const (
	//PostDataKV means post data is sent as KV
	PostDataKV = iota + 1
	//PostDataJSON means post data is sent as JSON
	PostDataJSON
	//PostDataXML means post data is sent as XML
	PostDataXML
)

//PostParameterT  has post related parameters, the URL and the data type
type PostParameterT struct {
	URL     string
	Payload int //PostDataKV or PostDataJSON or PostDataXML
}

//destPostURLMap keeps mapping between destinationID and the post URL
var destPostInfoMap = map[string]PostParameterT{
	"GA": PostParameterT{URL: "https://www.google-analytics.com/collect", Payload: PostDataKV},
}

//GetPostInfo provides the post parameters for this destination
func GetPostInfo(destID string) PostParameterT {
	postInfo, ok := destPostInfoMap[destID]
	misc.Assert(ok)
	return postInfo
}

//GetDestinationIDs parses the destination names from the
//input JSON and returns the IDSs
func GetDestinationIDs(clientEvent interface{}) (retVal []string) {
	clientIntgs, ok := misc.GetRudderEventVal("rl_integrations", clientEvent)
	if !ok {
		return
	}

	clientIntgsList, ok := clientIntgs.([]interface{})
	if !ok {
		return
	}
	var outVal []string
	for _, integ := range clientIntgsList {
		customVal, ok := destNameIDMap[strings.ToLower(integ.(string))]
		if ok {
			outVal = append(outVal, customVal)
		}
	}
	retVal = outVal
	return
}

//Transform calls the right destination specific transform function
func (integ *HandleT) Transform(clientEvent []*interface{}, destID string) ([]*interface{}, bool) {
	destHandle, ok := destTransformerMap[destID]
	if !ok {
		return nil, false
	}
	outEvents := make([]*interface{}, 0)
	for _, event := range clientEvent {
		respEvent, ok := destHandle.Transform(event)
		misc.Assert(ok)
		outEvents = append(outEvents, &respEvent)
	}
	return outEvents, true
}

var (
	maxChanSize, numTransformWorker, maxRetry int
	retrySleep                                time.Duration
)

func loadConfig() {
	maxChanSize = viper.GetInt("Integrations.maxChanSize")
	numTransformWorker = viper.GetInt("Integrations.numTransformWorker")
	maxRetry = viper.GetInt("Integrations.maxRetry")
	retrySleep = viper.GetDuration("Integrations.retrySleepInMS") * time.Millisecond
}

func (integ *HandleT) transformWorker() {
	for job := range integ.requestQ {
		//Call remote transformation
		postData := new(bytes.Buffer)
		json.NewEncoder(postData).Encode(job.data)

		//Get the transform URL
		transformURL, ok := destJSTransformerMap[job.dest]
		misc.Assert(ok)

		retryCount := 0
		var resp *http.Response
		var err error
		//We should rarely have error communicating with our JS
		for {
			resp, err = http.Post(transformURL, "application/json; charset=utf-8", postData)
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

		//misc.Assert(resp.StatusCode == http.StatusOK or resp.StatusCode == http.StatusBadRequest)

		var respData json.RawMessage

		if resp.StatusCode == http.StatusOK {
			respData, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				respData = nil
			}
		}

		integ.responseQ <- &transformMessageT{data: respData, index: job.index}
	}
}

//Setup initializes this class
func (integ *HandleT) Setup() {
	loadConfig()
	integ.requestQ = make(chan *transformMessageT, maxChanSize)
	integ.responseQ = make(chan *transformMessageT, maxChanSize)
	integ.perfStats = &misc.PerfStats{}
	integ.perfStats.Setup("JS Call")
	for i := 0; i < numTransformWorker; i++ {
		fmt.Println("Starting transformer worker", i)
		go integ.transformWorker()
	}
}

//TransformJS function is used to invoke transformer API
func (integ *HandleT) TransformJS(clientEvents []interface{}, destID string) ([]interface{}, bool) {

	//Get the transform URL
	_, ok := destJSTransformerMap[destID]
	if !ok {
		return nil, true
	}

	var transformResponse = make([]*transformMessageT, 0)

	//Enqueue all the jobs
	inputIdx := 0
	outputIdx := 0
	reqQ := integ.requestQ
	resQ := integ.responseQ

	integ.perfStats.Start()

	for {
		var rawJSON json.RawMessage
		if reqQ != nil {
			rawJSON, _ = json.Marshal(clientEvents[inputIdx])
		}
		select {
		case reqQ <- &transformMessageT{index: inputIdx, data: rawJSON, dest: destID}:
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
	//Each element of the response is an array.
	for _, resp := range transformResponse {
		var respArray []interface{}
		//Bad JSON
		if resp.data == nil {
			continue
		}
		err := json.Unmarshal(resp.data, &respArray)
		//This is returned by our JS engine so should  be parsable
		//but still handling it
		if err != nil {
			continue
		}
		for _, respElem := range respArray {
			outClientEvents = append(outClientEvents, respElem)
		}
	}
	integ.perfStats.End(len(clientEvents))
	integ.perfStats.Print()

	return outClientEvents, true
}

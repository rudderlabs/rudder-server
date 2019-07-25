package misc

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"time"
)

//AssertError panics if error
func AssertError(err error) {
	if err != nil {
		debug.SetTraceback("all")
		debug.PrintStack()
		panic(err)
	}
}

//Assert panics if false
func Assert(cond bool) {
	if !cond {
		debug.SetTraceback("all")
		debug.PrintStack()
		panic("Assertion failed")
	}
}

//GetRudderEventMap returns the event structure from the client payload
func GetRudderEventMap(rudderEvent interface{}) (map[string]interface{}, bool) {

	rudderEventMap, ok := rudderEvent.(map[string]interface{})
	if !ok {
		return nil, false
	}
	rudderMsg, ok := rudderEventMap["rl_message"]
	if !ok {
		return nil, false
	}
	rudderMsgMap, ok := rudderMsg.(map[string]interface{})
	if !ok {
		return nil, false
	}
	return rudderMsgMap, true
}

//GetRudderEventVal returns the value corresponding to the key in the message structure
func GetRudderEventVal(key string, rudderEvent interface{}) (interface{}, bool) {

	rudderMsgMap, ok := GetRudderEventMap(rudderEvent)
	if !ok {
		return nil, false
	}
	rudderVal, ok := rudderMsgMap[key]
	if !ok {
		return nil, false
	}
	return rudderVal, true
}

//ParseRudderEventBatch looks for the batch structure inside event
func ParseRudderEventBatch(eventPayload json.RawMessage) ([]interface{}, bool) {
	var eventListJSON map[string]interface{}
	err := json.Unmarshal(eventPayload, &eventListJSON)
	if err != nil {
		return nil, false
	}
	_, ok := eventListJSON["batch"]
	if !ok {
		return nil, false
	}
	eventListJSONBatchType, ok := eventListJSON["batch"].([]interface{})
	if !ok {
		return nil, false
	}
	return eventListJSONBatchType, true
}
//Return the UserID from the object
func GetRudderEventUserID(eventPayload json.RawMessage) (string, bool) {
	eventList, ok := ParseRudderEventBatch(eventPayload)
	if !ok || len(eventList) == 0 {
		return "", false
	}
	userID, ok := GetRudderEventVal("rl_anonymous_id", eventList[0])
	if !ok {
		return "", false
	}
	userIDStr, ok := userID.(string)
	return userIDStr, true
}

//PerfStats is the class for managing performance stats. Not multi-threaded safe now
type PerfStats struct {
	eventCount           int64
	elapsedTime          time.Duration
	lastPrintEventCount  int64
	lastPrintElapsedTime time.Duration
	lastPrintTime        time.Time
	compStr              string
	tmpStart             time.Time
	instantRateCall      float64
	printThres           int
}

//Setup initializes the stat collector
func (stats *PerfStats) Setup(comp string) {
	stats.compStr = comp
	stats.lastPrintTime = time.Now()
	stats.printThres = 5 //seconds
}

//Start marks the start of event collection
func (stats *PerfStats) Start() {
	stats.tmpStart = time.Now()
}

//End marks the end of one round of stat collection. events is number of events processed since start
func (stats *PerfStats) End(events int) {
	elapsed := time.Since(stats.tmpStart)
	stats.elapsedTime += elapsed
	stats.eventCount += int64(events)
	stats.instantRateCall = float64(events) * float64(time.Second) / float64(elapsed)
}

//Print displays the stats
func (stats *PerfStats) Print() {
	if time.Since(stats.lastPrintTime) > time.Duration(stats.printThres)*time.Second {
		overallRate := float64(stats.eventCount) * float64(time.Second) / float64(stats.elapsedTime)
		instantRate := float64(stats.eventCount-stats.lastPrintEventCount) * float64(time.Second) / float64(stats.elapsedTime-stats.lastPrintElapsedTime)
		fmt.Printf("%s: Total: %d Overall:%f, Instant(print):%f, Instant(call):%f\n",
			stats.compStr, stats.eventCount, overallRate, instantRate, stats.instantRateCall)
		stats.lastPrintEventCount = stats.eventCount
		stats.lastPrintElapsedTime = stats.elapsedTime
		stats.lastPrintTime = time.Now()
	}
}

// SetupLogger setup the logger with configs
func SetupLogger() {
	//Enable logging
	log.SetPrefix("LOG: ")
	log.SetFlags(log.Ldate | log.Lmicroseconds | log.Llongfile)
	log.Println("Setup Called")
	f, err := os.OpenFile("runtime.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	AssertError(err)
	defer f.Close()
	log.SetOutput(f)
}

package main

import (
	//"encoding/json"

	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	isBatchPath      = "batch"
	eventsPath       = "events"
	eventMappingPath = "events.#.mapping"
	rudderJSONPath   = "events.#.rudder"
	gaJSONPath       = "events.#.GA"
	variations       = 5
	serverIP         = "http://localhost:8080/v1/batch"
	// serverIP = "http://172.31.94.69:8080/hello"
)

var (
	successCount uint64
	failCount    uint64
	writeKey     *string
)

var done chan bool
var numberOfEventPtr *int
var badJSON *bool
var badJSONRate *int
var dupEventsRate *int

var loadStat stats.RudderStats
var requestTimeStat stats.RudderStats

func main() {
	stats.Setup()

	loadStat = stats.NewStat("genload.num_events", stats.CountType)
	requestTimeStat = stats.NewStat("genload.event_time", stats.TimerType)

	done = make(chan bool)

	numberOfUsers := flag.Int("nu", 1, "number of user threads that does the send, default is 1")
	numberOfEventPtr = flag.Int("n", 1, "number of events in a batch, default is 1")
	eventPtr := flag.String("event", "Track", "give the event name you want the jobs for, default is track")
	numberOfIterPtr := flag.Int("ni", -1, "number of iterations, default is infinite")
	sendToRuderPtr := flag.Bool("rudder", true, "true/false for sending to rudder BE, default true")
	// below flags are to send bad json req's to gateway
	// setting badjson rate 0f 60 sends ~60% (approx since we just compare with rand number) req's with bad json
	badJSON = flag.Bool("badjson", false, "true/false for sending malformed json as payload to rudder BE")
	badJSONRate = flag.Int("badjsonRate", 100, "percentage of malformed json sent as events")
	dupEventsRate = flag.Int("dupEventsRate", 0, "percentage of events sent with same message")
	writeKey = flag.String("writekey", "", "Writekey to be sent along with the event")

	flag.Parse()

	go printStats()

	for i := 1; i <= *numberOfUsers; i++ {
		id := uuid.NewV4()
		if *numberOfEventPtr == 1 {
			go generateJobsForSameEvent(id.String(), *eventPtr, *numberOfIterPtr, *sendToRuderPtr)
		} else {
			go generateJobsForMulitpleEvent(id.String(), *numberOfIterPtr, *sendToRuderPtr)
		}
	}

	for i := 1; i <= *numberOfUsers; i++ {
		<-done
	}

	fmt.Println("Total Sent", successCount)
}

func toSendGoodJSON() bool {
	toSendGoodJSON := true
	if *badJSON && (*badJSONRate == 100 || (*badJSONRate > rand.Intn(100))) {
		toSendGoodJSON = false
	}
	return toSendGoodJSON
}

func toSendDuplicateMessageID() bool {
	toSendDuplicateMessageID := false
	if *dupEventsRate > 0 && (*dupEventsRate == 100 || (*dupEventsRate > rand.Intn(100))) {
		toSendDuplicateMessageID = true
	}
	return toSendDuplicateMessageID
}

func sendBadJSON(lines []string, rudder bool) {
	value, _ := sjson.Set("", "batch", "random_string_to_be_replaced")
	value, _ = sjson.Set(value, "sent_at", time.Now())
	if rudder {
		value = strings.Replace(value, "random_string_to_be_replaced", fmt.Sprintf("[%s]", strings.Join(lines[:], ",")), 1)
		sendToRudder(value)
	}
}

func generateJobsForSameEvent(uid string, eventName string, count int, rudder bool) {
	////fmt.Println("event name input: ", eventName)
	var err error
	var data []byte
	var rudderEvents []map[string]interface{}
	var unmarshalleRudderdData map[string]interface{}
	data, err = ioutil.ReadFile("mapping.json")
	if err != nil {
		panic(err)
	}

	result := gjson.GetBytes(data, isBatchPath)

	isBatchToBeMade := result.Bool()
	////fmt.Println(isBatchToBeMade)

	events := gjson.GetBytes(data, eventsPath).Array()

	lines, err := misc.ReadLines("badJsonStrings.txt")
	if err != nil {
		panic(err)
	}
	var duplicateIds []string
	for index := 0; index < 10; index++ {
		duplicateIds = append(duplicateIds, uuid.NewV4().String())
	}
	countLoop := 0

	for _, event := range events {
		unmarshalleRudderdData = nil
		eventMap := event.Map()
		////fmt.Println(eventMap["name"])

		if eventMap["name"].Value() != eventName {
			continue
		}
		mapping := eventMap["mapping"].Map()
		rudderJSON := eventMap["rudder"]

		rudderData := []byte(rudderJSON.Raw)

		var userIDpath string

		for {
			if count > 0 && countLoop >= count {
				break
			}

			if toSendGoodJSON() {
				for k, _ := range mapping {
					////fmt.Printf("key %v, val %v \n", k, v.Value())

					if strings.Contains(k, "anonymousId") {
						userIDpath = k
					}

					// Use this to generate random data for rudder-stack
					//rudderData, err = sjson.SetBytes(rudderData, k, "abc")
					//if err != nil {
					//	panic(err)
					//}
					rudderData = generateData(&rudderData, k, gjson.Get(rudderJSON.Raw, k).Value())
				}
				eventTypes := []string{"track", "page", "screen"}
				rudderData = generateRandomDataFromSlice(eventTypes, &rudderData, "type", "track")
				eventNames := []string{"Homepage visited", "User signed up", "Product added to cart", "Product added to wishlist"}
				rudderData = generateRandomDataFromSlice(eventNames, &rudderData, "event", "track")
				rudderData, _ = sjson.SetBytes(rudderData, "originalTimestamp", time.Now().Add(-5*time.Second).Format(misc.RFC3339Milli))
				rudderData, _ = sjson.SetBytes(rudderData, "sentAt", time.Now().Format(misc.RFC3339Milli))

				rudderData, err = sjson.SetBytes(rudderData, userIDpath, uid)
				rudderData, err = sjson.SetBytes(rudderData, "anonymousId", uid)
				var messageID string
				if countLoop < 10 {
					messageID = duplicateIds[countLoop]
				} else if toSendDuplicateMessageID() {
					messageID = duplicateIds[rand.Intn(10)]
				} else {
					messageID = uuid.NewV4().String()
				}
				rudderData, err = sjson.SetBytes(rudderData, "messageId", messageID)
				if err != nil {
					panic(err)
				}

				// Unmarshal
				err = json.Unmarshal(rudderData, &unmarshalleRudderdData)
				if err != nil {
					panic(err)
				}

				//append to list to be send to rudder-stack
				rudderEvents = append(rudderEvents, unmarshalleRudderdData)

				if isBatchToBeMade {
					value, _ := sjson.Set("", "batch", rudderEvents)
					value, _ = sjson.Set(value, "sent_at", time.Now())
					////fmt.Println("==================")
					////fmt.Println(value)
					////fmt.Println("iter : ", countLoop)
					//Push the value as json to rudder-stack
					if rudder {
						sendToRudder(value)
					}

				}
				rudderEvents = nil
			} else {
				sendBadJSON(lines, rudder)
			}
			countLoop++

		}
	}
	done <- true

}

func generateJobsForMulitpleEvent(uid string, count int, rudder bool) {
	var err error
	var data []byte
	var rudderEvents []map[string]interface{}
	var unmarshalleRudderdData map[string]interface{}
	data, err = ioutil.ReadFile("mapping.json")
	if err != nil {
		panic(err)
	}

	result := gjson.GetBytes(data, isBatchPath)

	isBatchToBeMade := result.Bool()

	events := gjson.GetBytes(data, eventsPath).Array()

	lines, err := misc.ReadLines("badJsonStrings.txt")
	if err != nil {
		panic(err)
	}
	countLoop := 0

	var userIDpath string

	for {
		if count > 0 && countLoop >= count {
			break
		}

		if toSendGoodJSON() {
			eventsPerBatchCount := 0
			for {
				if eventsPerBatchCount >= *numberOfEventPtr {
					break
				}
				for _, event := range events {
					if eventsPerBatchCount >= *numberOfEventPtr {
						break
					}
					unmarshalleRudderdData = nil
					eventMap := event.Map()
					////fmt.Println(eventMap["name"])

					/* if eventMap["name"].Value() != eventName {
						continue
					} */
					mapping := eventMap["mapping"].Map()
					rudderJSON := eventMap["rudder"]

					rudderData := []byte(rudderJSON.Raw)

					for k, _ := range mapping {
						////fmt.Printf("key %v, val %v \n", k, v.Value())

						if strings.Contains(k, "anonymousId") {
							userIDpath = k
						}

						rudderData = generateData(&rudderData, k, gjson.Get(rudderJSON.Raw, k).Value())

					}
					rudderData, err = sjson.SetBytes(rudderData, userIDpath, uid)
					if err != nil {
						panic(err)
					}

					err = json.Unmarshal(rudderData, &unmarshalleRudderdData)
					if err != nil {
						panic(err)
					}

					rudderEvents = append(rudderEvents, unmarshalleRudderdData)

					eventsPerBatchCount++
				}
			}

			// Unmarshal

			if isBatchToBeMade {
				value, _ := sjson.Set("", "batch", rudderEvents)
				value, _ = sjson.Set(value, "sent_at", time.Now())
				////fmt.Println("==================")
				////fmt.Println(value)

				//Push the value as json to rudder-stack
				if rudder {
					sendToRudder(value)
				}

			}
			rudderEvents = nil
		} else {
			sendBadJSON(lines, rudder)
		}
		countLoop++
	}
	done <- true
}

// Uses the randomSlice only when type of value is string
func generateRandomDataFromSlice(randomSlice []string, payload *[]byte, path string, value interface{}) []byte {
	var err error
	switch value.(type) {
	case int:
		*payload, err = sjson.SetBytes(*payload, path, rand.Intn(100))
		if err != nil {
			panic(err)
		}

	case float64:
		*payload, err = sjson.SetBytes(*payload, path, math.Round(rand.Float64()+5))
		if err != nil {
			panic(err)
		}

	default:
		i := rand.Intn(len(randomSlice))
		*payload, err = sjson.SetBytes(*payload, path, randomSlice[i])
		if err != nil {
			panic(err)
		}

	}

	return *payload
}

func generateData(payload *[]byte, path string, value interface{}) []byte {
	randStr := []string{"abc", "efg", "ijk", "lmn", "opq"}
	return generateRandomDataFromSlice(randStr, payload, path, value)
}

func printStats() {
	for {
		time.Sleep(5 * time.Second)
		fmt.Println("Success/Fail", successCount, failCount)
	}
}
func sendToRudder(jsonPayload string) {
	loadStat.Increment()

	requestTimeStat.Start()
	req, err := http.NewRequest("POST", serverIP, bytes.NewBuffer([]byte(jsonPayload)))
	req.SetBasicAuth(*writeKey, "")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	requestTimeStat.End()
	if err != nil {
		atomic.AddUint64(&failCount, 1)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		atomic.AddUint64(&failCount, 1)
		return
	}
	// fmt.Println("response Status:", resp.Status)
	// fmt.Println("response Headers:", resp.Header)
	ioutil.ReadAll(resp.Body)
	// body, _ := ioutil.ReadAll(resp.Body)
	// fmt.Println("response Body:", string(body))
	atomic.AddUint64(&successCount, 1)
}

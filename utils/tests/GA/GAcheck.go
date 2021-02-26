package main

import (
	//"encoding/json"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/utils/logger"

	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	isBatchPath           = "batch"
	integrationsPath      = "integrations"
	eventsPath            = "events"
	eventMappingPath      = "events.#.mapping"
	rudderJSONPath        = "events.#.rudder"
	gaJSONPath            = "events.#.GA"
	rudderIntegrationPath = "message.integrations"
	variations            = 5
)

var (
	gaReferenceMap []byte
	sendToDest     map[string]bool
	destNameIDMap  = map[string]string{
		"GA": "google_analytics",
		//"rudderlabs":       "GA",
		"AM": "google_analytics",
	}
	pkgLogger logger.LoggerI
)

func init() {
	pkgLogger = logger.NewLogger().Child("tests")
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	fmt.Println("starting..")
	done := make(chan bool)
	sendToDest = make(map[string]bool)
	numberOfUsers := flag.Int("nu", 1, "number of user threads that does the send, default is 1")
	numberOfEventPtr := flag.String("n", "one", "number of events in a batch, default is one")
	eventPtr := flag.String("event", "Track", "give the event name you want the jobs for, default is track")
	numberOfIterPtr := flag.Int("ni", -1, "number of iterations, default is infinite")
	sendToRuderPtr := flag.Bool("rudder", false, "true/false for sending to rudder BE, default true")
	sendToDestPtr := flag.String("in", "GA", "list of dests to send, space separated, default GA")

	flag.Parse()

	fmt.Println("numberOfUsers", *numberOfUsers)
	fmt.Println("numberOfEventPtr", *numberOfEventPtr)
	fmt.Println("eventPtr", *eventPtr)
	fmt.Println("numberOfIterPtr", *numberOfIterPtr)
	fmt.Println("sendToRuderPtr", *sendToRuderPtr)
	fmt.Println("sendToDestPtr", *sendToDestPtr)

	destinations := strings.Split(*sendToDestPtr, " ")

	for _, dest := range destinations {
		sendToDest[dest] = true
	}

	fmt.Println(*sendToRuderPtr)

	gaReferenceMap, _ = ioutil.ReadFile("GAReference.json")

	for i := 1; i <= *numberOfUsers; i++ {
		id := uuid.NewV4()
		if *numberOfEventPtr == "one" {
			go generateJobsForSameEvent(id.String(), *eventPtr, *numberOfIterPtr, *sendToRuderPtr)
		} else {
			go generateJobsForMulitpleEvent(id.String(), *numberOfIterPtr, *sendToRuderPtr)
		}
	}

	<-done

}

func generateJobsForSameEvent(uid string, eventName string, count int, rudder bool) {
	fmt.Println("event name input: ", eventName)
	var err error
	var data, gaJSONData []byte
	var rudderEvents []map[string]interface{}
	var unmarshalleRudderdData, unmarshalleGAData map[string]interface{}
	data, err = ioutil.ReadFile("mapping.json")
	check(err)
	//fmt.Println(string(data))

	result := gjson.GetBytes(data, isBatchPath)
	resIntegration := gjson.GetBytes(data, integrationsPath)

	integrations := resIntegration.Array()
	isBatchToBeMade := result.Bool()
	//fmt.Println(isBatchToBeMade)

	events := gjson.GetBytes(data, eventsPath).Array()
	var countLoop int

	for _, event := range events {
		eventMap := event.Map()
		//fmt.Println(eventMap["name"])

		if eventMap["name"].Value() != eventName {
			continue
		}

		for _, itgr := range integrations {
			fmt.Println("building for destination: ", itgr.Value())
			countLoop = 0

			mappingPath := itgr.Value().(string) + "Mapping"

			mapping := eventMap[mappingPath].Map()

			netMapping := eventMap[itgr.Value().(string)+"Net"].Map()

			rudderJSON := eventMap["rudder"]
			gaJSON := eventMap[itgr.Value().(string)]

			rudderData := []byte(rudderJSON.Raw)
			gaJSONData = []byte(gaJSON.Raw)

			var userIDpath, gauserIDpath string

			for {
				if count > 0 && countLoop >= count {
					break
				}
				unmarshalleRudderdData = nil
				unmarshalleGAData = nil

				for k, v := range mapping {
					//fmt.Printf("key %v, val %v \n", k, v.Value())

					if strings.Contains(k, "anonymous_id") {
						userIDpath = k
						gauserIDpath = v.Value().(string)
					}

					// Use this to generate random data for rudder-stack
					//rudderData, err = sjson.SetBytes(rudderData, k, "abc")
					//check(err)
					rudderData = generateData(&rudderData, k, gjson.Get(rudderJSON.Raw, k).Value())

					//gaJSONData, err = sjson.SetBytes(gaJSONData, v.Value().(string), gjson.Get(rudderJSON.Raw, k).Value())
					// build the json to be pushed to GA, with the updated/changed rudderdata
					//fmt.Println("substituting ", v.Value().(string), gjson.Get(string(rudderData), k).Value())
					gaJSONData, err = sjson.SetBytes(gaJSONData, v.Value().(string), gjson.Get(string(rudderData), k).Value())
					check(err)
				}

				rudderData, err = sjson.SetBytes(rudderData, userIDpath, uid)
				check(err)
				rudderData, err = sjson.SetBytes(rudderData, rudderIntegrationPath, []string{destNameIDMap[itgr.Value().(string)]})
				check(err)

				//gaJSONData, err = sjson.SetBytes(gaJSONData, v.Value().(string), gjson.Get(rudderJSON.Raw, k).Value())
				// build the json to be pushed to GA, with the updated/changed rudderdata
				gaJSONData, err = sjson.SetBytes(gaJSONData, gauserIDpath, uid)
				check(err)

				err = json.Unmarshal(gaJSONData, &unmarshalleGAData)
				if err != nil {
					pkgLogger.Errorf("Error while unmarshalling GA JSON data : %v", err)
				}
				check(err)

				//fmt.Println(unmarshalleGAData)
				//fmt.Println("sendToDest ", sendToDest[itgr.Value().(string)], "netMapping", netMapping["type"].Value())
				if sendToDest[itgr.Value().(string)] {
					if netMapping["type"].Value() == "KV" {
						sendToGA(&unmarshalleGAData, netMapping["url"].Value().(string))
					}

				}

				// Unmarshal
				err = json.Unmarshal(rudderData, &unmarshalleRudderdData)
				check(err)

				//append to list to be send to rudder-stack
				rudderEvents = append(rudderEvents, unmarshalleRudderdData)

				if isBatchToBeMade {
					value, _ := sjson.Set("", "batch", rudderEvents)
					value, _ = sjson.Set(value, "sent_at", time.Now())
					fmt.Println("==================")
					//fmt.Println(value)
					//fmt.Println("iter : ", countLoop)
					//Push the value as json to rudder-stack
					if rudder {
						sendToRudder(value)
					}

				}
				rudderEvents = nil
				countLoop++

			}
		}

	}

}

func generateJobsForMulitpleEvent(uid string, count int, rudder bool) {
	var err error
	var data, gaJSONData []byte
	var rudderEvents []map[string]interface{}
	var unmarshalleRudderdData, unmarshalleGAData map[string]interface{}
	data, err = ioutil.ReadFile("mapping.json")
	check(err)
	//fmt.Println(string(data))

	result := gjson.GetBytes(data, isBatchPath)

	resIntegration := gjson.GetBytes(data, integrationsPath)

	integrations := resIntegration.Array()

	isBatchToBeMade := result.Bool()
	fmt.Println(isBatchToBeMade)

	events := gjson.GetBytes(data, eventsPath).Array()
	var countLoop int

	var userIDpath, gauserIDpath string

	for _, itgr := range integrations {
		fmt.Println("====starting for sink: ", itgr.Value())
		countLoop = 0
		for {
			fmt.Println("====looping ni number of times, creating batch====", countLoop)
			if count > 0 && countLoop >= count {
				break
			}

			for _, event := range events {
				unmarshalleRudderdData = nil
				unmarshalleGAData = nil
				eventMap := event.Map()
				fmt.Println(eventMap["name"])

				/* if eventMap["name"].Value() != eventName {
					continue
				} */

				mappingPath := itgr.Value().(string) + "Mapping"
				mapping := eventMap[mappingPath].Map()

				netMapping := eventMap[itgr.Value().(string)+"Net"].Map()

				rudderJSON := eventMap["rudder"]
				gaJSON := eventMap[itgr.Value().(string)]

				rudderData := []byte(rudderJSON.Raw)
				gaJSONData = []byte(gaJSON.Raw)

				for k, v := range mapping {
					//fmt.Printf("key %v, val %v \n", k, v.Value())

					if strings.Contains(k, "anonymous_id") {
						userIDpath = k
						gauserIDpath = v.Value().(string)
					}

					// Use this to generate random data for rudder-stack
					//rudderData, err = sjson.SetBytes(rudderData, k, "abc")
					//check(err)
					rudderData = generateData(&rudderData, k, gjson.Get(rudderJSON.Raw, k).Value())

					//gaJSONData, err = sjson.SetBytes(gaJSONData, v.Value().(string), gjson.Get(rudderJSON.Raw, k).Value())
					// build the json to be pushed to GA, with the updated/changed rudderdata
					gaJSONData, err = sjson.SetBytes(gaJSONData, v.Value().(string), gjson.Get(string(rudderData), k).Value())
					check(err)
				}

				rudderData, err = sjson.SetBytes(rudderData, userIDpath, uid)
				check(err)
				rudderData, err = sjson.SetBytes(rudderData, rudderIntegrationPath, []string{itgr.Value().(string)})
				check(err)

				//gaJSONData, err = sjson.SetBytes(gaJSONData, v.Value().(string), gjson.Get(rudderJSON.Raw, k).Value())
				// build the json to be pushed to GA, with the updated/changed rudderdata
				gaJSONData, err = sjson.SetBytes(gaJSONData, gauserIDpath, uid)
				check(err)

				err = json.Unmarshal(gaJSONData, &unmarshalleGAData)

				fmt.Println(unmarshalleGAData)

				fmt.Println("====trying sending to sink====")
				if sendToDest[itgr.Value().(string)] {
					if netMapping["type"].Value() == "KV" {
						sendToGA(&unmarshalleGAData, netMapping["url"].Value().(string))
					}

				}

				err = json.Unmarshal(rudderData, &unmarshalleRudderdData)
				check(err)

				//append to list to be send to rudder-stack
				rudderEvents = append(rudderEvents, unmarshalleRudderdData)

			}
			// Unmarshal

			fmt.Println("===end of a batch=== trying rudder send if enabled===")

			if isBatchToBeMade {
				value, _ := sjson.Set("", "batch", rudderEvents)
				value, _ = sjson.Set(value, "sent_at", time.Now())
				fmt.Println("==================")
				//fmt.Println(value)

				//Push the value as json to rudder-stack
				if rudder {
					sendToRudder(value)
				}

			}
			rudderEvents = nil
			countLoop++

		}
	}

}

func generateData(payload *[]byte, path string, value interface{}) []byte {
	var err error
	randStr := []string{"abc", "efg", "ijk", "lmn", "opq"}
	switch value.(type) {
	case int:
		*payload, err = sjson.SetBytes(*payload, path, rand.Intn(100))
		check(err)

	case float64:
		*payload, err = sjson.SetBytes(*payload, path, math.Round(rand.Float64()+5))
		check(err)

	default:
		i := rand.Intn(len(randStr))
		*payload, err = sjson.SetBytes(*payload, path, randStr[i])
		check(err)

	}

	return *payload
}

func sendToRudder(jsonPayload string) {
	fmt.Println("sending to rudder...")
	req, err := http.NewRequest("POST", "http://localhost:8080/hello", bytes.NewBuffer([]byte(jsonPayload)))
	if err != nil {
		check(err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		check(err)
	}
	defer resp.Body.Close()

	fmt.Println("response Status:", resp.Status)
	fmt.Println("response Headers:", resp.Header)
	body, _ := ioutil.ReadAll(resp.Body)
	fmt.Println("response Body:", string(body))
}

func sendToGA(payload *map[string]interface{}, url string) {
	fmt.Println("sending to sink")
	client := http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	check(err)

	queryParams := req.URL.Query()

	for key, val := range *payload {
		var value string
		// special handling of version
		result := gjson.GetBytes(gaReferenceMap, key)

		//special handling of types requires as gjson and sjson takes all numbers as float but GA restricts on the typr
		switch result.String() {
		case "int":
			switch val.(type) {
			case int:
				value = strconv.Itoa(val.(int))
				fmt.Println("int int ", value)

			case float64:
				value = strconv.Itoa(int(val.(float64)))
			default:
				value = string(val.(string))

			}
		case "float":
			switch val.(type) {
			case int:
				value = fmt.Sprintf("%.2f", float64(val.(int)))
				fmt.Println("float int ", value)
			case float64:
				value = fmt.Sprintf("%.2f", val.(float64))
				fmt.Println("float float ", value)
			default:
				value = string(val.(string))
			}
		case "map":
			s, _ := json.Marshal(val)
			fmt.Println("map: ", string(s))
			value = string(s)
		default:
			switch val.(type) {
			case int:
				value = string(val.(int))

			case float64:
				value = fmt.Sprintf("%.2f", val.(float64))
			default:
				value = string(val.(string))
			}

		}

		if len(value) > 0 {
			queryParams.Add(key, value)
		}
	}

	req.URL.RawQuery = queryParams.Encode()

	log.Printf("url for GA: %s", req.URL.String())

	req.Header.Add("User-Agent", "RudderLabs")

	resp, err := client.Do(req)

	check(err)

	defer resp.Body.Close()
	respBody, _ := ioutil.ReadAll(resp.Body)

	fmt.Println(string(respBody))

}

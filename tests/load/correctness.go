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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
	"github.com/rudderlabs/rudder-server/misc"
	"github.com/segmentio/ksuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	eventsPath  = "events"
	serverIP    = "http://localhost:8080/hello"
	redisServer = "localhost:6379"
)

type RudderEvent map[string]interface{}

var (
	totalCount uint64
	failCount  uint64
)

var testTimeUp bool
var done chan bool
var redisChan chan []RudderEvent

const testName = "Test9"

var redisUserSet = fmt.Sprintf("%s_user_src", testName)
var redisEventSet = fmt.Sprintf("%s_event_src", testName)
var redisEventTimeHash = fmt.Sprintf("%s_event_src_timestamp", testName)

var redisDestUserSet = fmt.Sprintf("%s_user_dst", testName)
var redisDestEventSet = fmt.Sprintf("%s_event_dst", testName)
var redisDestEventTimeHash = fmt.Sprintf("%s_event_dst_timestamp", testName)

func isArraySorted(arr []string) bool {
	for i := 0; i < len(arr); i++ {
		if arr[i] > arr[i+1] {
			return false
		}
	}
	return true
}

func computeTestResults(testDuration int) {

	fmt.Println("Processing Test Results ... ")
	fmt.Println(totalCount)
	ingestionRate := totalCount / uint64(testDuration)
	fmt.Printf("Ingestion Rate: %d req/sec\n", ingestionRate)

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})

	// Verify if same set of users
	differedUsers := redisClient.SDiff(redisUserSet, redisDestUserSet).Val()
	if len(differedUsers) > 0 {
		fmt.Printf("List of differed users: %v\n", differedUsers)
	} else {
		fmt.Println("Success: Users Matched!!")
	}

	// Verify if same set of events
	differedEvents := redisClient.SDiff(redisEventSet, redisDestEventSet).Val()
	if len(differedEvents) > 0 {
		fmt.Printf("List of differed events: %v\n", differedEvents)
	} else {
		fmt.Println("Success: Events Matched!!")
	}

	//Verify if the order of the events is same - This isn't working (ksuid check failed??)
	allUsers := redisClient.SMembers(redisDestUserSet).Val()
	inOrder := true
	// for _, user := range allUsers {
	// 	userDstEventListKey := testName + user + ":dst_list"
	// 	numEvents := redisClient.LLen(userDstEventListKey).Val()
	// 	userEvents := redisClient.LRange(userDstEventListKey, 0, numEvents-1).Val()
	// 	if isArraySorted(userEvents) {
	// 		fmt.Println("User Events are not in order for the user: " + user)
	// 		fmt.Println(userEvents)
	// 		inOrder = false
	// 	}
	// }
	// if inOrder {
	// 	fmt.Println("Success: Order of all events matched")
	// }

	// Verify Order again
	inOrder = true
	for _, user := range allUsers {
		userDstEventListKey := testName + ":" + user + ":dst_list"
		numDstEvents := redisClient.LLen(userDstEventListKey).Val()

		userSrcEventListKey := testName + ":" + user + ":src_list"
		numSrcEvents := redisClient.LLen(userSrcEventListKey).Val()

		if numSrcEvents != numDstEvents {
			fmt.Printf("User: %s, Src Events: %d, Dest Events: %d \n", user, numSrcEvents, numDstEvents)
			inOrder = false
		} else {
			//TODO: Batching for larger data sets
			dstEvents := redisClient.LRange(userDstEventListKey, 0, numDstEvents-1).Val()
			srcEvents := redisClient.LRange(userSrcEventListKey, 0, numSrcEvents-1).Val()

			var i int64
			for i = 0; i < numDstEvents; i++ {
				if dstEvents[i] != srcEvents[i] {
					inOrder = false
					fmt.Printf("Did not match: index: %d, Source Event: %s, Destination event: %s", i, srcEvents[i], dstEvents[i])
					break
				}
			}
		}
	}
	if inOrder {
		fmt.Println("Success: Events Ordering Matched!!")
	} else {
		fmt.Println("Failure: Events Ordering Missed")
	}
}

func generateRandomData(payload *[]byte, path string, value interface{}) ([]byte, error) {
	var err error
	randStr := []string{"abc", "efg", "ijk", "lmn", "opq"}
	switch value.(type) {
	case int:
		*payload, err = sjson.SetBytes(*payload, path, rand.Intn(100))
	case float64:
		*payload, err = sjson.SetBytes(*payload, path, math.Round(rand.Float64()+5))
	default:
		i := rand.Intn(len(randStr))
		*payload, err = sjson.SetBytes(*payload, path, randStr[i])
	}

	return *payload, err
}

func generateEvents(userID string, eventDelay int) {
	var batchEvents []RudderEvent
	var rudderEvent RudderEvent

	var fileData, err = ioutil.ReadFile("mapping.json")
	misc.AssertError(err)
	events := gjson.GetBytes(fileData, eventsPath).Array()

	for {
		if testTimeUp {
			break
		}

		for _, event := range events {
			eventMap := event.Map()
			mapping := eventMap["mapping"].Map()
			rudderJSON := eventMap["rudder"]

			rudderData := []byte(rudderJSON.Raw)
			eventTime := time.Now().Unix()
			eventTimeStr := strconv.FormatInt(eventTime, 10)
			messageID := ksuid.New().String()

			for path := range mapping {
				if strings.Contains(path, "anonymous_id") {
					rudderData, err = sjson.SetBytes(rudderData, path, userID)
				} else if strings.Contains(path, "rl_event") {
					rudderData, err = sjson.SetBytes(rudderData, path, userID+"-"+messageID+"-"+eventTimeStr)
				} else {
					rudderData, err = generateRandomData(&rudderData, path, gjson.Get(rudderJSON.Raw, path).Value())
				}
				misc.AssertError(err)
			}

			err = json.Unmarshal(rudderData, &rudderEvent)
			misc.AssertError(err)

			rudderEvent["id"] = messageID
			rudderEvent["userID"] = userID
			rudderEvent["timeStamp"] = eventTime

			batchEvents = append(batchEvents, rudderEvent)
		}

		value, _ := sjson.Set("", "batch", batchEvents)
		value, _ = sjson.Set(value, "sent_at", time.Now())

		sendToRudder(value)

		redisChan <- batchEvents

		if eventDelay > 0 {
			time.Sleep(time.Duration(eventDelay) * time.Millisecond)
		}
	}
	done <- true
}

func sendToRudder(jsonPayload string) {
	req, err := http.NewRequest("POST", serverIP, bytes.NewBuffer([]byte(jsonPayload)))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		atomic.AddUint64(&failCount, 1)
		return
	}
	defer resp.Body.Close()

	ioutil.ReadAll(resp.Body)
	atomic.AddUint64(&totalCount, 1)
}

func redisLoop() {
	var batchTimeout = 1000 * time.Millisecond
	var newEventsAdded bool

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})

	_, err := redisClient.Ping().Result()
	if err != nil {
		panic("Failed to connect to redis server")
	}

	pipe := redisClient.Pipeline()

	for {
		select {
		case events := <-redisChan:
			for _, event := range events {
				eventID, ok := event["id"].(string)
				userID, ok := event["userID"].(string)
				if !ok {
					panic("Invalid event ID")
				}

				pipe.RPush(testName+":"+userID+":src_list", eventID)
				pipe.SAdd(redisUserSet, userID)
				pipe.SAdd(redisEventSet, eventID)
				pipe.HSet(redisEventTimeHash, eventID, event["timeStamp"])
			}
			newEventsAdded = true
		case <-time.After(batchTimeout):
			if newEventsAdded {
				pipe.Exec()
				newEventsAdded = false
			}
		}
	}
}

func main() {

	done = make(chan bool)
	redisChan = make(chan []RudderEvent)

	numUsers := *flag.Int("n", 5, "number of user threads that does the send, default is 1")
	eventDelayInMs := *flag.Int("d", 1000, "Delay between two events for a given user in Millisec")
	testDurationInSec := *flag.Int("t", 60, "Duration of the test in seconds. Set it to -1 to run forever. Default is 60 sec")
	waitTimeInSec := *flag.Int("w", 10, "Time to wait till the events are synced to sink in sec. Default is 60s")

	flag.Parse()

	go redisLoop()

	fmt.Printf("Setting up test with %d users.\n", numUsers)
	fmt.Printf("Running test for %d seconds. -1 means forever. \n", testDurationInSec)

	for i := 0; i < numUsers; i++ {
		userID := ksuid.New()
		go generateEvents(userID.String(), eventDelayInMs)
	}

	if testDurationInSec > 0 {
		time.Sleep(time.Duration(testDurationInSec) * time.Second)
		testTimeUp = true
	}

	for i := 0; i < numUsers; i++ {
		<-done
	}

	fmt.Printf("Generation complete. Waiting to %d sec let the events flow to sink...\n", waitTimeInSec)

	time.Sleep(time.Duration(waitTimeInSec) * time.Second)

	computeTestResults(testDurationInSec)
}

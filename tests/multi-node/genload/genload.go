package main

// go run genload.go -t 300

import (

	//"encoding/json"

	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
	"github.com/segmentio/ksuid"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	rudderServerDefault = "http://localhost:8080/v1/batch"
	redisServerDefault  = "localhost:6379"
	sinkServerDefault   = "http://localhost:8181/isActive"
)

type RudderEvent map[string]interface{}

var (
	totalCount   uint64
	successCount uint64
	failCount    uint64
)

var startTime time.Time

var testTimeUp bool
var done chan bool
var redisChan chan []byte

var eventsStopped bool
var eventCheckLock sync.RWMutex

var testName = os.Getenv("TEST_NAME")
var redisServer = os.Getenv("REDIS_SERVER")
var writeKey = os.Getenv("WRITE_KEY")
var dataPlaneURL = os.Getenv("DATA_PLANE_URL") + "/v1/batch"

// flag.Int("n", 10, "number of user threads that does the send, default is 1")
var numUsersStr = os.Getenv("NUM_USERS")

// flag.Int("d", 1000, "Delay between two events for a given user in Millisec")
var eventDelayInMsStr = os.Getenv("EVENT_DELAY_MSEC")

// flag.Int("t", 60, "Duration of the test in seconds. Default is 60 sec")
var testDurationInSecStr = os.Getenv("TEST_DURATION_SEC")

const (
	timeFormat = "2006-01-02T15:04:05.000Z07:00"
)

func isArraySorted(arr []string) bool {
	for i := 0; i < len(arr); i++ {
		if arr[i] > arr[i+1] {
			return false
		}
	}
	return true
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

var batchEventJson = `
{
  "batch": [
    {
      "anonymousId": "49e4bdd1c280bc00",
      "channel": "android-sdk",
      "destination_props": {
        "AF": {
          "af_uid": "1566363489499-3377330514807116178"
        }
      },
      "context": {
        "app": {
          "build": "1",
          "name": "RudderAndroidClient",
          "namespace": "com.rudderlabs.android.sdk",
          "version": "1.0"
        },
        "device": {
          "id": "49e4bdd1c280bc00",
          "manufacturer": "Google",
          "model": "Android SDK built for x86",
          "name": "generic_x86"
        },
        "locale": "en-US",
        "network": {
          "carrier": "Android"
        },
        "screen": {
          "density": 420,
          "height": 1794,
          "width": 1080
        },
        "traits": {
          "anonymousId": "49e4bdd1c280bc00"
        },
        "user_agent": "Dalvik/2.1.0 (Linux; U; Android 9; Android SDK built for x86 Build/PSR1.180720.075)"
      },
      "event": "Demo Track",
      "integrations": {
        "All": true
      },
      "properties": {
        "label": "Demo Label",
        "category": "Demo Category",
        "value": 5
      },
      "type": "track",
      "originalTimestamp": "2019-08-12T05:08:30.909Z",
      "sentAt": "2019-08-12T05:08:30.909Z"
    }
  ]
}
`

func generateEvents(userID string, eventDelay int) {

	fileData := []byte(batchEventJson)
	events := gjson.GetBytes(fileData, "batch")

	for {
		if testTimeUp {
			break
		}

		var index int
		events.ForEach(func(_, _ gjson.Result) bool {
			messageID := ksuid.New().String()
			fileData, _ = sjson.SetBytes(fileData, fmt.Sprintf(`batch.%v.anonymousId`, index), userID)
			fileData, _ = sjson.SetBytes(fileData, fmt.Sprintf(`batch.%v.messageId`, index), messageID)
			fileData, _ = sjson.SetBytes(fileData, fmt.Sprintf(`batch.%v.sentAt`, index), time.Now().Format(timeFormat))
			index++
			return true // keep iterating
		})

		success := sendToRudderGateway(fileData, userID)
		if success {
			redisChan <- fileData
		}

		if eventDelay > 0 {
			time.Sleep(time.Duration(eventDelay) * time.Millisecond)
		}
	}
	done <- true
}

func sendToRudderGateway(jsonPayload []byte, userID string) bool {
	req, err := http.NewRequest("POST", dataPlaneURL, bytes.NewBuffer([]byte(jsonPayload)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("anonymousId", userID)
	req.SetBasicAuth(writeKey, "")

	client := &http.Client{}
	resp, err := client.Do(req)
	atomic.AddUint64(&totalCount, 1)
	if err != nil {
		atomic.AddUint64(&failCount, 1)
		return false
	}
	defer resp.Body.Close()

	ioutil.ReadAll(resp.Body)
	if resp.StatusCode == 200 {
		atomic.AddUint64(&successCount, 1)
		return true
	} else {
		atomic.AddUint64(&failCount, 1)
		return false
	}
}

type BatchEvent struct {
	Batch []interface{}
}

func getRedisClient() *redis.Client {
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})

	_, err := redisClient.Ping().Result()
	fmt.Println(err)
	if err != nil {
		panic(err)
	}
	return redisClient
}

func redisLoop() {
	var batchTimeout = 1000 * time.Millisecond
	var newEventsAdded bool

	redisClient := getRedisClient()

	pipe := redisClient.Pipeline()

	var redisUserSet = fmt.Sprintf("%s_user_src", testName)
	var redisEventSet = fmt.Sprintf("%s_event_src", testName)
	var redisEventTimeHash = fmt.Sprintf("%s_event_src_timestamp", testName)

	for {
		select {
		case events := <-redisChan:
			var batchEvent BatchEvent
			err := json.Unmarshal(events, &batchEvent)
			if err != nil {
				panic(err)
			}
			for _, event := range batchEvent.Batch {
				messageID, ok := event.(map[string]interface{})["messageId"].(string)
				userID, ok := event.(map[string]interface{})["anonymousId"].(string)
				timeStamp, ok := event.(map[string]interface{})["sentAt"].(string)
				if !ok {
					panic(errors.New("Invalid event ID"))
				}

				pipe.RPush(testName+":"+userID+":src_list", messageID)
				pipe.SAdd(redisUserSet, userID)
				pipe.SAdd(redisEventSet, messageID)

				pipe.HSet(redisEventTimeHash, messageID, timeStamp)
			}
			newEventsAdded = true
		case <-time.After(batchTimeout):
			if newEventsAdded {
				_, err := pipe.Exec()
				if err != nil {
					panic(err)
				}
				newEventsAdded = false
			} else {
				// No new events added since last batchTimeout
				eventCheckLock.Lock()
				eventsStopped = true
				eventCheckLock.Unlock()
			}
		}
	}
}

func main() {

	startTime = time.Now()
	done = make(chan bool)
	redisChan = make(chan []byte)
	var err error

	numUsers := 10
	if numUsersStr != "" {
		numUsers, err = strconv.Atoi(numUsersStr)
		if err != nil {
			panic(err.Error())
		}
	}

	eventDelayInMs := 1000
	if eventDelayInMsStr != "" {
		eventDelayInMs, err = strconv.Atoi(eventDelayInMsStr)
		if err != nil {
			panic(err.Error())
		}
	}

	testDurationInSec := 60
	if testDurationInSecStr != "" {
		testDurationInSec, err = strconv.Atoi(testDurationInSecStr)
		if err != nil {
			panic(err.Error())
		}
	}

	flag.Parse()

	if writeKey == "" || dataPlaneURL == "" || testName == "" || redisServer == "" {
		panic("Missing one of required env variable: TEST_NAME, WRITE_KEY, REDIS_SERVER, DATA_PLANE_URL")
	}
	go redisLoop()

	fmt.Printf("Setting up test with %d users.\n", numUsers)
	fmt.Printf("Running test for %d seconds. \n", testDurationInSec)

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

	for {
		time.Sleep(1 * time.Second)
		eventCheckLock.RLock()
		if eventsStopped {
			break
		}
		eventCheckLock.RUnlock()
		fmt.Println("Waiting till all events are flushed to Redis...")
	}

	fmt.Println("Event Generation complete.")

	var ch chan int
	<-ch
}

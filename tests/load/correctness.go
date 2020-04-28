package main

// To run the S3 Test:
// go run correctness.go -t 300 -S3 true  -writeKey <writekey> -sourceID <sourceid> -bucketName <bucketName>

import (

	//"encoding/json"

	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
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

var writeKey *string
var sourceID *string
var isS3Test *bool
var s3Manager filemanager.S3Manager
var startTime time.Time

var testTimeUp bool
var done chan bool
var redisChan chan []byte

var testName = config.GetEnv("TEST_NAME", "TEST-default")
var redisServer = config.GetEnv("REDIS_SERVER", redisServerDefault)
var sinkServer = config.GetEnv("SINK_SERVER", sinkServerDefault)
var rudderServer = config.GetEnv("RUDDER_SERVER", rudderServerDefault)

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
	fmt.Println(totalCount, successCount, failCount)
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
			// Batching for larger data sets
			var batchSize int64 = 10000
			var currStart int64
			for i := currStart; i < (numDstEvents/batchSize)+1; i++ {
				dstEvents := redisClient.LRange(userDstEventListKey, currStart, currStart+batchSize).Val()
				srcEvents := redisClient.LRange(userSrcEventListKey, currStart, currStart+batchSize).Val()

				for j := 0; j < len(dstEvents); j++ {
					if dstEvents[j] != srcEvents[j] {
						inOrder = false
						fmt.Printf("Did not match: index: %d, Source Event: %s, Destination event: %s", i, srcEvents[j], dstEvents[j])
						break
					}
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

func getS3DestData() {
	// TODO: Handle Pagination for ListFilesWithPrefix
	s3Objects, err := s3Manager.ListFilesWithPrefix(fmt.Sprintf("rudder-logs/%s", *sourceID))
	if err != nil {
		panic(err)
	}

	sort.Slice(s3Objects, func(i, j int) bool {
		return s3Objects[i].LastModifiedTime.Before(s3Objects[j].LastModifiedTime)
	})

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})
	pipe := redisClient.Pipeline()

	for _, s3Object := range s3Objects {
		if s3Object.LastModifiedTime.Before(startTime) {
			continue
		}
		jsonPath := "/Users/srikanth/" + "s3-correctness/" + uuid.NewV4().String()
		err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
		jsonFile, err := os.Create(jsonPath)
		if err != nil {
			panic(err)
		}

		err = s3Manager.Download(jsonFile, s3Object.Key)
		if err != nil {
			panic(err)
		}
		jsonFile.Close()
		defer os.Remove(jsonPath)

		rawf, err := os.Open(jsonPath)
		reader, _ := gzip.NewReader(rawf)

		sc := bufio.NewScanner(reader)

		count := 0
		for sc.Scan() {
			lineBytes := sc.Bytes()
			eventID := gjson.GetBytes(lineBytes, "messageId").String()
			userID := gjson.GetBytes(lineBytes, "anonymousId").String()
			timeStamp := gjson.GetBytes(lineBytes, "timeStamp").String()
			pipe.RPush(testName+":"+userID+":dst_list", eventID)
			pipe.SAdd(redisDestUserSet, userID)
			pipe.SAdd(redisDestEventSet, eventID)
			pipe.HSet(redisDestEventTimeHash, eventID, timeStamp)
			if count%100 == 0 {
				pipe.Exec()
			}
			count++
		}
		pipe.Exec()
		reader.Close()
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
	var fileData, err = ioutil.ReadFile("batchEvent.json")
	if err != nil {
		panic(err)
	}
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
			fileData, _ = sjson.SetBytes(fileData, fmt.Sprintf(`batch.%v.sentAt`, index), time.Now().Format(misc.RFC3339Milli))
			index++
			return true // keep iterating
		})

		if sendToRudderGateway(fileData) {
			redisChan <- fileData
		}

		if eventDelay > 0 {
			time.Sleep(time.Duration(eventDelay) * time.Millisecond)
		}
	}
	done <- true
}

func sendToRudderGateway(jsonPayload []byte) bool {
	req, err := http.NewRequest("POST", rudderServer, bytes.NewBuffer([]byte(jsonPayload)))
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(*writeKey, "")

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

func redisLoop() {
	var batchTimeout = 1000 * time.Millisecond
	var newEventsAdded bool

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})

	redisClient.FlushAll()

	_, err := redisClient.Ping().Result()
	fmt.Println(err)
	if err != nil {
		panic(err)
	}

	pipe := redisClient.Pipeline()

	for {
		select {
		case events := <-redisChan:
			var batchEvent BatchEvent
			err := json.Unmarshal(events, &batchEvent)
			if err != nil {
				panic(err)
			}
			for _, event := range batchEvent.Batch {
				eventID, ok := event.(map[string]interface{})["messageId"].(string)
				userID, ok := event.(map[string]interface{})["anonymousId"].(string)
				timeStamp, ok := event.(map[string]interface{})["sentAt"].(string)
				if !ok {
					panic(errors.New("Invalid event ID"))
				}

				pipe.RPush(testName+":"+userID+":src_list", eventID)
				pipe.SAdd(redisUserSet, userID)
				pipe.SAdd(redisEventSet, eventID)
				pipe.HSet(redisEventTimeHash, eventID, timeStamp)
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

	startTime = time.Now()
	done = make(chan bool)
	redisChan = make(chan []byte)

	numUsers := flag.Int("n", 10, "number of user threads that does the send, default is 1")
	eventDelayInMs := flag.Int("d", 1000, "Delay between two events for a given user in Millisec")
	testDurationInSec := flag.Int("t", 60, "Duration of the test in seconds. Default is 60 sec")
	pollTimeInSec := flag.Int("p", 2, "Polling interval in sec to find if sink is inactive")
	waitTimeInSec := flag.Int("w", 600, "Max wait-time in sec waiting for sink. Default 600s")
	writeKey = flag.String("writeKey", "1RHJcwtP1PHXwmsJSG1LrBVjRTO", "Write key of source the events should be sent against")
	sourceID = flag.String("sourceID", "1RHJcypX5HCdEYe6L3PjoCU3j6A", "ID of source the events should be sent against")
	bucketName = flag.String("bucketName", "rl-s3-correctness-test", "S3 Bucket name")
	isS3Test = flag.Bool("S3", false, "Set true to test s3 destination events")

	flag.Parse()

	go redisLoop()

	fmt.Printf("Setting up test with %d users.\n", *numUsers)
	fmt.Printf("Running test for %d seconds. \n", *testDurationInSec)

	for i := 0; i < *numUsers; i++ {
		userID := ksuid.New()
		go generateEvents(userID.String(), *eventDelayInMs)
	}

	if *testDurationInSec > 0 {
		time.Sleep(time.Duration(*testDurationInSec) * time.Second)
		testTimeUp = true
	}

	for i := 0; i < *numUsers; i++ {
		<-done
	}

	fmt.Println("Event Generation complete.")

	if *isS3Test {
		s3Manager = filemanager.S3Manager{
			Bucket: *bucketName,
		}
		time.Sleep(60 * time.Second)
		fmt.Println("Fetching S3 files...")
		getS3DestData()

	} else {
		fmt.Printf("Waiting for test sink at %s...\n", sinkServer)
		var retryCount int
		for {
			time.Sleep(time.Duration(*pollTimeInSec) * time.Second)
			resp, err := http.Get(sinkServer)
			if err != nil {
				fmt.Println("Invalid Sink URL")
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if string(body) == "no" {
				break
			}
			retryCount++
			if retryCount > (*waitTimeInSec / *pollTimeInSec) {
				fmt.Println("Wait time exceeded. Exiting... ")
			}
		}
	}

	computeTestResults(*testDurationInSec)
}

package main

// To run the S3 Test:
// go run correctness.go -S3=true -sourceID=1TC7ksYbzOK0T2NVStt31B85q4R -bucketName="test-sayan-s3"

import (

	//"encoding/json"

	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-redis/redis"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/filemanager"
	"github.com/rudderlabs/rudder-server/utils/misc"
	uuid "github.com/satori/go.uuid"
	"github.com/tidwall/gjson"
)

const (
	rudderServerDefault = "http://localhost:8080/v1/batch"
	redisServerDefault  = "localhost:6379"
	sinkServerDefault   = "http://localhost:8181/isActive"
)

type RudderEvent map[string]interface{}

var (
	totalCount uint64
)

var sourceID *string
var isS3Test *bool
var s3Manager filemanager.S3Manager
var startTime time.Time

var testTimeUp bool
var redisChan chan []byte

var testName = config.GetEnv("TEST_NAME", "REDIS_TEST")
var redisServer = config.GetEnv("REDIS_SERVER", redisServerDefault)
var sinkServer = config.GetEnv("SINK_SERVER", sinkServerDefault)
var rudderServer = config.GetEnv("RUDDER_SERVER", rudderServerDefault)

var redisUserSet = fmt.Sprintf("%s_user_src", testName)
var redisEventSet = fmt.Sprintf("%s_event_src", testName)
var redisEventTimeHash = fmt.Sprintf("%s_event_src_timestamp", testName)

var redisDestUserSet = fmt.Sprintf("%s_user_dst", testName)
var redisDestEventSet = fmt.Sprintf("%s_event_dst", testName)
var redisDestEventTimeHash = fmt.Sprintf("%s_event_dst_timestamp", testName)

func computeTestResults(testDuration int) {

	fmt.Println("Processing Test Results ... ")
	ingestionRate := totalCount / uint64(testDuration)
	fmt.Printf("Ingestion Rate: %d req/sec\n", ingestionRate)

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})

	// Verify if same set of users
	srcUsers := redisClient.SMembers(redisUserSet).Val()
	fmt.Println("==src users len== ", len(srcUsers))

	dstUsers := redisClient.SMembers(redisDestUserSet).Val()
	fmt.Println("==dst users len== ", len(dstUsers))

	differedUsers := redisClient.SDiff(redisUserSet, redisDestUserSet).Val()
	if len(differedUsers) > 0 {
		fmt.Printf("List of differed users: %v\n", differedUsers)
	} else {
		fmt.Println("Success: Users Matched!!")
	}

	// Verify if same set of events
	srcEvents := redisClient.SMembers(redisEventSet).Val()
	fmt.Println("==src events len== ", len(srcEvents))

	dstEvents := redisClient.SMembers(redisDestEventSet).Val()
	fmt.Println("==dst events len== ", len(dstEvents))

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
	fmt.Println("== allUsers length== ", len(allUsers))
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
	fmt.Println("in getS3DestData...")
	s3Objects, err := s3Manager.ListFilesWithPrefix(fmt.Sprintf("rudder-logs/%s", *sourceID))
	fmt.Println("err... ", err)
	misc.AssertError(err)

	fmt.Println("==len s3Objects=== ", len(s3Objects))

	sort.Slice(s3Objects, func(i, j int) bool {
		return s3Objects[i].LastModifiedTime.Before(s3Objects[j].LastModifiedTime)
	})

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})
	pipe := redisClient.Pipeline()

	for _, s3Object := range s3Objects {
		fmt.Println("===found s3 objects===")
		/* if s3Object.LastModifiedTime.Before(startTime) {
			continue
		} */
		jsonPath := "/Users/sayanmitra/" + "s3-correctness/" + uuid.NewV4().String()
		err = os.MkdirAll(filepath.Dir(jsonPath), os.ModePerm)
		jsonFile, err := os.Create(jsonPath)
		misc.AssertError(err)

		err = s3Manager.Download(jsonFile, s3Object.Key)
		misc.AssertError(err)
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
	}
}

type BatchEvent struct {
	Batch []interface{}
}

func main() {

	startTime = time.Now()
	redisChan = make(chan []byte)

	testDurationInSec := flag.Int("t", 60, "Duration of the test in seconds. Default is 60 sec")
	pollTimeInSec := flag.Int("p", 2, "Polling interval in sec to find if sink is inactive")
	waitTimeInSec := flag.Int("w", 600, "Max wait-time in sec waiting for sink. Default 600s")
	sourceID = flag.String("sourceID", "1TC7ksYbzOK0T2NVStt31B85a7T", "ID of source the events should be sent against")
	bucketName := flag.String("bucketName", "rl-test-sayan-s3", "S3 Bucket name")
	isS3Test = flag.Bool("S3", false, "Set true to test s3 destination events")

	flag.Parse()

	fmt.Printf("Running test for %d seconds. \n", *testDurationInSec)

	fmt.Println("Event Generation complete.")

	if *isS3Test {
		fmt.Println("in s3 test...")
		s3Manager = filemanager.S3Manager{
			Bucket: *bucketName,
		}
		time.Sleep(5 * time.Second)
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

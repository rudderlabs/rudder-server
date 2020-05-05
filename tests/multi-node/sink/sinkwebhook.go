package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bugsnag/bugsnag-go"
	"github.com/go-redis/redis"
	"github.com/prometheus/common/log"
	"github.com/tidwall/gjson"
	"golang.org/x/time/rate"
)

const (
	timeFormat = "2006-01-02T15:04:05.000Z07:00"
)

var redisServer = os.Getenv("REDIS_SERVER")
var testName = os.Getenv("TEST_NAME")

var count uint64
var showPayload = false
var enableTestStats = true
var enableError = false
var redisChan chan *EventT

var burstError = false
var randomError = false
var randomErrorCodes = []int{200, 200, 200, 200, 200, 200, 200, 200, 400, 500}
var errorCounts = make(map[string]uint64)
var errorMutex sync.Mutex

var timeOfStart = time.Now()
var limitRate = 100
var limitBurst = 1000
var limiter = rate.NewLimiter(rate.Limit(limitRate), limitBurst)

func limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if limiter.Allow() == false {
			//fmt.Println("====sending 429 =====")
			http.Error(w, http.StatusText(http.StatusTooManyRequests), http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Correctness Test parameters
var batchTimeout = 1000 * time.Millisecond

// After these many empty batch timeouts, we mark the test inactive
var inactivityBatchesThreshold = 30
var isInactive int32

func handleActiveReq(rw http.ResponseWriter, req *http.Request) {
	if atomic.LoadInt32(&isInactive) == 1 {
		rw.Write([]byte("no"))
	} else {
		rw.Write([]byte("yes"))
	}
}

func countError(errType string) {
	errorMutex.Lock()
	defer errorMutex.Unlock()

	_, ok := errorCounts[errType]
	if !ok {
		errorCounts[errType] = 0
	}
	errorCounts[errType]++
}

type EventT struct {
	userID    string
	messageID string
	sentAt    string
}

func handleReq(rw http.ResponseWriter, req *http.Request) {
	if showPayload {
		requestDump, _ := httputil.DumpRequest(req, true)
		fmt.Println(string(requestDump))
	}

	atomic.AddUint64(&count, 1)
	var respMessage string
	if burstError {
		//fmt.Println("====sending 401 ======")
		http.Error(rw, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		countError("401")
		return
	}
	if randomError {
		statusCode := rand.Intn(len(randomErrorCodes))
		switch randomErrorCodes[statusCode] {
		case 200:
			//fmt.Println("====sending 200 OK=======")
			respMessage = "OK"
			countError("200")
		case 400:
			//fmt.Println("====sending 400 =======")
			http.Error(rw, http.StatusText(http.StatusBadRequest),
				http.StatusBadRequest)
			countError("400")
			return
		case 500:
			//fmt.Println("====sending 500 =======")
			http.Error(rw, http.StatusText(http.StatusInternalServerError),
				http.StatusInternalServerError)
			countError("500")
			return
		}
	}
	if !randomError && !burstError {
		countError("200-Reg")
	}

	//Reached here means no synthetic error OR error-code = 200

	if enableTestStats {
		body, _ := ioutil.ReadAll(req.Body)
		defer req.Body.Close()
		userID := gjson.GetBytes(body, "anonymousId").Str
		messageID := gjson.GetBytes(body, "messageId").Str
		sentAt := gjson.GetBytes(body, "sentAt").Str
		redisChan <- &EventT{userID: userID, messageID: messageID, sentAt: sentAt}
	}
	rw.Write([]byte(respMessage))

}

func flipErrorType() {
	for {
		//20 seconds of good run
		fmt.Println("Disabling error")
		randomError = false
		burstError = false
		<-time.After(20 * time.Second)

		//60 seconds of burst error
		fmt.Println("Enabling burst")
		burstError = true
		randomError = false
		<-time.After(60 * time.Second)

		//20 sec of good run
		fmt.Println("Disabling error")
		randomError = false
		burstError = false
		<-time.After(20 * time.Second)

		//20 seconds of random error
		fmt.Println("Enabling random error")
		randomError = true
		burstError = false
		<-time.After(20 * time.Second)

	}
}
func printCounter() {
	startTime := time.Now()
	for {
		time.Sleep(2 * time.Second)
		errorMutex.Lock()
		fmt.Println("Count", count, time.Since(startTime), errorCounts)
		errorMutex.Unlock()
	}
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
	redisClient := getRedisClient()

	redisUserSet := fmt.Sprintf("%s_user_dst", testName)
	redisEventSet := fmt.Sprintf("%s_event_dst", testName)
	redisEventTimeHash := fmt.Sprintf("%s_event_dst_timestamp", testName)

	pipe := redisClient.Pipeline()
	var eventAdded bool
	var inactiveBatchCount int

	for {
		select {
		case eventData := <-redisChan:
			userID := eventData.userID
			messageID := eventData.messageID
			eventTime, _ := time.Parse(timeFormat, eventData.sentAt)

			pipe.RPush(testName+":"+userID+":dst_list", messageID)
			pipe.SAdd(redisUserSet, userID)
			pipe.SAdd(redisEventSet, messageID)
			pipe.HSet(redisEventTimeHash, messageID, eventTime.Unix())
			eventAdded = true
			// pipe.ZAdd(redisEventTimeSortedSet, redis.Z{Score: float64(eventTime), Member: messageID})

		case <-time.After(batchTimeout):
			if eventAdded {
				_, err := pipe.Exec()
				if err != nil {
					panic(err)
				}
				atomic.StoreInt32(&isInactive, 0)
				inactiveBatchCount = 0
			} else {
				if !burstError {
					inactiveBatchCount++
					if inactiveBatchCount > inactivityBatchesThreshold {
						atomic.StoreInt32(&isInactive, 1)
					}
				}
			}
			eventAdded = false
		}
	}
}
func handleTestResults(rw http.ResponseWriter, req *http.Request) {
	test := req.URL.Query().Get("test")
	success := computeTestResults(test)
	if success {
		rw.Write([]byte("success"))
	} else {
		rw.Write([]byte("failed"))
	}
}

// Source User Set should match Dest user set
// Source Event Set should match Dest event set
// Source Events per user should match exact order of Dest Events per user

// <test_name>_user_src == Set of all source users
// <test_name>_user_dst == Set of all destination users

func computeTestResults(test string) bool {

	redisClient := getRedisClient()

	var redisSrcUserSet = fmt.Sprintf("%s_user_src", test)
	var redisSrcEventSet = fmt.Sprintf("%s_event_src", test)
	// var redisEventTimeHash = fmt.Sprintf("%s_event_src_timestamp", test)

	var redisDestUserSet = fmt.Sprintf("%s_user_dst", test)
	var redisDestEventSet = fmt.Sprintf("%s_event_dst", test)
	// var redisDestEventTimeHash = fmt.Sprintf("%s_event_dst_timestamp", test)

	// Verify if same set of users
	fmt.Println("Source User len: ", redisClient.SCard(redisSrcUserSet))
	fmt.Println("Dest User len: ", redisClient.SCard(redisDestUserSet))
	differedUsers := redisClient.SDiff(redisSrcUserSet, redisDestUserSet).Val()
	if len(differedUsers) > 0 {
		fmt.Printf("List of differed users: %v\n", differedUsers)
	} else {
		fmt.Println("Success: Users Matched!!")
	}

	// Verify if same set of events
	fmt.Println("Source Event len: ", redisClient.SCard(redisSrcEventSet))
	fmt.Println("Dest Event len: ", redisClient.SCard(redisDestEventSet))
	differedEvents := redisClient.SDiff(redisSrcEventSet, redisDestEventSet).Val()
	if len(differedEvents) > 0 {
		fmt.Printf("List of differed events: %v\n", differedEvents)
	} else {
		fmt.Println("Success: Events Matched!!")
	}

	//Verify if the order of the events is same - This isn't working (ksuid check failed??)
	allUsers := redisClient.SMembers(redisDestUserSet).Val()
	inOrder := true
	// for _, user := range allUsers {
	// 	userDstEventListKey := test + user + ":dst_list"
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
		userDstEventListKey := test + ":" + user + ":dst_list"
		numDstEvents := redisClient.LLen(userDstEventListKey).Val()

		userSrcEventListKey := test + ":" + user + ":src_list"
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
		return true
	}
	fmt.Println("Failure: Events Ordering Missed")
	return false
}

func main() {
	fmt.Println("Starting test sink server")

	// fmt.Println(config.GetInt("SinkServer.rate", 100), config.GetInt("SinkServer.burst", 1000))
	redisChan = make(chan *EventT)

	if enableError {
		go flipErrorType()
	}

	go printCounter()

	if enableTestStats {
		if len(redisServer) == 0 || len(testName) == 0 {
			panic(errors.New("REDIS_SERVER or TEST_NAME variables can't be empty"))
		}

		go redisLoop()
		http.HandleFunc("/testResult", handleTestResults)
	}

	http.HandleFunc("/", handleReq)
	log.Fatal(http.ListenAndServe(":8181", bugsnag.Handler(nil)))
}

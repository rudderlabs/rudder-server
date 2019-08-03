package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/misc"
	"golang.org/x/time/rate"
)

const (
	redisServerDefault = "localhost:6379"
)

var redisServer = config.GetEnv("REDIS_SERVER", redisServerDefault)
var testName = config.GetEnv("TEST_NAME", "TEST-default")

var count uint64
var showPayload = false
var enableTestStats = true
var enableError =  true
var redisChan chan string

var authorizationError = false
var authorizationErrorCount uint64
var timeOfStart = time.Now()
var errorCodes = []int{200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 200, 400, 500}
var limitRate = 100
var limitBurst = 1000
var limiter = rate.NewLimiter(rate.Limit(limitRate), limitBurst)

func limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if limiter.Allow() == false {
			fmt.Println("====sending 429 =====")
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

func handleReq(rw http.ResponseWriter, req *http.Request) {
	if showPayload {
		requestDump, _ := httputil.DumpRequest(req, true)
		fmt.Println(string(requestDump))
	}

	atomic.AddUint64(&count, 1)
	var respMessage string
	if enableError {
		if authorizationError {
			fmt.Println("====sending 401 ======")
			http.Error(rw, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
			atomic.AddUint64(&authorizationErrorCount, 1)
			return
		}
		statusCode := rand.Intn(len(errorCodes))
		switch errorCodes[statusCode] {
		case 200:
			fmt.Println("====sending 200 OK=======")
			respMessage = "OK"
		case 400:
			fmt.Println("====sending 400 =======")
			http.Error(rw, http.StatusText(http.StatusBadRequest),
				http.StatusBadRequest)
			return
		case 500:
			fmt.Println("====sending 500 =======")
			http.Error(rw, http.StatusText(http.StatusInternalServerError),
				http.StatusInternalServerError)
			return
		}
	}

	//Reached here means no synthetic error OR error-code = 200
	rw.Write([]byte(respMessage))

	if enableTestStats {
		redisChan <- req.URL.Query().Get("ea")
	}
}


func disableError() {
		<-time.After(120 * time.Second)
		enableError = false

}
func flipAuthorizationError() {
	for {
		<-time.After(100 * time.Millisecond)
		if authorizationError {
			if authorizationErrorCount > 10 {
				authorizationError = false
				atomic.StoreUint64(&authorizationErrorCount, 0)
			}
		        continue
		}
		<-time.After(120 * time.Second)
		authorizationError = true
		atomic.StoreUint64(&authorizationErrorCount, 0)
	}
}
func printCounter() {
	startTime := time.Now()
	for {
		time.Sleep(5 * time.Second)
		fmt.Println("Count", count, time.Since(startTime))
	}
}

func redisLoop() {
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})

	redisUserSet := fmt.Sprintf("%s_user_dst", testName)
	redisEventSet := fmt.Sprintf("%s_event_dst", testName)
	redisEventTimeHash := fmt.Sprintf("%s_event_dst_timestamp", testName)

	pipe := redisClient.Pipeline()
	var eventAdded bool
	var inactiveBatchCount int

	for {
		select {
		case eventData := <-redisChan:
			data := strings.Split(eventData, "-")
			if len(data) < 3 {
				fmt.Println("Invalid Event data")
				break
			}

			userID := data[0]
			messageID := data[1]
			eventTime, err := strconv.Atoi(data[2])
			misc.AssertError(err)

			pipe.RPush(testName+":"+userID+":dst_list", messageID)
			pipe.SAdd(redisUserSet, userID)
			pipe.SAdd(redisEventSet, messageID)
			pipe.HSet(redisEventTimeHash, messageID, eventTime)
			eventAdded = true
			// pipe.ZAdd(redisEventTimeSortedSet, redis.Z{Score: float64(eventTime), Member: messageID})

		case <-time.After(batchTimeout):
			if eventAdded {
				_, err := pipe.Exec()
				misc.AssertError(err)
				atomic.StoreInt32(&isInactive, 0)
				inactiveBatchCount = 0
			} else {
				inactiveBatchCount++
				if inactiveBatchCount > inactivityBatchesThreshold {
					atomic.StoreInt32(&isInactive, 1)
				}
			}
			eventAdded = false
		}
	}
}

func main() {
	fmt.Println("Starting server")

	config.Initialize()
	fmt.Println(config.GetInt("SinkServer.rate", 100), config.GetInt("SinkServer.burst", 1000))
	redisChan = make(chan string)

	if enableError {
		go flipAuthorizationError()
		go disableError()
	}

	go printCounter()

	if enableTestStats {
		if len(redisServer) == 0 || len(testName) == 0 {
			panic("REDIS_URL or TEST_NAME variables can't be empty")
		}

		go redisLoop()
		http.HandleFunc("/isActive", handleActiveReq)
	}

	http.HandleFunc("/", handleReq)
	http.ListenAndServe(":8181", nil)
}

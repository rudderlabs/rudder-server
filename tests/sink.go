package main

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bugsnag/bugsnag-go"
	"github.com/go-redis/redis"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"golang.org/x/time/rate"
)

const (
	redisServerDefault = "localhost:6379"
)

var redisServer = config.GetEnv("REDIS_SERVER", redisServerDefault)
var testName = config.GetEnv("TEST_NAME", "TEST-default")

var count uint64
var showPayload = false
var enableTestStats = false
var enableError = false
var redisChan chan string

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

var countStat, successStat stats.RudderStats

func stat(wrappedFunc func(http.ResponseWriter, *http.Request)) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		countStat.Increment()
		wrappedFunc(w, r)
	}
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
	rw.Write([]byte(respMessage))
	successStat.Increment()

	if enableTestStats {
		redisChan <- req.URL.Query().Get("ea")
	}
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
			if err != nil {
				panic(err)
			}

			pipe.RPush(testName+":"+userID+":dst_list", messageID)
			pipe.SAdd(redisUserSet, userID)
			pipe.SAdd(redisEventSet, messageID)
			pipe.HSet(redisEventTimeHash, messageID, eventTime)
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

// skipcq: SCC-compile
func main() {
	fmt.Println("Starting test sink server")

	stats.Setup()

	countStat = stats.NewStat("sink.request_count", stats.CountType)
	successStat = stats.NewStat("sink.success_count", stats.CountType)

	fmt.Println(config.GetInt("SinkServer.rate", 100), config.GetInt("SinkServer.burst", 1000))
	redisChan = make(chan string)

	if enableError {
		go flipErrorType()
	}

	go printCounter()

	if enableTestStats {
		if len(redisServer) == 0 || len(testName) == 0 {
			panic(errors.New("REDIS_URL or TEST_NAME variables can't be empty"))
		}

		go redisLoop()
		http.HandleFunc("/isActive", handleActiveReq)
	}

	http.HandleFunc("/", stat(handleReq))
	http.ListenAndServe(":8181", bugsnag.Handler(nil))
}

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
	redisServer = "redis:6379"
	testName    = "Test9"
)

var count uint64
var showPayload = false
var enableTestStats = true
var enableError = true
var redisChan chan string

var timeOfStart = time.Now()
var errorCodes = []int{200, 200, 200, 200, 200, 400, 400, 500}
var authorizationError = false
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

func flipAuthorizationError() {
	for {
		<-time.After(5 * time.Second)
		authorizationError = !authorizationError
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
	var batchTimeout = 1000 * time.Millisecond

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer,
	})

	redisUserSet := fmt.Sprintf("%s_user_dst", testName)
	redisEventSet := fmt.Sprintf("%s_event_dst", testName)
	redisEventTimeHash := fmt.Sprintf("%s_event_dst_timestamp", testName)

	pipe := redisClient.Pipeline()
	var eventAdded bool

	for {
		select {
		case eventData := <-redisChan:
			data := strings.Split(eventData, "-")
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
	}

	go printCounter()

	if enableTestStats {
		go redisLoop()
	}

	http.HandleFunc("/", handleReq)
	http.ListenAndServe(":8181", nil)
}

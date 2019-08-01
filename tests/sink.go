package main

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/misc"
)

const (
	redisServerDefault = "localhost:6379"
)

var redisServer = config.GetEnv("REDIS_URL", redisServerDefault)
var testName = config.GetEnv("TEST_NAME", "TEST-default")

var count uint64
var showPayload = false

var enableTestStats = true
var redisChan chan string

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
	/*
		if showPayload {
			requestDump, _ := httputil.DumpRequest(req, true)
			fmt.Println(string(requestDump))
		}
		if req.Body != nil {
			ioutil.ReadAll(req.Body)
			defer req.Body.Close()
		}
		respMessage := "OK"
		rw.Write([]byte(respMessage))
	*/

	atomic.AddUint64(&count, 1)

	if enableTestStats {
		redisChan <- req.URL.Query().Get("ea")
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
	redisChan = make(chan string)
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

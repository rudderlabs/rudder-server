package main

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-server/misc"

	"github.com/go-redis/redis"
)

const (
	redisServer = "redis:6379"
	testName    = "Test9"
)

var count uint64
var showPayload = false

var enableTestStats = true
var redisChan chan string

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
	redisChan = make(chan string)
	go printCounter()

	if enableTestStats {
		go redisLoop()
	}

	http.HandleFunc("/", handleReq)
	http.ListenAndServe(":8181", nil)
}

package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"golang.org/x/time/rate"
)

var count uint64

var timeOfStart = time.Now()

var errorCodes = []int{200, 200, 200, 200, 200, 400, 400, 500} //success prob increase

var limiter = rate.NewLimiter(rate.Limit(config.GetInt("SinkServer.rate", 100)), config.GetInt("SinkServer.burst", 1000))

var authorizationError = false //not using mutex, as that sync not required

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
	atomic.AddUint64(&count, 1)
	var respMessage string
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
		http.Error(rw, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	case 500:
		fmt.Println("====sending 500 =======")
		http.Error(rw, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	rw.Write([]byte(respMessage))

}

func returnEnableAuthorizationError() {
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

func main() {
	fmt.Println("Starting server")
	config.Initialize()
	fmt.Println(config.GetInt("SinkServer.rate", 100), config.GetInt("SinkServer.burst", 1000))
	go printCounter()
	go returnEnableAuthorizationError()
	serve := http.NewServeMux()
	serve.HandleFunc("/", handleReq)
	http.ListenAndServe(":8181", limit(serve))
}

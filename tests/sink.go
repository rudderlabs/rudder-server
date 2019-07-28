package main

import (
	"fmt"
	//"io/ioutil"
	"net/http"
	//"net/http/httputil"
	"sync/atomic"
	"time"
)

var count uint64
var showPayload = false

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
	go printCounter()
	http.HandleFunc("/", handleReq)
	http.ListenAndServe(":8181", nil)
}

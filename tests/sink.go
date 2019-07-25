package main

import (
	"fmt"
	"net/http"
)
var count int
func handleReq(rw http.ResponseWriter, req *http.Request) {
	count += 1
	if count % 1000 == 0 {
		fmt.Println("Count", count)
	}
	respMessage := "OK"
	rw.Write([]byte(respMessage))

}

func main() {
	fmt.Println("Starting server")
	http.HandleFunc("/", handleReq)
	http.ListenAndServe(":8181", nil)
}



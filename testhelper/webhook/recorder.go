package webhook

import (
	"bufio"
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"sync"
)

type Recorder struct {
	Server     *httptest.Server
	requests   [][]byte
	requestsMu sync.RWMutex
}

func NewRecorder() *Recorder {
	whr := Recorder{}
	whr.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dump, err := httputil.DumpRequest(r, true)
		if err != nil {
			http.Error(w, fmt.Sprint(err), http.StatusInternalServerError)
			return
		}
		whr.requestsMu.Lock()
		whr.requests = append(whr.requests, dump)
		whr.requestsMu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))

	return &whr
}

func (whr *Recorder) RequestsCount() int {
	whr.requestsMu.RLock()
	defer whr.requestsMu.RUnlock()
	return len(whr.requests)
}

func (whr *Recorder) Requests() []*http.Request {
	whr.requestsMu.RLock()
	defer whr.requestsMu.RUnlock()

	requests := make([]*http.Request, len(whr.requests))
	for i, d := range whr.requests {
		requests[i], _ = http.ReadRequest(bufio.NewReader(bytes.NewReader(d)))
	}
	return requests
}

func (whr *Recorder) Close() {
	whr.Server.Close()
}

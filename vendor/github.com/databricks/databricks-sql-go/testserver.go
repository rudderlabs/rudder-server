package dbsql

import (
	"net/http"
	"net/http/httptest"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
)

type thriftHandler struct {
	processor               thrift.TProcessor
	inPfactory, outPfactory thrift.TProtocolFactory
	count503_2_retries      int
	count503_5_retries      int
	count429_2_retries      int
}

func (h *thriftHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.RequestURI {
	case "/503-2-retries":
		if h.count503_2_retries <= 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			h.count503_2_retries++
			return
		} else {
			h.count503_2_retries = 0
		}
	case "/429-2-retries":
		if h.count429_2_retries <= 1 {

			w.Header().Add("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			h.count429_2_retries++
			return
		} else {
			h.count429_2_retries = 0
		}
	case "/503-5-retries":
		if h.count503_5_retries <= 5 {
			w.WriteHeader(http.StatusServiceUnavailable)
			h.count503_5_retries++
			return
		} else {
			h.count503_5_retries = 0
		}
	case "/429-5-retries":
		if h.count503_5_retries <= 5 {
			w.Header().Set("Retry-After", "12")
			w.WriteHeader(http.StatusTooManyRequests)
			h.count503_5_retries++
			return
		} else {
			h.count503_5_retries = 0
		}
	}

	thriftHandler := thrift.NewThriftHandlerFunc(h.processor, h.inPfactory, h.outPfactory)
	thriftHandler(w, r)
}

func initThriftTestServer(handler cli_service.TCLIService) *httptest.Server {

	tcfg := &thrift.TConfiguration{
		TLSConfig: nil,
	}

	protocolFactory := thrift.NewTBinaryProtocolFactoryConf(tcfg)

	processor := cli_service.NewTCLIServiceProcessor(handler)

	th := thriftHandler{
		processor:   processor,
		inPfactory:  protocolFactory,
		outPfactory: protocolFactory,
	}

	ts := httptest.NewServer(&th)

	return ts
}

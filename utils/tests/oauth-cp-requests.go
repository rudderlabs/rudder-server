package testutils

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

type CpResponseParams struct {
	Timeout  time.Duration
	Code     int
	Response string
}
type CpResponseProducer struct {
	Responses []CpResponseParams
	callCount int
}

func (cp *CpResponseProducer) GetNext() CpResponseParams {
	if cp.callCount >= len(cp.Responses) {
		panic("ran out of responses")
	}
	cpResp := cp.Responses[cp.callCount]
	cp.callCount++
	return cpResp
}

func (cp *CpResponseProducer) MockCpRequests() *chi.Mux {
	srvMux := chi.NewMux()
	srvMux.Post("/destination/workspaces/{workspaceId}/accounts/{accountId}/token", func(w http.ResponseWriter, req *http.Request) {
		// iterating over request parameters
		for _, reqParam := range []string{"workspaceId", "accountId"} {
			param := chi.URLParam(req, reqParam)
			if param == "" {
				// This case wouldn't occur I guess
				http.Error(w, fmt.Sprintf("Wrong url being sent: %v", reqParam), http.StatusBadRequest)
				return
			}
		}

		cpResp := cp.GetNext()
		// sleep is being used to mimic the waiting in actual transformer response
		if cpResp.Timeout > 0 {
			time.Sleep(cpResp.Timeout)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(cpResp.Code)
		_, _ = w.Write([]byte(cpResp.Response))
	})

	srvMux.Put("/workspaces/{workspaceId}/destinations/{destinationId}/authStatus/toggle", func(w http.ResponseWriter, req *http.Request) {
		// iterating over request parameters
		for _, reqParam := range []string{"workspaceId", "destinationId"} {
			param := chi.URLParam(req, reqParam)
			if param == "" {
				// This case wouldn't occur I guess
				http.Error(w, fmt.Sprintf("Wrong url being sent: %v", reqParam), http.StatusNotFound)
				return
			}
		}

		cpResp := cp.GetNext()
		// sleep is being used to mimic the waiting in actual transformer response
		if cpResp.Timeout > 0 {
			time.Sleep(cpResp.Timeout)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(cpResp.Code)
		_, _ = w.Write([]byte(cpResp.Response))
	})
	return srvMux
}

type BasicAuthMock struct{}

func (b *BasicAuthMock) BasicAuth() (string, string) {
	return "test", "test"
}

func (b *BasicAuthMock) ID() string {
	return "test"
}

func (b *BasicAuthMock) Type() deployment.Type {
	return "test"
}

package testutils

import (
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

type CpResponseParams struct {
	Timeout  time.Duration
	Code     int
	Response string
}
type CpResponseProducer struct {
	Responses []CpResponseParams
	CallCount int
}

func (s *CpResponseProducer) GetNext() CpResponseParams {
	if s.CallCount >= len(s.Responses) {
		panic("ran out of responses")
	}
	cpResp := s.Responses[s.CallCount]
	s.CallCount++
	return cpResp
}

func (cp *CpResponseProducer) MockCpRequests() *chi.Mux {
	srvMux := chi.NewMux()
	srvMux.HandleFunc("/destination/workspaces/{workspaceId}/accounts/{accountId}/token", func(w http.ResponseWriter, req *http.Request) {
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
		// Lint error fix
		_, err := w.Write([]byte(cpResp.Response))
		if err != nil {
			http.Error(w, fmt.Sprintf("Provided response is faulty, please check it. Err: %v", err.Error()), http.StatusInternalServerError)
			return
		}
	})

	srvMux.HandleFunc("/workspaces/{workspaceId}/destinations/{destinationId}/authStatus/toggle", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
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
		// Lint error fix
		_, err := w.Write([]byte(cpResp.Response))
		if err != nil {
			http.Error(w, fmt.Sprintf("Provided response is faulty, please check it. Err: %v", err.Error()), http.StatusInternalServerError)
			return
		}
	})
	return srvMux
}

package api_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/rudderlabs/rudder-server/gateway"
	"github.com/rudderlabs/rudder-server/gateway/rudder-sources/api"
	"github.com/rudderlabs/rudder-server/regulation-worker/internal/delete/api"
	"github.com/stretchr/testify/require"
)

func TestDelete(t *testing.T) {
	gateway.Init()
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockSvc := api.NewMockSourcesService(mockCtrl)
	gw := gateway.HandleT{
		SourcesAPI: api.Source{
			SVC: mockSvc,
		},
	}
	gw.StartWebHandler(context.Background())

	var tests = []struct {
		name      string
		endpoint  string
		method    string
		arguments []map[string]string
	}{
		{
			name:     "basic test",
			endpoint: prepURL("/v1/job-status/{job_id}", "123"),
		},
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			t.Log("endpoint tested:", tt.endpoint)
			url := fmt.Sprintf()
			req, err := http.NewRequest(tt.method, "http://localhost:8080"+tt.endpoint, bytes.NewBuffer(tt.reqBody))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()

			h := (&api.ManageAPI{Svc: tt.mockRegSvc}).Handler()
			h.ServeHTTP(resp, req)
			body, err := ioutil.ReadAll(resp.Body)
			t.Log(string(body))
			require.NoError(t, err)
			if tt.responseCode < 400 {
				require.Equal(t, "application/json", resp.Header().Get("Content-Type"))
			}
			require.Equal(t, tt.responseCode, resp.Code, "Response code mismatch")
			if tt.expectedResponseBody != "" {
				require.Equal(t, tt.expectedResponseBody, string(body))

			}
			if len(tt.expectedPostRegulations) > 0 {
				for i := 0; i < len(tt.expectedPostRegulations); i++ {
					tt.expectedPostRegulations[i].Id = tt.mockRegSvc.createdRegulations[i].Id
				}
				require.Equal(t, tt.expectedPostRegulations, tt.mockRegSvc.createdRegulations)
				require.Equal(t, tt.expectedDest, tt.mockRegSvc.receivedDest, "actual destination received is different than expected")
			}
			if tt.expCancelReg.regulationId != "" {
				require.Equal(t, tt.expCancelReg, tt.mockRegSvc.actualCancelRegulation)
			}
			require.Equal(t, tt.expectedPageSize, tt.mockRegSvc.pageSize, "actual page size different than expected")
		})
	}
}

func prepURL(url string, params ...string) string {
	var re = regexp.MustCompile(`{.*?}`)
	i := 0
	return string(re.ReplaceAllFunc([]byte(url), func(matched []byte) []byte {
		if i >= len(params) {
			panic(fmt.Sprintf("value for %q not provided", matched))
		}
		v := params[i]
		i++
		return []byte(v)
	}))
}

package klaviyobulkupload

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

type importErrorsTransport func(*http.Request) (*http.Response, error)

func (f importErrorsTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestGetUploadErrorsFollowsTwoPages(t *testing.T) {
	requests := 0
	client := &RateLimiterHTTPClient{
		client: &http.Client{Transport: importErrorsTransport(func(req *http.Request) (*http.Response, error) {
			requests++
			require.Equal(t, "100", req.URL.Query().Get("page[size]"))
			require.Equal(t, "Klaviyo-API-Key key", req.Header.Get("Authorization"))
			body := `{"data":[{"attributes":{"original_payload":{"id":"first"}}}],"links":{"next":"https://a.klaviyo.com/api/profile-bulk-import-jobs/chunk/import-errors?page[size]=100&page[cursor]=two"}}`
			if requests == 2 {
				require.Equal(t, "two", req.URL.Query().Get("page[cursor]"))
				body = `{"data":[{"attributes":{"original_payload":{"id":"second"}}}]}`
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		})},
		Ratelimiter: rate.NewLimiter(rate.Inf, 1),
	}
	service := &KlaviyoAPIServiceImpl{client: client, PrivateAPIKey: "key"}
	result, err := service.GetUploadErrors("chunk")
	require.NoError(t, err)
	require.Equal(t, 2, requests)
	require.Len(t, result.Data, 2)
	require.Equal(t, "first", result.Data[0].Attributes.OriginalPayload.Id)
	require.Equal(t, "second", result.Data[1].Attributes.OriginalPayload.Id)
}

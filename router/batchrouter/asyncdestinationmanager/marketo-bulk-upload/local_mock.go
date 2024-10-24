package marketobulkupload

import "net/http"

// mockTransport implements http.RoundTripper interface
type mockTransport struct {
	response *http.Response
	err      error
}

func (m *mockTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return m.response, m.err
}

// createMockClient creates a new http.Client with mocked transport
func createMockClient(response *http.Response, err error) *http.Client {
	return &http.Client{
		Transport: &mockTransport{
			response: response,
			err:      err,
		},
	}
}

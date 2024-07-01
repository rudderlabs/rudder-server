package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
)

type TestStruct struct {
	field1 string
}

func TestMakePOSTRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewReportingClient(server.URL, logger.NOP, stats.NOP, nil)

	testData := TestStruct{
		field1: "test",
	}

	t.Run("test MakePOSTRequest success", func(t *testing.T) {
		err := client.MakePOSTRequest(context.Background(), &testData)
		assert.NoError(t, err)
	})

	t.Run("JSON marshal error", func(t *testing.T) {
		invalidPayload := make(chan int) // invalid payload that cannot be marshalled to JSON

		err := client.MakePOSTRequest(context.Background(), invalidPayload)

		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "json: unsupported type: chan int")
	})
}

func TestIsHTTPRequestSuccessful(t *testing.T) {
	client := &ReportingClient{}

	tests := []struct {
		name   string
		status int
		want   bool
	}{
		{
			name:   "status 200",
			status: 200,
			want:   true,
		},
		{
			name:   "status 400",
			status: 400,
			want:   true,
		},
		{
			name:   "status 429",
			status: 429,
			want:   false,
		},
		{
			name:   "status 500",
			status: 500,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := client.isHTTPRequestSuccessful(tt.status)
			assert.Equal(t, tt.want, got)
		})
	}
}

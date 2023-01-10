package client_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/stretchr/testify/require"
)

func TestWarehouse(t *testing.T) {

	t.Run("timeout", func(t *testing.T) {
		t.Parallel()

		block := make(chan struct{})
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-block

		}))
		t.Cleanup(ts.Close)
		t.Cleanup(func() {
			close(block)
		})

		c := client.NewWarehouse(ts.URL, client.WithTimeout(10*time.Millisecond))

		err := c.Process(context.Background(), client.StagingFile{})
		require.EqualError(t, err, fmt.Sprintf("http request to \"%[1]s\": Post \"%[1]s/v1/process\": context deadline exceeded (Client.Timeout exceeded while awaiting headers", ts.URL))
	})
}

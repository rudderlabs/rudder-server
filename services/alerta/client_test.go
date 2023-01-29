package alerta_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/services/alerta"
	"github.com/stretchr/testify/require"
)

func TestSendFeatures(t *testing.T) {
	var (
		testTeam     = "test-team"
		testResource = "test-resource"
		testService  = alerta.Service{"test-service"}
		testSeverity = alerta.SeverityOk
		testPriority = alerta.PriorityP3
	)

	wantBody := `{"resource":"test-resource","event":"ok/rudder/test-resource:notificationServiceMode=PRODUCTION,priority=P3,sendToNotificationService=true,tag1=value1,tag2=value2,team=test-team","environment":"PRODUCTION","severity":"ok","group":"notificationServiceMode=PRODUCTION,priority=P3,sendToNotificationService=true,tag1=value1,tag2=value2,team=test-team","text":"temp-text","service":["test-service"],"timeout":86400,"tags":["notificationServiceMode=PRODUCTION","priority=P3","sendToNotificationService=true","tag1=value1","tag2=value2","team=test-team"]}`

	alertaClient := func(s *httptest.Server, o ...alerta.OptFn) alerta.AlertSender {
		var op []alerta.OptFn
		op = append(op, o...)
		op = append(op, alerta.WithHTTPClient(s.Client()))
		op = append(op, alerta.WithTeam(testTeam))
		op = append(op, alerta.WithTags(alerta.Tags{
			"tag1=value1",
			"tag2=value2",
		}))

		return alerta.NewClient(
			s.URL,
			op...,
		)
	}

	t.Run("success", func(t *testing.T) {
		t.Parallel()

		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, "application/json; charset=utf-8", r.Header.Get("Content-Type"))
			require.Equal(t, http.MethodPost, r.Method)
			require.Equal(t, "/alert", r.URL.Path)

			body, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.Equal(t, wantBody, string(body))

			w.WriteHeader(http.StatusOK)
		}))
		defer s.Close()

		ctx := context.Background()

		err := alertaClient(s,
			alerta.WithHTTPClient(s.Client()),
			alerta.WithText("temp-text"),
		).SendAlert(
			ctx,
			testResource,
			testService,
			testSeverity,
			testPriority,
		)
		require.NoError(t, err)
	})

	t.Run("unexpected retriable status", func(t *testing.T) {
		t.Parallel()

		const maxRetries = 2
		var count int64

		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&count, 1)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer s.Close()

		ctx := context.Background()

		err := alertaClient(s, alerta.WithMaxRetries(maxRetries)).SendAlert(
			ctx,
			testResource,
			testService,
			testSeverity,
			testPriority,
		)
		require.EqualError(t, err, "unexpected status code 500: ")

		require.Equalf(t, int64(maxRetries+1), atomic.LoadInt64(&count), "retry %d times", maxRetries)
	})

	t.Run("unexpected non-retriable status", func(t *testing.T) {
		t.Parallel()

		const maxRetries = 2
		var count int64

		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&count, 1)
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer s.Close()

		ctx := context.Background()

		err := alertaClient(s, alerta.WithMaxRetries(maxRetries)).SendAlert(
			ctx,
			testResource,
			testService,
			testSeverity,
			testPriority,
		)
		require.EqualError(t, err, "non retriable: unexpected status code 400: ")

		require.Equalf(t, int64(1), atomic.LoadInt64(&count), "retry %d times", maxRetries)
	})

	t.Run("timeout", func(t *testing.T) {
		t.Parallel()

		const maxRetries = 1

		blocker := make(chan struct{})
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-blocker
		}))
		defer s.Close()
		defer close(blocker)

		ctx := context.Background()

		err := alertaClient(s,
			alerta.WithHTTPClient(s.Client()),
			alerta.WithMaxRetries(maxRetries),
			alerta.WithTimeout(time.Millisecond),
		).SendAlert(
			ctx,
			testResource,
			testService,
			testSeverity,
			testPriority,
		)

		require.Error(t, err, "deadline exceeded ")
	})

	t.Run("context cancelled", func(t *testing.T) {
		t.Parallel()

		const maxRetries = 1

		blocker := make(chan struct{})
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-blocker
		}))
		defer s.Close()
		defer close(blocker)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := alertaClient(s,
			alerta.WithHTTPClient(s.Client()),
			alerta.WithMaxRetries(maxRetries),
		).SendAlert(
			ctx,
			testResource,
			testService,
			testSeverity,
			testPriority,
		)

		require.Error(t, err, "deadline exceeded ")
	})
}

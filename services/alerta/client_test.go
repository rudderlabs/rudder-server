package alerta_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/alerta"
	"github.com/stretchr/testify/require"
)

func TestSendFeatures(t *testing.T) {
	var (
		testTeam         = "test-team"
		testResource     = "test-resource"
		testText         = "test-text"
		testNamespace    = "test-namespce"
		testAlertTimeout = 10
		testEnvironment  = "DEVELOPMENT"
		testService      = alerta.Service{"test-service"}
		testSeverity     = alerta.SeverityOk
		testPriority     = alerta.PriorityP3
		testTags         = alerta.Tags{
			"tag1=value1",
			"tag2=value2",
		}
	)

	wantBody := `{"resource":"test-resource","event":"rudder/test-resource:namespace=test-namespce,notificationServiceMode=DEVELOPMENT,priority=P3,sendToNotificationService=true,tag1=value1,tag2=value2,team=test-team","environment":"DEVELOPMENT","severity":"ok","group":"namespace=test-namespce,notificationServiceMode=DEVELOPMENT,priority=P3,sendToNotificationService=true,tag1=value1,tag2=value2,team=test-team","text":"test-text","service":["test-service"],"timeout":10,"tags":["namespace=test-namespce","notificationServiceMode=DEVELOPMENT","priority=P3","sendToNotificationService=true","tag1=value1","tag2=value2","team=test-team"]}`

	c := config.New()

	alertaClient := func(s *httptest.Server, o ...alerta.OptFn) alerta.AlertSender {
		var op []alerta.OptFn
		op = append(op, o...)
		op = append(op, alerta.WithHTTPClient(s.Client()))
		op = append(op, alerta.WithTeam(testTeam))
		op = append(op, alerta.WithKubeNamespace(testNamespace))
		op = append(op, alerta.WithAlertTimeout(testAlertTimeout))
		op = append(op, alerta.WithEnvironment(testEnvironment))
		op = append(op, alerta.WithConfig(c))

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

			w.WriteHeader(http.StatusCreated)
		}))
		defer s.Close()

		ctx := context.Background()

		err := alertaClient(s,
			alerta.WithHTTPClient(s.Client()),
		).SendAlert(
			ctx,
			testResource,
			alerta.SendAlertOpts{
				Severity: testSeverity,
				Priority: testPriority,
				Service:  testService,
				Text:     testText,
				Tags:     testTags,
			},
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
			alerta.SendAlertOpts{
				Severity: testSeverity,
				Priority: testPriority,
				Service:  testService,
				Text:     testText,
				Tags:     testTags,
			},
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
			alerta.SendAlertOpts{
				Severity: testSeverity,
				Priority: testPriority,
				Service:  testService,
				Text:     testText,
				Tags:     testTags,
			},
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
			alerta.SendAlertOpts{
				Severity: testSeverity,
				Priority: testPriority,
				Service:  testService,
				Text:     testText,
				Tags:     testTags,
			},
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
			alerta.SendAlertOpts{
				Severity: testSeverity,
				Priority: testPriority,
				Service:  testService,
				Text:     testText,
				Tags:     testTags,
			},
		)

		require.Error(t, err, "deadline exceeded ")
	})
}

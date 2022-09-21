package features_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rudderlabs/rudder-server/services/controlplane/features"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
	"github.com/stretchr/testify/require"
)

func Test_Client_Send(t *testing.T) {
	validSecret := "valid-secret"

	wantBody := `{"components":[{"name":"test","features":["feature1","feature2"]}]}`

	testcases := []struct {
		name     string
		identity identity.Identifier

		wantPath string
		wantErr  error
	}{
		{
			name: "workspace",
			identity: &identity.Workspace{
				WorkspaceID:    "valid-workspace-id",
				WorkspaceToken: validSecret,
			},

			wantPath: "/data-plane/v1/workspaces/valid-workspace-id/settings",
			wantErr:  nil,
		},
		{
			name: "namespace",
			identity: &identity.Namespace{
				Namespace:    "valid-namespace",
				HostedSecret: validSecret,
			},
			wantPath: "/data-plane/v1/namespaces/valid-namespace/settings",
			wantErr:  nil,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				u, _, _ := r.BasicAuth()

				require.Equal(t, "application/json", r.Header.Get("Content-Type"))
				require.Contains(t, r.UserAgent(), "control-plane/features")
				require.Equal(t, validSecret, u)
				require.Equal(t, http.MethodPost, r.Method)
				require.Equal(t, tc.wantPath, r.URL.Path)

				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, wantBody, string(body))

				w.WriteHeader(http.StatusNoContent)
			}))
			defer s.Close()

			c := features.NewClient(s.URL, tc.identity, features.WithHTTPClient(s.Client()))

			err := c.Send(context.Background(), "test", []string{"feature1", "feature2"})
			require.NoError(t, err)
		})
	}

	t.Run("unexpected retriable status", func(t *testing.T) {
		const maxRetries = 2
		var count int64

		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&count, 1)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer s.Close()

		c := features.NewClient(s.URL, &identity.Namespace{},
			features.WithHTTPClient(s.Client()),
			features.WithMaxRetries(maxRetries),
		)

		err := c.Send(context.Background(), "test", []string{"feature1", "feature2"})
		require.ErrorContains(t, err, "")

		require.Equalf(t, int64(maxRetries+1), atomic.LoadInt64(&count), "retry %d times", maxRetries)
	})

	t.Run("unexpected non-retriable status", func(t *testing.T) {
		const maxRetries = 2
		var count int64

		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&count, 1)
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer s.Close()

		c := features.NewClient(s.URL, &identity.Namespace{},
			features.WithHTTPClient(s.Client()),
			features.WithMaxRetries(maxRetries),
		)

		err := c.Send(context.Background(), "test", []string{"feature1", "feature2"})
		require.ErrorContains(t, err, "")

		require.Equalf(t, int64(1), atomic.LoadInt64(&count), "retry %d times", maxRetries)
	})

	t.Run("timeout", func(t *testing.T) {
		const maxRetries = 1

		blocker := make(chan struct{})
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-blocker
		}))
		defer s.Close()
		defer close(blocker)

		c := features.NewClient(s.URL, &identity.Namespace{},
			features.WithHTTPClient(s.Client()),
			features.WithMaxRetries(maxRetries),
			features.WithTimeout(time.Millisecond),
		)

		err := c.Send(context.Background(), "test", []string{"feature1", "feature2"})
		require.ErrorContains(t, err, "deadline exceeded")
	})
}

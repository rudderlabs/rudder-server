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

	r := &features.Registry{}
	r.Register("test", "feature1", "feature2")

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

			wantPath: "/data-plane/workspaces/valid-workspace-id/settings",
			wantErr:  nil,
		},
		{
			name: "namespace",
			identity: &identity.Namespace{
				Namespace:    "valid-namespace",
				HostedSecret: validSecret,
			},
			wantPath: "/data-plane/namespaces/valid-namespace/settings",
			wantErr:  nil,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				u, _, _ := r.BasicAuth()

				require.Equal(t, validSecret, u)
				require.Equal(t, http.MethodPost, r.Method)
				require.Equal(t, tc.wantPath, r.URL.Path)

				body, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, wantBody, string(body))

				w.WriteHeader(http.StatusOK)
			}))
			defer s.Close()

			c := features.New(tc.identity, features.WithHTTPClient(s.Client()), features.WithURL(s.URL))

			err := c.Send(context.Background(), r)
			require.NoError(t, err)
		})
	}

	t.Run("unexpected status", func(t *testing.T) {
		count := int64(0)
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&count, 1)
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer s.Close()

		c := features.New(&identity.Namespace{}, features.WithHTTPClient(s.Client()), features.WithURL(s.URL))

		err := c.Send(context.Background(), r)
		require.Error(t, err)

		require.Equalf(t, int64(4), atomic.LoadInt64(&count), "retry %d times", features.MaxRetries)
	})

	t.Run("timeout", func(t *testing.T) {
		t.Skip("test is too slow")
		count := int64(0)

		blocker := make(chan struct{})
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&count, 1)

			<-blocker
		}))
		defer s.Close()
		defer close(blocker)

		c := features.New(&identity.Namespace{}, features.WithHTTPClient(s.Client()), features.WithURL(s.URL), features.WithTimeout(time.Millisecond))

		err := c.Send(context.Background(), r)
		require.Error(t, err)

		require.Equalf(t, int64(4), atomic.LoadInt64(&count), "retry %d times", features.MaxRetries)
	})

}

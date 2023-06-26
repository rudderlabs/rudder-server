package controlplane_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/controlplane"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
)

func TestSendFeatures(t *testing.T) {
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
		tc := tc
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

			c := controlplane.NewClient(s.URL, tc.identity, controlplane.WithHTTPClient(s.Client()))

			err := c.SendFeatures(context.Background(), "test", []string{"feature1", "feature2"})
			require.NoError(t, err)
		})
	}
}

func TestDestinationHistory(t *testing.T) {
	validSecret := "valid-secret"

	body, err := os.ReadFile("./testdata/destination_history.json")
	require.NoError(t, err)

	expectedDestination := backendconfig.DestinationT{
		ID:   "2ENkYVMMUInzUGYp32R6dVghgvj",
		Name: "webhook.site",
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			ID:          "1aIXpUrvpGno4gEuF2GvI3O9dOe",
			Name:        "WEBHOOK",
			DisplayName: "Webhook",
			Config: map[string]interface{}{
				"transformAt":             "processor",
				"transformAtV1":           "processor",
				"saveDestinationResponse": false,
			},
			ResponseRules: map[string]interface{}(nil),
		},
		Config: map[string]interface{}{
			"webhookMethod": "POST",
			"webhookUrl":    "https://webhook.site/4e257da5-000e-4390-9ad2-d28d37fa934e",
		},
		Enabled:            true,
		WorkspaceID:        "1wBceIM3I4Fg5PJYiq8tbbvm3Ww",
		Transformations:    []backendconfig.TransformationT(nil),
		IsProcessorEnabled: false,
		RevisionID:         "2ENkYQ1MR9f83YhqS5k7uTSe5XH",
	}

	testcases := []struct {
		name       string
		identity   identity.Identifier
		region     string
		revisionID string

		responseBody   string
		responseStatus int

		wantDestination backendconfig.DestinationT
		wantPath        string
		wantErr         error
	}{
		{
			name: "valid request in workspace",
			identity: &identity.Workspace{
				WorkspaceID:    "valid-workspace-id",
				WorkspaceToken: validSecret,
			},
			revisionID:   "2ENkYQ1MR9f83YhqS5k7uTSe5XH",
			responseBody: string(body),

			wantDestination: expectedDestination,
			wantPath:        "/workspaces/destinationHistory/2ENkYQ1MR9f83YhqS5k7uTSe5XH",
		},
		{
			name: "valid request in workspace with region",
			identity: &identity.Workspace{
				WorkspaceID:    "valid-workspace-id",
				WorkspaceToken: validSecret,
			},
			revisionID:   "2ENkYQ1MR9f83YhqS5k7uTSe5XH",
			responseBody: string(body),
			region:       "eu",

			wantDestination: expectedDestination,
			wantPath:        "/workspaces/destinationHistory/2ENkYQ1MR9f83YhqS5k7uTSe5XH?region=eu",
		},
		{
			name: "valid request in namespace",
			identity: &identity.Namespace{
				Namespace:    "valid-namespace",
				HostedSecret: validSecret,
			},
			revisionID: "2ENkYQ1MR9f83YhqS5k7uTSe5XH",

			responseBody: string(body),

			wantDestination: expectedDestination,
			wantPath:        "/workspaces/destinationHistory/2ENkYQ1MR9f83YhqS5k7uTSe5XH",
		},
		{
			name: "valid request in namespace with region",
			identity: &identity.Namespace{
				Namespace:    "valid-namespace",
				HostedSecret: validSecret,
			},
			revisionID: "2ENkYQ1MR9f83YhqS5k7uTSe5XH",
			region:     "eu",

			responseBody: string(body),

			wantDestination: expectedDestination,
			wantPath:        "/workspaces/destinationHistory/2ENkYQ1MR9f83YhqS5k7uTSe5XH?region=eu",
		},
		{
			name: "valid request in namespace",
			identity: &identity.Namespace{
				Namespace:    "valid-namespace",
				HostedSecret: validSecret,
			},
			revisionID: "2ENkYQ1MR9f83YhqS5k7uTSe5XH",
			region:     "eu",

			responseBody:   `{"message":"History does not exist for revision id"}`,
			responseStatus: http.StatusNotFound,

			wantDestination: expectedDestination,
			wantPath:        "/workspaces/destinationHistory/2ENkYQ1MR9f83YhqS5k7uTSe5XH?region=eu",
			wantErr:         fmt.Errorf("non retriable: unexpected status code 404: {\"message\":\"History does not exist for revision id\"}"),
		},

		{
			name: "invalid JSON",
			identity: &identity.Workspace{
				WorkspaceID:    "valid-workspace-id",
				WorkspaceToken: validSecret,
			},
			revisionID: "2ENkYQ1MR9f83YhqS5k7uTSe5XH",

			responseBody: `<html>Hello</html>`,

			wantDestination: backendconfig.DestinationT{},
			wantPath:        "/workspaces/destinationHistory/2ENkYQ1MR9f83YhqS5k7uTSe5XH",
			wantErr:         fmt.Errorf("unmarshal response body: invalid character '<' looking for beginning of value"),
		},
	}

	for _, tc := range testcases {
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			if tc.responseStatus == 0 {
				tc.responseStatus = http.StatusOK
			}

			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				u, _, _ := r.BasicAuth()

				require.Equal(t, "application/json", r.Header.Get("Content-Type"))
				require.Contains(t, r.UserAgent(), "control-plane/features")
				require.Equal(t, validSecret, u)
				require.Equal(t, http.MethodGet, r.Method)
				require.Equal(t, tc.wantPath, r.URL.String())

				w.WriteHeader(tc.responseStatus)
				_, _ = fmt.Fprint(w, tc.responseBody)
			}))
			defer s.Close()

			c := controlplane.NewClient(
				s.URL,
				tc.identity,
				controlplane.WithHTTPClient(s.Client()),
				controlplane.WithRegion(tc.region),
			)

			destination, err := c.DestinationHistory(context.Background(), tc.revisionID)
			if tc.wantErr != nil {
				require.EqualError(t, err, tc.wantErr.Error())
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantDestination, destination)
			}
		})
	}
}

func TestRetriesTimeout(t *testing.T) {
	t.Log("all methods should exhibit the same retry and timeout behavior")
	methods := []struct {
		fn   func(*controlplane.Client) error
		name string
	}{
		{
			name: "SendFeatures",
			fn: func(c *controlplane.Client) error {
				return c.SendFeatures(context.Background(), "test", []string{"feature1", "feature2"})
			},
		},
		{
			name: "DestinationHistory",
			fn: func(c *controlplane.Client) error {
				_, err := c.DestinationHistory(context.Background(), "test")
				return err
			},
		},
	}

	for _, m := range methods {
		m := m
		t.Run(m.name, func(t *testing.T) {
			t.Parallel()
			t.Run("unexpected retriable status", func(t *testing.T) {
				t.Parallel()

				const maxRetries = 2
				var count int64

				s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					atomic.AddInt64(&count, 1)
					w.WriteHeader(http.StatusInternalServerError)
				}))
				defer s.Close()

				c := controlplane.NewClient(s.URL, &identity.Namespace{},
					controlplane.WithHTTPClient(s.Client()),
					controlplane.WithMaxRetries(maxRetries),
				)

				err := m.fn(c)
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

				c := controlplane.NewClient(s.URL, &identity.Namespace{},
					controlplane.WithHTTPClient(s.Client()),
					controlplane.WithMaxRetries(maxRetries),
				)

				err := m.fn(c)
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

				c := controlplane.NewClient(s.URL, &identity.Namespace{},
					controlplane.WithHTTPClient(s.Client()),
					controlplane.WithMaxRetries(maxRetries),
					controlplane.WithTimeout(time.Millisecond),
				)

				err := m.fn(c)
				require.ErrorContains(t, err, "deadline exceeded")
			})
		})
	}
}

func TestGetDestinationSSHKeyPair(t *testing.T) {
	var count int
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count == 0 { // failing the first call to test retries
			count++
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if r.Header.Get("Content-Type") != "application/json" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if r.Header.Get("User-Agent") == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if r.URL.Path != "/dataplane/admin/destinations/123/sshKeys" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		username, password, ok := r.BasicAuth()
		if !ok || username != "johnDoe" || password != "so-secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		require.NoError(t, json.NewEncoder(w).Encode(controlplane.SSHKeyPair{
			PrivateKey: "test-private-key",
			PublicKey:  "test-public-key",
		}))
	}))
	t.Cleanup(mockServer.Close)

	client := controlplane.NewAdminClient(
		mockServer.URL,
		&identity.Admin{
			Username: "johnDoe",
			Password: "so-secret",
		},
		controlplane.WithHTTPClient(mockServer.Client()),
	)

	keyPair, err := client.GetDestinationSSHKeyPair(context.Background(), "123")
	require.NoError(t, err)
	require.Equal(t, controlplane.SSHKeyPair{
		PrivateKey: "test-private-key",
		PublicKey:  "test-public-key",
	}, keyPair)

	_, err = client.GetDestinationSSHKeyPair(context.Background(), "456")
	require.Error(t, err)
}

package suppression

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
)

type syncResponse struct {
	expectedUrl string
	statusCode  int
	respBody    []byte
}

type suppressUserFixture struct {
	h      *handler
	server *httptest.Server

	mu             sync.Mutex
	serverResponse syncResponse
}

func newSuppressUserFixture(t *testing.T, getRepo func(t *testing.T) Repository) *suppressUserFixture {
	t.Helper()
	config.Reset()
	backendconfig.Init()

	f := &suppressUserFixture{}
	var count int
	var prevRespBody []byte
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.serverResponse.expectedUrl != "" {
			assert.Equal(t, f.serverResponse.expectedUrl, r.URL.Path)
		}
		var respBody []byte
		// send the expected payload if it is the first time or the payload has changed
		if count == 0 || prevRespBody != nil && !bytes.Equal(prevRespBody, f.serverResponse.respBody) {
			respBody = f.serverResponse.respBody
			prevRespBody = f.serverResponse.respBody
			count++
		} else { // otherwise send an response containing no items
			respBody, _ = jsonrs.Marshal(suppressionsResponse{
				Token: "tempToken123",
			})
		}

		w.WriteHeader(f.serverResponse.statusCode)
		_, _ = w.Write(respBody)
	}))
	t.Cleanup(f.server.Close)

	f.h = newHandler(getRepo(t), logger.NOP)
	return f
}

func (f *suppressUserFixture) setServerResponse(resp syncResponse) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.serverResponse = resp
}

func (f *suppressUserFixture) startSyncLoop(t *testing.T, identifier identity.Identifier) {
	t.Helper()
	s := MustNewSyncer(f.server.URL, identifier, f.h.r, WithPollIntervalFn(func() time.Duration { return 1 * time.Millisecond }))
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.SyncLoop(t.Context())
	}()
	t.Cleanup(func() { <-done })
}

func (f *suppressUserFixture) isSuppressed(workspaceID, userID, sourceID string) bool {
	return f.h.GetSuppressedUser(workspaceID, userID, sourceID) != nil
}

func TestSuppressUser(t *testing.T) {
	t.Run("memory", func(t *testing.T) {
		generateTests(t, func(t *testing.T) Repository {
			return NewMemoryRepository(logger.NewLogger())
		})
	})

	t.Run("badgerdb", func(t *testing.T) {
		generateTests(t, func(t *testing.T) Repository {
			p := path.Join(t.TempDir(), strings.ReplaceAll(uuid.New().String(), "-", ""))
			r, err := NewBadgerRepository(p, logger.NOP)
			require.NoError(t, err)
			t.Cleanup(func() { _ = r.Stop() })
			return r
		})
	})
}

func generateTests(t *testing.T, getRepo func(t *testing.T) Repository) {
	identifier := &identity.Workspace{
		WorkspaceID: "workspace-1",
	}
	defaultSuppression := model.Suppression{
		Canceled:    false,
		WorkspaceID: "workspace-1",
		UserID:      "user-1",
		SourceIDs:   []string{"src-1", "src-2"},
	}
	defaultResponse := suppressionsResponse{
		Items: []model.Suppression{defaultSuppression},
		Token: "tempToken123",
	}

	t.Run("sync error scenarios", func(t *testing.T) {
		t.Run("returns an error when a wrong server address is provided", func(t *testing.T) {
			f := newSuppressUserFixture(t, getRepo)
			_, _, err := MustNewSyncer("", identifier, f.h.r).sync(nil)
			require.Error(t, err)
		})

		t.Run("returns an error when server responds with HTTP 500", func(t *testing.T) {
			f := newSuppressUserFixture(t, getRepo)
			f.setServerResponse(syncResponse{
				statusCode: 500,
				respBody:   []byte(""),
			})
			_, _, err := MustNewSyncer(f.server.URL, identifier, f.h.r).sync(nil)
			require.EqualError(t, err, "failed to fetch source regulations: statusCode: 500")
		})

		t.Run("returns an error when server responds with invalid (empty) data in the response body", func(t *testing.T) {
			f := newSuppressUserFixture(t, getRepo)
			f.setServerResponse(syncResponse{
				statusCode: 200,
				respBody:   []byte(""),
			})
			_, _, err := MustNewSyncer(f.server.URL, identifier, f.h.r).sync(nil)
			require.Error(t, err)
			require.NotEmpty(t, err.Error())
		})

		t.Run("returns an error when server responds with invalid (corrupted json) data in the response body", func(t *testing.T) {
			f := newSuppressUserFixture(t, getRepo)
			f.setServerResponse(syncResponse{
				statusCode: 200,
				respBody:   []byte("{w"),
			})
			_, _, err := MustNewSyncer(f.server.URL, identifier, f.h.r).sync(nil)
			require.Error(t, err)
			require.NotEmpty(t, err.Error())
		})

		t.Run("returns an error when server responds with no token in the response body", func(t *testing.T) {
			f := newSuppressUserFixture(t, getRepo)
			resp := defaultResponse
			resp.Token = ""
			respBody, _ := jsonrs.Marshal(resp)
			f.setServerResponse(syncResponse{
				statusCode: 200,
				respBody:   respBody,
			})
			suppressions, _, err := MustNewSyncer(f.server.URL, identifier, f.h.r).sync(nil)
			require.EqualError(t, err, "no token returned in regulation API response")
			require.Equal(t, defaultResponse.Items, suppressions)
		})
	})

	t.Run("handler, repository, syncer integration", func(t *testing.T) {
		t.Run("exact user suppression match", func(t *testing.T) {
			f := newSuppressUserFixture(t, getRepo)
			respBody, _ := jsonrs.Marshal(defaultResponse)
			f.setServerResponse(syncResponse{
				expectedUrl: fmt.Sprintf("/dataplane/workspaces/%s/regulations/suppressions", identifier.WorkspaceID),
				statusCode:  200,
				respBody:    respBody,
			})
			f.startSyncLoop(t, identifier)
			require.Eventually(t, func() bool {
				return f.isSuppressed("workspace-1", "user-1", "src-1")
			}, time.Second, 10*time.Millisecond)
		})

		t.Run("user suppression added, then cancelled", func(t *testing.T) {
			f := newSuppressUserFixture(t, getRepo)
			resp := defaultResponse
			resp.Items = []model.Suppression{
				defaultSuppression,
				{
					WorkspaceID: "workspace-1",
					Canceled:    false,
					UserID:      "user-2",
					SourceIDs:   []string{"src-1", "src-2"},
				},
			}
			respBody, _ := jsonrs.Marshal(resp)
			f.setServerResponse(syncResponse{
				statusCode: 200,
				respBody:   respBody,
			})
			f.startSyncLoop(t, identifier)
			require.Eventually(t, func() bool {
				return f.isSuppressed("workspace-1", "user-2", "src-1")
			}, time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool {
				return f.isSuppressed("workspace-1", "user-1", "src-1")
			}, time.Second, 10*time.Millisecond)

			resp.Items[0].Canceled = true
			respBody, _ = jsonrs.Marshal(resp)
			f.setServerResponse(syncResponse{
				statusCode: 200,
				respBody:   respBody,
			})
			require.Eventually(t, func() bool {
				return !f.isSuppressed("workspace-1", "user-1", "src-1")
			}, time.Second, 10*time.Millisecond)
		})

		t.Run("wildcard user suppression match", func(t *testing.T) {
			f := newSuppressUserFixture(t, getRepo)
			resp := defaultResponse
			resp.Items = []model.Suppression{
				{
					WorkspaceID: "workspace-1",
					Canceled:    false,
					UserID:      "user-1",
					SourceIDs:   []string{},
				},
				{
					WorkspaceID: "workspace-1",
					Canceled:    false,
					UserID:      "user-2",
					SourceIDs:   []string{"src-2"},
				},
			}
			respBody, _ := jsonrs.Marshal(resp)
			f.setServerResponse(syncResponse{
				statusCode: 200,
				respBody:   respBody,
			})
			f.startSyncLoop(t, identifier)
			require.Eventually(t, func() bool {
				return f.isSuppressed("workspace-1", "user-1", "src-1")
			}, time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool {
				return f.isSuppressed("workspace-1", "user-2", "src-2")
			}, time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool {
				return !f.isSuppressed("workspace-1", "user-2", "src-1")
			}, time.Second, 10*time.Millisecond)
		})

		t.Run("wildcard user suppression rule added and then cancelled", func(t *testing.T) {
			f := newSuppressUserFixture(t, getRepo)
			resp := defaultResponse
			resp.Items = []model.Suppression{
				{
					WorkspaceID: "workspace-1",
					Canceled:    false,
					UserID:      "user-1",
					SourceIDs:   []string{},
				},
				{
					WorkspaceID: "workspace-1",
					Canceled:    false,
					UserID:      "user-2",
					SourceIDs:   []string{"src-2"},
				},
			}
			respBody, _ := jsonrs.Marshal(resp)
			f.setServerResponse(syncResponse{
				statusCode: 200,
				respBody:   respBody,
			})
			f.startSyncLoop(t, identifier)

			require.Eventually(t, func() bool {
				return f.isSuppressed("workspace-1", "user-1", "src-1")
			}, time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool {
				return f.isSuppressed("workspace-1", "user-2", "src-2")
			}, time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool {
				return !f.isSuppressed("workspace-1", "user-2", "src-1")
			}, time.Second, 10*time.Millisecond)

			resp.Items[0].Canceled = true
			respBody, _ = jsonrs.Marshal(resp)
			f.setServerResponse(syncResponse{
				statusCode: 200,
				respBody:   respBody,
			})
			require.Eventually(t, func() bool {
				return !f.isSuppressed("workspace-1", "user-1", "src-1")
			}, time.Second, 10*time.Millisecond)
		})

		t.Run("try to sync while restoring", func(t *testing.T) {
			f := newSuppressUserFixture(t, getRepo)
			respBody, _ := jsonrs.Marshal(defaultResponse)
			f.setServerResponse(syncResponse{
				statusCode: 200,
				respBody:   respBody,
			})
			var r readerFunc = func(_ []byte) (int, error) {
				time.Sleep(1 * time.Second)
				return 0, errors.New("read error")
			}
			restoreErr := make(chan error, 1)
			go func() {
				restoreErr <- f.h.r.Restore(r)
			}()
			f.startSyncLoop(t, identifier)
			require.Eventually(t, func() bool {
				return f.isSuppressed("workspace-1", "user-1", "src-1")
			}, 5*time.Second, 100*time.Millisecond)
			require.Error(t, <-restoreErr)
		})
	})

	t.Run("multi-tenant support", func(t *testing.T) {
		t.Run("supports older version of regulation service", func(t *testing.T) {
			f := newSuppressUserFixture(t, getRepo)
			// older version of regulation service doesn't return workspaceID as part of the suppressions
			resp := defaultResponse
			sup := &resp.Items[0]
			sup.WorkspaceID = "workspace-1"
			respBody, _ := jsonrs.Marshal(resp)
			f.setServerResponse(syncResponse{
				statusCode: 200,
				respBody:   respBody,
			})
			f.startSyncLoop(t, identifier)
			require.Eventually(t, func() bool {
				return f.isSuppressed("workspace-1", "user-1", "src-1")
			}, time.Second, 10*time.Millisecond)
		})
	})

	t.Run("supports syncing suppressions for namespace", func(t *testing.T) {
		f := newSuppressUserFixture(t, getRepo)
		namespaceIdentifier := &identity.Namespace{
			Namespace: "namespace-1",
		}

		resp := defaultResponse
		resp.Items = []model.Suppression{
			defaultSuppression,
			{
				WorkspaceID: "workspace-2",
				Canceled:    false,
				UserID:      "user-2",
				SourceIDs:   []string{"src-1", "src-2"},
			},
		}
		respBody, _ := jsonrs.Marshal(resp)
		f.setServerResponse(syncResponse{
			expectedUrl: fmt.Sprintf("/dataplane/namespaces/%s/regulations/suppressions", namespaceIdentifier.Namespace),
			statusCode:  200,
			respBody:    respBody,
		})
		f.startSyncLoop(t, namespaceIdentifier)
		require.Eventually(t, func() bool {
			return f.isSuppressed("workspace-2", "user-2", "src-1")
		}, time.Second, 10*time.Millisecond)
		require.Eventually(t, func() bool {
			return f.isSuppressed("workspace-1", "user-1", "src-1")
		}, time.Second, 10*time.Millisecond)
	})
}

type readerFunc func(p []byte) (n int, err error)

func (f readerFunc) Read(p []byte) (n int, err error) {
	return f(p)
}

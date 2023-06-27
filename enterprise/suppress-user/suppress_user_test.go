package suppression

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/services/controlplane/identity"
)

var _ = Describe("Suppress user", func() {
	Context("memory", func() {
		generateTests(func() Repository {
			return NewMemoryRepository(logger.NewLogger())
		})
	})

	Context("badgerdb", func() {
		generateTests(func() Repository {
			p := path.Join(GinkgoT().TempDir(), strings.ReplaceAll(uuid.New().String(), "-", ""))
			r, err := NewBadgerRepository(p, logger.NOP)
			Expect(err).To(BeNil())
			return r
		})
	})
})

func generateTests(getRepo func() Repository) {
	type syncResponse struct {
		expectedUrl string
		statusCode  int
		respBody    []byte
	}
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

	var h *handler
	var serverResponse syncResponse
	var server *httptest.Server
	newTestServer := func() *httptest.Server {
		var count int
		var prevRespBody []byte
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if serverResponse.expectedUrl != "" {
				Expect(r.URL.Path).To(Equal(serverResponse.expectedUrl))
			}
			var respBody []byte
			// send the expected payload if it is the first time or the payload has changed
			if count == 0 || prevRespBody != nil && !bytes.Equal(prevRespBody, serverResponse.respBody) {
				respBody = serverResponse.respBody
				prevRespBody = serverResponse.respBody
				count++
			} else { // otherwise send an response containing no items
				respBody, _ = json.Marshal(suppressionsResponse{
					Token: "tempToken123",
				})
			}

			w.WriteHeader(serverResponse.statusCode)
			_, _ = w.Write(respBody)
		}))
	}

	BeforeEach(func() {
		config.Reset()
		backendconfig.Init()
		server = newTestServer()

		r := getRepo()
		h = newHandler(r, logger.NOP)
	})
	AfterEach(func() {
		server.Close()
	})
	Context("sync error scenarios", func() {
		It("returns an error when a wrong server address is provided", func() {
			_, _, err := MustNewSyncer("", identifier, h.r).sync(nil)
			Expect(err.Error()).NotTo(Equal(nil))
		})

		It("returns an error when server responds with HTTP 500", func() {
			serverResponse = syncResponse{
				statusCode: 500,
				respBody:   []byte(""),
			}
			_, _, err := MustNewSyncer(server.URL, identifier, h.r).sync(nil)
			Expect(err.Error()).To(Equal("failed to fetch source regulations: statusCode: 500"))
		})

		It("returns an error when server responds with invalid (empty) data in the response body", func() {
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   []byte(""),
			}
			_, _, err := MustNewSyncer(server.URL, identifier, h.r).sync(nil)
			Expect(err.Error()).To(Equal("unexpected end of JSON input"))
		})

		It("returns an error when server responds with invalid (corrupted json) data in the response body", func() {
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   []byte("{w"),
			}
			_, _, err := MustNewSyncer(server.URL, identifier, h.r).sync(nil)
			Expect(err.Error()).To(Equal("invalid character 'w' looking for beginning of object key string"))
		})

		It("returns an error when server responds with no token in the response body", func() {
			resp := defaultResponse
			resp.Token = ""
			respBody, _ := json.Marshal(resp)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			suppressions, _, err := MustNewSyncer(server.URL, identifier, h.r).sync(nil)
			Expect(err.Error()).To(Equal("no token returned in regulation API response"))
			Expect(suppressions).To(Equal(defaultResponse.Items))
		})
	})

	Context("handler, repository, syncer integration", func() {
		It("exact user suppression match", func() {
			respBody, _ := json.Marshal(defaultResponse)
			serverResponse = syncResponse{
				expectedUrl: fmt.Sprintf("/dataplane/workspaces/%s/regulations/suppressions", identifier.WorkspaceID),
				statusCode:  200,
				respBody:    respBody,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := MustNewSyncer(server.URL, identifier, h.r, WithPollIntervalFn(func() time.Duration { return 1 * time.Millisecond }))
			go func() {
				s.SyncLoop(ctx)
			}()
			Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-1", "src-1") != nil }).Should(BeTrue())
		})

		It("user suppression added, then cancelled", func() {
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
			respBody, _ := json.Marshal(resp)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := MustNewSyncer(server.URL, identifier, h.r, WithPollIntervalFn(func() time.Duration { return 1 * time.Millisecond }))
			go func() {
				s.SyncLoop(ctx)
			}()
			Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-2", "src-1") != nil }).Should(BeTrue())
			Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-1", "src-1") != nil }).Should(BeTrue())

			resp.Items[0].Canceled = true
			respBody, _ = json.Marshal(resp)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-1", "src-1") != nil }).Should(BeFalse())
		})

		It("wildcard user suppression match", func() {
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
			respBody, _ := json.Marshal(resp)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := MustNewSyncer(server.URL, identifier, h.r, WithPollIntervalFn(func() time.Duration { return 1 * time.Millisecond }))
			go func() {
				s.SyncLoop(ctx)
			}()
			Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-1", "src-1") != nil }).Should(BeTrue())
			Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-2", "src-2") != nil }).Should(BeTrue())
			Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-2", "src-1") != nil }).Should(BeFalse())
		})

		It("wildcard user suppression rule added and then cancelled", func() {
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
			respBody, _ := json.Marshal(resp)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := MustNewSyncer(server.URL, identifier, h.r, WithPollIntervalFn(func() time.Duration { return 1 * time.Millisecond }))
			go func() {
				s.SyncLoop(ctx)
			}()

			Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-1", "src-1") != nil }).Should(BeTrue())
			Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-2", "src-2") != nil }).Should(BeTrue())
			Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-2", "src-1") != nil }).Should(BeFalse())

			resp.Items[0].Canceled = true
			respBody, _ = json.Marshal(resp)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-1", "src-1") != nil }).Should(BeFalse())
		})

		It("try to sync while restoring", func() {
			respBody, _ := json.Marshal(defaultResponse)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var r readerFunc = func(_ []byte) (int, error) {
				time.Sleep(1 * time.Second)
				return 0, errors.New("read error")
			}
			go func() {
				err := h.r.Restore(r)
				Expect(err).To(Not(BeNil()))
			}()
			s := MustNewSyncer(server.URL, identifier, h.r, WithPollIntervalFn(func() time.Duration { return 1 * time.Millisecond }))
			go func() {
				s.SyncLoop(ctx)
			}()
			Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-1", "src-1") != nil }, 5*time.Second, 100*time.Millisecond).Should(BeTrue())
		})
	})

	Context("multi-tenant support", func() {
		It("supports older version of regulation service", func() {
			// older version of regulation service doesn't return workspaceID as part of the suppressions
			resp := defaultResponse
			sup := &resp.Items[0]
			sup.WorkspaceID = "workspace-1"
			respBody, _ := json.Marshal(resp)
			serverResponse = syncResponse{
				statusCode: 200,
				respBody:   respBody,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := MustNewSyncer(server.URL, identifier, h.r, WithPollIntervalFn(func() time.Duration { return 1 * time.Millisecond }))
			go func() {
				s.SyncLoop(ctx)
			}()
			Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-1", "src-1") != nil }).Should(BeTrue())
		})
	})

	It("supports syncing suppressions for namespace", func() {
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
		respBody, _ := json.Marshal(resp)
		serverResponse = syncResponse{
			expectedUrl: fmt.Sprintf("/dataplane/namespaces/%s/regulations/suppressions", namespaceIdentifier.Namespace),
			statusCode:  200,
			respBody:    respBody,
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s := MustNewSyncer(server.URL, namespaceIdentifier, h.r, WithPollIntervalFn(func() time.Duration { return 1 * time.Millisecond }))
		go func() {
			s.SyncLoop(ctx)
		}()
		Eventually(func() bool { return h.GetSuppressedUser("workspace-2", "user-2", "src-1") != nil }).Should(BeTrue())
		Eventually(func() bool { return h.GetSuppressedUser("workspace-1", "user-1", "src-1") != nil }).Should(BeTrue())
	})
}

type readerFunc func(p []byte) (n int, err error)

func (f readerFunc) Read(p []byte) (n int, err error) {
	return f(p)
}

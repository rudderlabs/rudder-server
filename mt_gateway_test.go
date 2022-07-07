package main

import (
	"context"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/ory/dockertest/v3"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-server/app"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/testhelper/destination"
	thEtcd "github.com/rudderlabs/rudder-server/testhelper/etcd"
	"github.com/rudderlabs/rudder-server/testhelper/health"
	"github.com/rudderlabs/rudder-server/testhelper/rand"
	whUtil "github.com/rudderlabs/rudder-server/testhelper/webhook"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
)

func TestMultiTenantGateway(t *testing.T) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-c; cancel() }()
	defer cancel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	var (
		group             errgroup.Group
		etcdContainer     *thEtcd.Resource
		postgresContainer *destination.PostgresResource
		serverInstanceID  = "1"
	)

	group.Go(func() (err error) {
		postgresContainer, err = destination.SetupPostgres(pool, t)
		return err
	})
	group.Go(func() (err error) {
		etcdContainer, err = thEtcd.Setup(pool, t)
		return err
	})
	require.NoError(t, group.Wait())

	webhook := whUtil.NewRecorder()
	t.Cleanup(webhook.Close)

	writeKey := rand.String(27)
	workspaceID := rand.String(27)
	workspaces := map[string]backendconfig.ConfigT{
		workspaceID: {
			EnableMetrics: false,
			WorkspaceID:   workspaceID,
			Sources: []backendconfig.SourceT{
				{
					ID:          "xxxyyyzzEaEurW247ad9WYZLUyk",
					Name:        "Dev Integration Test 1",
					WriteKey:    writeKey,
					WorkspaceID: workspaceID,
					Enabled:     true,
					SourceDefinition: backendconfig.SourceDefinitionT{
						ID:   "xxxyyyzzpWDzNxgGUYzq9sZdZZB",
						Name: "HTTP",
					},
					Destinations: []backendconfig.DestinationT{
						{
							ID:                 "xxxyyyzzP9kQfzOoKd1tuxchYAG",
							Name:               "Dev WebHook Integration Test 1",
							Enabled:            true,
							IsProcessorEnabled: false,
							Config: map[string]interface{}{
								"webhookUrl":    webhook.Server.URL,
								"webhookMethod": "POST",
							},
							DestinationDefinition: backendconfig.DestinationDefinitionT{
								ID:          "xxxyyyzzSOU9pLRavMf0GuVnWV3",
								Name:        "WEBHOOK",
								DisplayName: "Webhook",
								Config: map[string]interface{}{
									"destConfig": map[string]interface{}{
										"defaultConfig": []string{"webhookUrl", "webhookMethod", "headers"},
									},
									"secretKeys":           []string{"headers.to"},
									"supportedSourceTypes": []string{"web"},
									"supportedMessageTypes": []string{
										"alias",
										"group",
										"identify",
										"page",
										"screen",
										"track",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	marshalledWorkspaces, err := json.Marshal(&workspaces)
	require.NoError(t, err)

	backendConfRouter := mux.NewRouter()
	if testing.Verbose() {
		backendConfRouter.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Logf("BackendConfig server call: %+v", r)
				next.ServeHTTP(w, r)
			})
		})
	}
	backendConfRouter.HandleFunc("/hostedWorkspaceConfig", func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("Unexpected call to /hostedWorkspaceConfig given that hosted=false")
	}).Methods("GET")
	backendConfRouter.HandleFunc("/multitenantWorkspaceConfig", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, `["`+workspaceID+`"]`, r.FormValue("workspaceIds"))
		_, _ = w.Write(marshalledWorkspaces)
	}).Methods("GET")

	backendConfigSrv := httptest.NewServer(backendConfRouter)
	t.Logf("BackendConfig server listening on: %s", backendConfigSrv.URL)
	t.Cleanup(backendConfigSrv.Close)

	httpPort, err := freeport.GetFreePort()
	require.NoError(t, err)
	httpAdminPort, err := freeport.GetFreePort()
	require.NoError(t, err)

	rudderTmpDir, err := os.MkdirTemp("", "rudder_server_*_test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(rudderTmpDir) })

	releaseName := t.Name()
	t.Setenv("APP_TYPE", app.GATEWAY)
	t.Setenv("INSTANCE_ID", serverInstanceID)
	t.Setenv("RELEASE_NAME", releaseName)
	t.Setenv("ETCD_HOSTS", etcdContainer.Hosts[0])
	t.Setenv("JOBS_DB_PORT", postgresContainer.Port)
	t.Setenv("JOBS_DB_USER", postgresContainer.User)
	t.Setenv("JOBS_DB_DB_NAME", postgresContainer.Database)
	t.Setenv("JOBS_DB_PASSWORD", postgresContainer.Password)
	t.Setenv("CONFIG_BACKEND_URL", backendConfigSrv.URL)
	t.Setenv("RSERVER_GATEWAY_WEB_PORT", strconv.Itoa(httpPort))
	t.Setenv("RSERVER_GATEWAY_ADMIN_WEB_PORT", strconv.Itoa(httpAdminPort))
	t.Setenv("RSERVER_ENABLE_STATS", "false")
	t.Setenv("RSERVER_BACKEND_CONFIG_USE_HOSTED_BACKEND_CONFIG", "false")
	t.Setenv("RUDDER_TMPDIR", rudderTmpDir)
	t.Setenv("HOSTED_MULTITENANT_SERVICE_SECRET", "so-secret")
	t.Setenv("DEPLOYMENT_TYPE", string(deployment.MultiTenantType))
	if testing.Verbose() {
		require.NoError(t, os.Setenv("LOG_LEVEL", "DEBUG"))
	}

	// The Gateway will not become healthy until we trigger a valid configuration via ETCD
	// TODO: this is to be reviewed after "Review health checkpoint (probes)
	// https://www.notion.so/rudderstacks/Review-health-checkpoint-probes-ec33b45c1b7541f3bf802f3276667920
	etcdReqKey := getGatewayWorkspacesReqKey(releaseName, serverInstanceID)
	_, err = etcdContainer.Client.Put(ctx, etcdReqKey, `{"workspaces":"`+workspaceID+`","ack_key":"test-ack/1"}`)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx)
	}()
	t.Cleanup(func() { cancel(); <-done })

	serviceHealthEndpoint := fmt.Sprintf("http://localhost:%d/health", httpPort)
	t.Log("serviceHealthEndpoint", serviceHealthEndpoint)
	health.WaitUntilReady(ctx, t,
		serviceHealthEndpoint,
		time.Minute,
		250*time.Millisecond,
		"serviceHealthEndpoint",
	)

	select {
	case ack := <-etcdContainer.Client.Watch(ctx, "test-ack/1"):
		v, err := unmarshalWorkspaceAckValue(t, &ack)
		require.NoError(t, err)
		require.Equal(t, "RELOADED", v.Status)
		require.Equal(t, "", v.Error)
	case <-time.After(20 * time.Second):
		t.Fatal("Timeout waiting for test-ack/1")
	}

	// Test basic Gateway happy path
	t.Run("EventsAreReceived", func(t *testing.T) {
		require.Empty(t, webhook.Requests(), "webhook should have no requests before sending the events")
		sendEventsToGateway(t, httpPort, writeKey)
		t.Cleanup(func() {
			_, _ = postgresContainer.DB.ExecContext(ctx, `DELETE FROM gw_jobs_1 WHERE workspace_id = $1`, workspaceID)
		})

		var (
			eventPayload string
			message      map[string]interface{}
		)
		require.Eventually(t, func() bool {
			return postgresContainer.DB.QueryRowContext(ctx,
				"SELECT event_payload FROM gw_jobs_1 WHERE workspace_id = $1", workspaceID,
			).Scan(&eventPayload) == nil
		}, time.Minute, 50*time.Millisecond)
		require.NoError(t, json.Unmarshal([]byte(eventPayload), &message))

		batch, ok := message["batch"].([]interface{})
		require.True(t, ok)
		require.Len(t, batch, 1)
		require.Equal(t, message["writeKey"], writeKey)
		for _, msg := range batch {
			m, ok := msg.(map[string]interface{})
			require.True(t, ok)
			require.Equal(t, "anonymousId_1", m["anonymousId"])
			require.Equal(t, "identified_user_id", m["userId"])
			require.Equal(t, "identify", m["type"])
			require.Equal(t, "1", m["eventOrderNo"])
			require.Equal(t, "messageId_1", m["messageId"])
		}

		// Only the Gateway is running, so we don't expect any destinations to be hit.
		require.Empty(t, webhook.Requests(), "webhook should have no requests because there is no processor")
	})

	// Trigger degraded mode, the Gateway should still work
	t.Run("ServerModeDegraded", func(t *testing.T) {
		serverModeReqKey := getETCDServerModeReqKey(releaseName, serverInstanceID)
		t.Logf("Server mode ETCD key: %s", serverModeReqKey)

		_, err := etcdContainer.Client.Put(ctx, serverModeReqKey, `{"mode":"DEGRADED","ack_key":"test-ack/2"}`)
		require.NoError(t, err)
		t.Log("Triggering degraded mode")

		select {
		case ack := <-etcdContainer.Client.Watch(ctx, "test-ack/", clientv3.WithPrefix()):
			require.Len(t, ack.Events, 1)
			require.Equal(t, "test-ack/2", string(ack.Events[0].Kv.Key))
			require.Equal(t, `{"status":"DEGRADED"}`, string(ack.Events[0].Kv.Value))
		case <-time.After(20 * time.Second):
			t.Fatal("Timeout waiting for server-mode test-ack")
		}

		sendEventsToGateway(t, httpPort, writeKey)
		t.Cleanup(func() {
			_, _ = postgresContainer.DB.ExecContext(ctx, `DELETE FROM gw_jobs_1 WHERE workspace_id = $1`, workspaceID)
		})
		require.Eventually(t, func() bool {
			var count int
			err := postgresContainer.DB.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM gw_jobs_1 WHERE workspace_id = $1", workspaceID,
			).Scan(&count)
			if err != nil {
				return false
			}
			return count == 1
		}, time.Minute, 50*time.Millisecond)
	})

	// Checking that Workspace Changes errors are handled
	t.Run("InvalidWorkspaceChangeTermination", func(t *testing.T) {
		// do not move this test up because the gateway terminates after a configuration error
		_, err := etcdContainer.Client.Put(ctx, etcdReqKey, `{"workspaces":",,,","ack_key":"test-ack/2"}`)
		require.NoError(t, err)
		select {
		case ack := <-etcdContainer.Client.Watch(ctx, "test-ack/2"):
			v, err := unmarshalWorkspaceAckValue(t, &ack)
			require.NoError(t, err)
			require.Equal(t, "ERROR", v.Status)
			require.NotEqual(t, "", v.Error)
		case <-time.After(20 * time.Second):
			t.Fatal("Timeout waiting for test-ack/2")
		}
	})
}

func getGatewayWorkspacesReqKey(releaseName, instance string) string {
	return getETCDWorkspacesReqKey(releaseName, instance, app.GATEWAY)
}

func getETCDServerModeReqKey(releaseName, instance string) string {
	return fmt.Sprintf("/%s/SERVER/%s/MODE", releaseName, instance)
}

func getETCDWorkspacesReqKey(releaseName, instance, appType string) string {
	return fmt.Sprintf("/%s/SERVER/%s/%s/WORKSPACES", releaseName, instance, appType)
}

func sendEventsToGateway(t *testing.T, httpPort int, writeKey string) {
	payload1 := strings.NewReader(`{
		"userId": "identified_user_id",
		"anonymousId":"anonymousId_1",
		"messageId":"messageId_1",
		"type": "identify",
		"eventOrderNo":"1",
		"context": {
		  "traits": {
			 "trait1": "new-val"
		  },
		  "ip": "14.5.67.21",
		  "library": {
			  "name": "http"
		  }
		},
		"timestamp": "2020-02-02T00:23:09.544Z"
	}`)
	sendEvent(t, httpPort, payload1, "identify", writeKey)
}

func sendEvent(t *testing.T, httpPort int, payload *strings.Reader, callType, writeKey string) {
	t.Helper()
	t.Logf("Sending %s Event", callType)

	var (
		httpClient = &http.Client{}
		method     = "POST"
		url        = fmt.Sprintf("http://localhost:%d/v1/%s", httpPort, callType)
	)

	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		t.Logf("sendEvent error: %v", err)
		return
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Basic %s", b64.StdEncoding.EncodeToString(
		[]byte(fmt.Sprintf("%s:", writeKey)),
	)))

	res, err := httpClient.Do(req)
	if err != nil {
		t.Logf("sendEvent error: %v", err)
		return
	}
	defer func() { _ = res.Body.Close() }()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Logf("sendEvent error: %v", err)
		return
	}
	if res.Status != "200 OK" {
		return
	}

	t.Logf("Event Sent Successfully: (%s)", body)
}

type workspaceAckValue struct {
	Status string `json:"status"`
	Error  string `json:"error"`
}

func unmarshalWorkspaceAckValue(t *testing.T, res *clientv3.WatchResponse) (workspaceAckValue, error) {
	t.Helper()
	var v workspaceAckValue
	if len(res.Events) == 0 {
		return v, fmt.Errorf("no events in the response")
	}
	if err := json.Unmarshal(res.Events[0].Kv.Value, &v); err != nil {
		return v, fmt.Errorf("could not unmarshal key value response: %v", err)
	}
	return v, nil
}

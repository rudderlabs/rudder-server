package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	suppressModel "github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

const (
	namespaceID = "spaghetti"
	tokenKey    = "__token__"
)

func makeHTTPRequest(method, url string, payload io.Reader) (int, []byte, error) {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = res.Body.Close() }()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, nil, err
	}
	return res.StatusCode, body, nil
}

func backupHasSuppression(filePath string, data []byte) (bool, error) {
	subDirName, err := os.MkdirTemp(filePath, "")
	defer func() { _ = os.RemoveAll(subDirName) }()
	if err != nil {
		return false, err
	}
	repo, err := suppression.NewBadgerRepository(subDirName, logger.NOP)
	if err != nil {
		return false, err
	}
	defer func() { _ = repo.Stop() }()
	err = repo.Restore(bytes.NewReader(data))
	if err != nil {
		return false, err
	}
	isSuppressed, err := repo.Suppressed("workspace-1", "user-1", "src-1")
	if err != nil {
		return false, err
	}
	return isSuppressed != nil, nil
}

func TestMain(t *testing.T) {
	config.Reset()
	logger.Reset()
	misc.Init()
	srv := httptest.NewServer(handler(t))
	defer t.Cleanup(srv.Close)
	t.Setenv("WORKSPACE_TOKEN", "216Co97d9So9TkqphM0cxBzRxc3")
	t.Setenv("CONFIG_BACKEND_URL", srv.URL)
	t.Setenv("SUPPRESS_USER_BACKEND_URL", srv.URL)
	ctx := context.Background()
	go func() { require.NoError(t, Run(ctx)) }()
	tests := []struct {
		name                 string
		endpoint             string
		method               string
		expectedResponseCode int
		expectedResponseBody string
		exportVerifyFileName string
	}{
		{
			name:                 "full export e-2-e test",
			endpoint:             "/full-export",
			method:               http.MethodGet,
			expectedResponseCode: http.StatusOK,
			exportVerifyFileName: "full-export-restore",
		},
		{
			name:                 "latest export e-2-e test",
			endpoint:             "/latest-export",
			method:               http.MethodGet,
			expectedResponseCode: http.StatusOK,
			exportVerifyFileName: "latest-export-restore",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Eventually(t, func() bool {
				res, err := http.Get("http://localhost:8000" + "/health")
				if err == nil {
					_ = res.Body.Close()
				}
				return err == nil && res.StatusCode == http.StatusOK
			}, 10*time.Second, 100*time.Millisecond, "server should start and be ready")

			var lastErr error
			require.Eventually(t, func() bool {
				code, body, err := makeHTTPRequest(tt.method, fmt.Sprintf("http://localhost:%s%s", "8000", tt.endpoint), http.NoBody)
				if err != nil {
					lastErr = err
					return false
				}
				if code != tt.expectedResponseCode {
					lastErr = fmt.Errorf("unexpected response status %d", code)
					return false
				}
				exportBaseDir, err := exportPath()
				if err != nil {
					lastErr = err
					return false
				}
				ok, err := backupHasSuppression(exportBaseDir, body)
				if err != nil {
					lastErr = err
					return false
				}
				lastErr = nil
				return ok
			}, 10*time.Second, 1*time.Second, "should be able to get restorable response from "+tt.endpoint)
			require.NoError(t, lastErr)
		})
	}
	defer func() {
		path, _ := exportPath()
		_ = os.RemoveAll(path)
	}()
}

func handler(t *testing.T) http.Handler {
	t.Helper()
	srvMux := chi.NewMux()
	srvMux.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req)
		})
	})
	srvMux.Get("/dataplane/workspaces/{workspace_id}/regulations/suppressions", getSuppressions)
	srvMux.Get("/dataplane/namespaces/{namespace_id}/regulations/suppressions", getSuppressions)
	srvMux.Get("/workspaceConfig", getSingleTenantWorkspaceConfig)
	srvMux.Get("/data-plane/v1/namespaces/{namespace_id}/config", getMultiTenantNamespaceConfig)

	return srvMux
}

func getSuppressions(w http.ResponseWriter, r *http.Request) {
	defaultSuppression := suppressModel.Suppression{
		Canceled:    false,
		WorkspaceID: "workspace-1",
		UserID:      "user-1",
		SourceIDs:   []string{"src-1", "src-2"},
	}
	respStruct := suppressionsResponse{
		Items: []suppressModel.Suppression{defaultSuppression},
		Token: tokenKey,
	}
	w.Header().Set("Content-Type", "application/json")
	pt := r.URL.Query().Get("pageToken")
	var body []byte
	var err error
	if pt == tokenKey {
		w.WriteHeader(http.StatusOK)
		body, err = jsonrs.Marshal(suppressionsResponse{Token: tokenKey})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		body, err = jsonrs.Marshal(respStruct)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	_, _ = w.Write(body)
}

type suppressionsResponse struct {
	Items []suppressModel.Suppression `json:"items"`
	Token string                      `json:"token"`
}

func getSingleTenantWorkspaceConfig(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	config := backendconfig.ConfigT{
		WorkspaceID: "reg-test-workspaceId",
	}
	body, err := jsonrs.Marshal(config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}

func getMultiTenantNamespaceConfig(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	config := map[string]backendconfig.ConfigT{namespaceID: {
		WorkspaceID: "reg-test-workspaceId",
	}}
	body, err := jsonrs.Marshal(config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}

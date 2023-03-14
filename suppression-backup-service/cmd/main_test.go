package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	suppression "github.com/rudderlabs/rudder-server/enterprise/suppress-user"
	suppressModel "github.com/rudderlabs/rudder-server/enterprise/suppress-user/model"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/stretchr/testify/require"
)

const (
	namespaceID = "spaghetti"
	tokenKey    = "__token__"
)

func makeHTTPRequest(t *testing.T, method, url string, payload io.Reader) (int, []byte) {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, payload)
	require.NoError(t, err)
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	require.NoError(t, err, "should be able to make http request to "+method+" "+url)
	defer func() { _ = res.Body.Close() }()

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err, "should be able to read http response body from "+method+" "+url)
	return res.StatusCode, body
}

func verifyBackup(t *testing.T, filePath, data string) {
	randBytes := make([]byte, 8)
	_, err := rand.Read(randBytes)
	require.NoError(t, err)
	subDirName := filepath.Join(filePath, "subdir-"+fmt.Sprintf("%x", randBytes))
	require.NoError(t, os.Mkdir(subDirName, 0o755))
	repo, err := suppression.NewBadgerRepository(subDirName, logger.NOP)
	require.NoError(t, err)
	err = repo.Restore(strings.NewReader(data))
	require.NoError(t, err)
	isSuppressed, err := repo.Suppressed("workspace-1", "user-1", "src-1")
	require.NoError(t, err)
	require.True(t, isSuppressed)
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

			require.Eventually(t, func() bool {
				code, body := makeHTTPRequest(t, tt.method, fmt.Sprintf("http://localhost:%s%s", "8000", tt.endpoint), http.NoBody)
				exportBaseDir, err := exportPath()
				require.NoError(t, err)
				verifyBackup(t, exportBaseDir, string(body))
				return code == tt.expectedResponseCode
			}, 10*time.Second, 1*time.Second, "should be able to get response from "+tt.endpoint)
		})
	}
}

func handler(t *testing.T) http.Handler {
	t.Helper()
	srvMux := mux.NewRouter()
	srvMux.HandleFunc("/dataplane/workspaces/{workspace_id}/regulations/suppressions", getSuppressions).Methods(http.MethodGet)
	srvMux.HandleFunc("/dataplane/namespaces/{namespace_id}/regulations/suppressions", getSuppressions).Methods(http.MethodGet)
	srvMux.HandleFunc("/workspaceConfig", getSingleTenantWorkspaceConfig).Methods(http.MethodGet)
	srvMux.HandleFunc("/data-plane/v1/namespaces/{namespace_id}/config", getMultiTenantNamespaceConfig).Methods(http.MethodGet)

	srvMux.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req)
		})
	})

	return srvMux
}

func getSuppressions(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")

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
	body, err := json.Marshal(respStruct)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
	body, err := json.Marshal(config)
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
	body, err := json.Marshal(config)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}

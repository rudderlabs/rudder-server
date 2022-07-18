package backendconfig_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/stretchr/testify/require"
)

type backendConfigServer struct {
	responses map[string]string
}

func (server *backendConfigServer) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	body, ok := server.responses[req.URL.Path]
	if !ok {
		resp.WriteHeader(http.StatusNotFound)
		return
	}

	resp.WriteHeader(http.StatusOK)
	_, _ = resp.Write([]byte(body))
}

func Test_Namespace_Get(t *testing.T) {

	logger.Init()

	namespace := "free-us-1"

	be := &backendConfigServer{
		responses: map[string]string{
			"/dataPlane/v1/namespace/" + namespace + "/config": `{"workspaceId_1": {"workspaceId": "workspaceId_1"}}`,
		},
	}

	ts := httptest.NewServer(be)
	defer ts.Close()

	client := &backendconfig.NamespaceConfig{
		Namespace:        namespace,
		Token:            "token",
		ConfigBackendURL: ts.URL,
	}

	client.SetUp()
	c, err := client.Get(context.Background(), "workspaceId_1")
	require.NoError(t, err)
	require.Equal(t, "", c.WorkspaceID)

}

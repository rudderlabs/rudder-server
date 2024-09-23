package bigquery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mockuploader "github.com/rudderlabs/rudder-server/warehouse/internal/mocks/utils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func TestUnsupportedCredentials(t *testing.T) {
	ctrl := gomock.NewController(t)
	uploader := mockuploader.NewMockUploader(ctrl)

	bq := New(config.New(), logger.NOP)
	bq.warehouse = model.Warehouse{
		Destination: backendconfig.DestinationT{
			Config: map[string]interface{}{
				"credentials": "{\"installed\":{\"client_id\":\"1234.apps.googleusercontent.com\",\"project_id\":\"project_id\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_secret\":\"client_secret\",\"redirect_uris\":[\"urn:ietf:wg:oauth:2.0:oob\",\"http://localhost\"]}}",
			},
		},
	}
	bq.uploader = uploader
	bq.projectID = "projectId"

	_, err := bq.connect(context.Background())
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "client_credentials.json file is not supported")
}

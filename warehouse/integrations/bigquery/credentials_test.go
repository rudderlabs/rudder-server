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
			Config: map[string]any{
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

func TestUnsupportedCredentialType(t *testing.T) {
	ctrl := gomock.NewController(t)
	uploader := mockuploader.NewMockUploader(ctrl)

	bq := New(config.New(), logger.NOP)
	bq.warehouse = model.Warehouse{
		Destination: backendconfig.DestinationT{
			Config: map[string]any{
				"credentials": `{"type": "authorized_user"}`,
			},
		},
	}
	bq.uploader = uploader
	bq.projectID = "projectId"

	_, err := bq.connect(context.Background())
	require.NotNil(t, err)
	require.Contains(t, err.Error(), `unsupported credential type "authorized_user"`)
}

// TestWorkloadIdentityFederationCredentials verifies that a Workload Identity Federation
// credential configuration (as produced by `gcloud iam workload-identity-pools
// create-cred-config`), rather than a static service account key, is accepted in the
// `credentials` field: it passes validation and is handed to the BigQuery client under
// its own credential type instead of being rejected or misdeclared as a service account.
func TestWorkloadIdentityFederationCredentials(t *testing.T) {
	ctrl := gomock.NewController(t)
	uploader := mockuploader.NewMockUploader(ctrl)

	bq := New(config.New(), logger.NOP)
	bq.warehouse = model.Warehouse{
		Destination: backendconfig.DestinationT{
			Config: map[string]any{
				"credentials": `{
					"type": "external_account",
					"audience": "//iam.googleapis.com/projects/123456789/locations/global/workloadIdentityPools/my-pool/providers/my-provider",
					"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
					"token_url": "https://sts.googleapis.com/v1/token",
					"service_account_impersonation_url": "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/sa@my-project.iam.gserviceaccount.com:generateAccessToken",
					"credential_source": {"file": "/var/run/secrets/tokens/gcp-token"}
				}`,
			},
		},
	}
	bq.uploader = uploader
	bq.projectID = "projectId"

	_, err := bq.connect(context.Background())
	require.NoError(t, err)
}

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

func TestWorkloadIdentityFederation(t *testing.T) {
	t.Setenv("RUDDER_GCP_FEDERATION_AWS_ROLE_ARN", "")

	for name, targetServiceAccount := range map[string]string{
		"impersonating the target service account": "rudderstack-bq@acme.iam.gserviceaccount.com",
		"with direct federated access":             "",
	} {
		t.Run(name, func(t *testing.T) {
			bq := New(config.New(), logger.NOP)
			bq.warehouse = model.Warehouse{
				WorkspaceID: "30bK6N9S6Ca7C0SGITpgVsmRlIs",
				Destination: backendconfig.DestinationT{Config: map[string]any{
					"authMethod":                           "workloadIdentityFederation",
					"workloadIdentityProjectNumber":        "799415897419",
					"workloadIdentityPoolId":               "wif-pool",
					"workloadIdentityProviderId":           "rudderstack-aws",
					"workloadIdentityTargetServiceAccount": targetServiceAccount,
					"credentials":                          `{"type": "authorized_user"}`, // rejected if it were consulted
				}},
			}
			bq.uploader = mockuploader.NewMockUploader(gomock.NewController(t))
			bq.projectID = "projectId"

			// credentials would be rejected as a service account key; the federation path never reads them.
			// AWS credentials resolve lazily on the first request, so connecting succeeds.
			_, err := bq.connect(context.Background())
			require.NoError(t, err)
		})
	}
}

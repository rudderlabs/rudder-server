package bigquery_test

import (
	"context"
	"testing"

	"github.com/rudderlabs/rudder-server/warehouse/bigquery"
	"github.com/stretchr/testify/assert"
)

func TestUnsupportedCredentials(t *testing.T) {
	credentials := bigquery.BQCredentialsT{
		ProjectID:   "projectId",
		Credentials: "{\"installed\":{\"client_id\":\"1234.apps.googleusercontent.com\",\"project_id\":\"project_id\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_secret\":\"client_secret\",\"redirect_uris\":[\"urn:ietf:wg:oauth:2.0:oob\",\"http://localhost\"]}}",
	}

	_, err := bigquery.Connect(context.Background(), &credentials)

	assert.NotNil(t, err)
	assert.EqualError(t, err, "Google Developers Console client_credentials.json file is not supported")

}

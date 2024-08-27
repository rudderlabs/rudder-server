package googlepubsub

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

const (
	projectId = "my-project-id"
	topic     = "my-topic"
)

func Test_Timeout(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 2 * time.Minute

	testConfig, err := SetupTestGooglePubSub(pool, t)
	require.NoError(t, err)

	config := map[string]interface{}{
		"ProjectId": projectId,
		"EventToTopicMap": []map[string]string{
			{"to": topic},
		},
		"TestConfig": testConfig,
	}
	destination := backendconfig.DestinationT{Config: config}

	producer, err := NewProducer(&destination, common.Opts{Timeout: 1 * time.Microsecond})
	if err != nil {
		t.Fatalf("Expected no error, got: %s.", err)
	}
	json := `{"topicId": "my-topic", "message": "{}"}`
	statusCode, respStatus, responseMessage := producer.Produce([]byte(json), nil)

	const expectedStatusCode = 504
	if statusCode != expectedStatusCode {
		t.Errorf("Expected status code %d, got %d.", expectedStatusCode, statusCode)
	}

	const expectedRespStatus = "Failure"
	if respStatus != expectedRespStatus {
		t.Errorf("Expected response status %s, got %s.", expectedRespStatus, respStatus)
	}

	const expectedResponseMessage = "[GooglePubSub] error :: Failed to publish:context deadline exceeded"
	if responseMessage != expectedResponseMessage {
		t.Errorf("Expected response message %s, got %s.", expectedResponseMessage, responseMessage)
	}
}

func TestUnsupportedCredentials(t *testing.T) {
	config := map[string]interface{}{
		"ProjectId": projectId,
		"EventToTopicMap": []map[string]string{
			{"to": topic},
		},
		"Credentials": "{\"installed\":{\"client_id\":\"1234.apps.googleusercontent.com\",\"project_id\":\"project_id\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"auth_provider_x509_cert_url\":\"https://www.googleapis.com/oauth2/v1/certs\",\"client_secret\":\"client_secret\",\"redirect_uris\":[\"urn:ietf:wg:oauth:2.0:oob\",\"http://localhost\"]}}",
	}
	destination := backendconfig.DestinationT{Config: config}

	_, err := NewProducer(&destination, common.Opts{})

	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "client_credentials.json file is not supported")
}

type cleaner interface {
	Cleanup(func())
	Log(...interface{})
}

func SetupTestGooglePubSub(pool *dockertest.Pool, cln cleaner) (*TestConfig, error) {
	var testConfig TestConfig
	pubsubContainer, err := pool.Run("messagebird/gcloud-pubsub-emulator", "latest", []string{
		"PUBSUB_PROJECT1=my-project-id,my-topic1",
	})
	if err != nil {
		return nil, fmt.Errorf("Could not start resource: %s", err)
	}
	cln.Cleanup(func() {
		if err := pool.Purge(pubsubContainer); err != nil {
			cln.Log(fmt.Errorf("could not purge resource: %v", err))
		}
	})
	testConfig.Endpoint = fmt.Sprintf("127.0.0.1:%s", pubsubContainer.GetPort("8681/tcp"))

	client, err := pubsub.NewClient(
		context.Background(),
		projectId,
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		option.WithEndpoint(testConfig.Endpoint))
	if err != nil {
		return nil, err
	}
	if err := pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, err = client.CreateTopic(ctx, topic)
		return err
	}); err != nil {
		return nil, err
	}
	return &testConfig, nil
}

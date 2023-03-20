package googlepubsub

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/ory/dockertest/v3"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/testhelper"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	hold       bool
	testConfig TestConfig
)

const (
	projectId = "my-project-id"
	topic     = "my-topic"
)

func Test_Timeout(t *testing.T) {
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

func TestMain(m *testing.M) {
	flag.BoolVar(&hold, "hold", false, "hold environment clean-up after test execution until Ctrl+C is provided")
	flag.Parse()

	// hack to make defer work, without being affected by the os.Exit in TestMain
	os.Exit(run(m))
}

func run(m *testing.M) int {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	pool.MaxWait = 2 * time.Minute
	if err != nil {
		log.Printf("Could not connect to docker: %s", err)
		return -1
	}
	cleanup := &testhelper.Cleanup{}
	defer cleanup.Run()
	config, err := SetupTestGooglePubSub(pool, cleanup)
	if err != nil {
		log.Printf("Could not start google pubsub service: %s", err)
		return -1
	}
	testConfig = *config
	code := m.Run()
	blockOnHold()
	return code
}

func blockOnHold() {
	if !hold {
		return
	}

	fmt.Println("Test on hold, before cleanup")
	fmt.Println("Press Ctrl+C to exit")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
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

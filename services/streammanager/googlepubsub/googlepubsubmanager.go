package googlepubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/tidwall/gjson"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/googleutil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

type TestConfig struct {
	Endpoint string `json:"endpoint"`
}
type PubsubClient struct {
	pbs      *pubsub.Client
	topicMap map[string]*pubsub.Topic
	opts     common.Opts
}

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("googlepubsub")
}

type GooglePubSubProducer struct {
	client *PubsubClient
}

// NewProducer creates a producer based on destination config
func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (*GooglePubSubProducer, error) {
	ctx := context.Background()
	var endpoint string
	projectId, ok := destination.Config["projectId"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid configuration provided, missing projectId")
	}
	credentials, _ := destination.Config["credentials"].(string)
	testConfig, ok := destination.Config["testConfig"].(map[string]interface{})
	if ok {
		endpoint, _ = testConfig["endpoint"].(string)
	}
	eventToTopicMapArr, ok := destination.Config["eventToTopicMap"].([]interface{})
	var err error
	var client *pubsub.Client
	var options []option.ClientOption
	if endpoint != "" { // Normal configuration requires credentials
		opts := []option.ClientOption{
			option.WithoutAuthentication(),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithEndpoint(endpoint),
		}
		options = append(options, opts...)
	} else if !googleutil.ShouldSkipCredentialsInit(credentials) { // Test configuration requires a custom endpoint
		credsBytes := []byte(credentials)
		if err := googleutil.CompatibleGoogleCredentialsJSON(credsBytes); err != nil {
			return nil, fmt.Errorf("[GooglePubSub] error :: %w", err)
		}
		options = append(options, option.WithCredentialsJSON(credsBytes))
	}
	if client, err = pubsub.NewClient(ctx, projectId, options...); err != nil {
		return nil, err
	}
	topicMap := make(map[string]*pubsub.Topic, len(eventToTopicMapArr))
	for _, s := range eventToTopicMapArr {
		sMap := s.(map[string]interface{})
		topic := client.Topic(sMap["to"].(string))
		topic.PublishSettings.DelayThreshold = 0
		topicMap[sMap["to"].(string)] = topic
	}
	pkgLogger.Infof("topicMap: %+v", topicMap)
	return &GooglePubSubProducer{client: &PubsubClient{client, topicMap, o}}, nil
}

func (producer *GooglePubSubProducer) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessage string) {
	parsedJSON := gjson.ParseBytes(jsonData)
	message := parsedJSON.Get("message").String()
	attributes := parsedJSON.Get("attributes").Map()
	pbs := producer.client
	if pbs == nil {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error :: Could not create producer"
		return 400, respStatus, responseMessage
	}
	ctx, cancel := context.WithTimeout(context.Background(), pbs.opts.Timeout)
	defer cancel()

	if message == "" {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error :: message from payload not found"
		return 400, respStatus, responseMessage
	}
	value := []byte(message)
	topicId := parsedJSON.Get("topicId").String()
	if topicId == "" {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error :: empty topic id string"
		return 400, respStatus, responseMessage
	}
	topic, ok := pbs.topicMap[topicId]
	if !ok {
		statusCode = 400
		responseMessage = "[GooglePubSub] error :: Topic not found in project"
		respStatus = "Failure"
		return statusCode, respStatus, responseMessage
	}
	var result *pubsub.PublishResult

	if len(attributes) != 0 {
		attributesMap := make(map[string]string)
		for k, v := range attributes {
			attributesMap[k] = v.Str
		}
		result = topic.Publish(
			ctx,
			&pubsub.Message{
				Data:       value,
				Attributes: attributesMap,
			},
		)
	} else {
		result = topic.Publish(
			ctx,
			&pubsub.Message{
				Data: value,
			},
		)
	}

	serverID, err := result.Get(ctx)
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, context.DeadlineExceeded) {
			statusCode = 504
		} else {
			statusCode = getError(err)
		}
		responseMessage = "[GooglePubSub] error :: Failed to publish:" + err.Error()
		respStatus = "Failure"
		return statusCode, respStatus, responseMessage
	}
	return 200, "Success", "Message publish with serverID: " + serverID
}

// Close closes a given producer
func (producer *GooglePubSubProducer) Close() error {
	var err error
	client := producer.client
	if client != nil {
		for _, s := range client.topicMap {
			s.Stop()
		}
		err := client.pbs.Close()
		if err != nil {
			pkgLogger.Errorf("error in closing Google Pub/Sub producer: %s", err.Error())
		}
	}
	return err
}

func getError(err error) (statusCode int) {
	switch status.Code(err) {
	case codes.Canceled:
		statusCode = 499
	case codes.Unknown:
	case codes.InvalidArgument:
	case codes.FailedPrecondition:
	case codes.Aborted:
	case codes.OutOfRange:
	case codes.Unimplemented:
	case codes.DataLoss:
		statusCode = 400
	case codes.DeadlineExceeded:
		statusCode = 504
	case codes.NotFound:
		statusCode = 404
	case codes.AlreadyExists:
		statusCode = 409
	case codes.PermissionDenied:
		statusCode = 403
	case codes.ResourceExhausted:
		statusCode = 429
	case codes.Internal:
		statusCode = 500
	case codes.Unavailable:
		statusCode = 503
	case codes.Unauthenticated:
		statusCode = 401
	default:
		statusCode = 400
	}
	return statusCode
}

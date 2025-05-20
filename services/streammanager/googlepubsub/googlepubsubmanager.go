package googlepubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/tidwall/gjson"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/googleutil"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
)

type PubSubConfig struct {
	Credentials     string              `json:"credentials"`
	ProjectId       string              `json:"projectId"`
	EventToTopicMap []map[string]string `json:"eventToTopicMap"`
	TestConfig      TestConfig          `json:"testConfig"`
}

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
	conf   *config.Config
}

// NewProducer creates a producer based on destination config
func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (*GooglePubSubProducer, error) {
	var pubsubConfig PubSubConfig
	ctx := context.Background()
	jsonConfig, err := jsonrs.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("[GooglePubSub] Error while marshalling destination config :: %w", err)
	}
	err = jsonrs.Unmarshal(jsonConfig, &pubsubConfig)
	if err != nil {
		return nil, fmt.Errorf("[GooglePubSub] error  :: error in GooglePubSub while unmarshelling destination config:: %w", err)
	}
	if pubsubConfig.ProjectId == "" {
		return nil, fmt.Errorf("invalid configuration provided, missing projectId")
	}
	var client *pubsub.Client
	var options []option.ClientOption
	if pubsubConfig.TestConfig.Endpoint != "" { // Normal configuration requires credentials
		opts := []option.ClientOption{
			option.WithoutAuthentication(),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithEndpoint(pubsubConfig.TestConfig.Endpoint),
		}
		options = append(options, opts...)
	} else if !googleutil.ShouldSkipCredentialsInit(pubsubConfig.Credentials) { // Test configuration requires a custom endpoint
		credsBytes := []byte(pubsubConfig.Credentials)
		if err = googleutil.CompatibleGoogleCredentialsJSON(credsBytes); err != nil {
			return nil, err
		}
		options = append(options, option.WithCredentialsJSON(credsBytes))
	}
	if client, err = pubsub.NewClient(ctx, pubsubConfig.ProjectId, options...); err != nil {
		return nil, err
	}
	topicMap := make(map[string]*pubsub.Topic, len(pubsubConfig.EventToTopicMap))
	for _, s := range pubsubConfig.EventToTopicMap {
		topic := client.Topic(s["to"])
		topic.PublishSettings.DelayThreshold = config.GetDurationVar(10, time.Millisecond, "StreamManager.GooglePubSub.DelayThreshold")
		topic.PublishSettings.CountThreshold = config.GetIntVar(64, 1, "StreamManager.GooglePubSub.CountThreshold", "Router.GOOGLEPUBSUB.noOfWorkers", "Router.noOfWorkers")
		topic.PublishSettings.ByteThreshold = config.GetIntVar(int(10*bytesize.MB), 1, "StreamManager.GooglePubSub.ByteThreshold")
		topic.PublishSettings.FlowControlSettings = pubsub.FlowControlSettings{
			LimitExceededBehavior: pubsub.FlowControlBlock,
		}
		topic.PublishSettings.FlowControlSettings.MaxOutstandingMessages = config.GetIntVar(1000, 1, "StreamManager.GooglePubSub.MaxOutstandingMessages")
		topic.PublishSettings.FlowControlSettings.MaxOutstandingBytes = config.GetIntVar(-1, 1, "StreamManager.GooglePubSub.MaxOutstandingBytes")
		topicMap[s["to"]] = topic
	}
	return &GooglePubSubProducer{client: &PubsubClient{client, topicMap, o}, conf: config.Default}, nil
}

func (producer *GooglePubSubProducer) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessage string) {
	parsedJSON := gjson.ParseBytes(jsonData)
	pbs := producer.client
	if pbs == nil {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error :: Could not create producer"
		return 400, respStatus, responseMessage
	}
	ctx, cancel := context.WithTimeout(context.Background(), pbs.opts.Timeout)
	defer cancel()

	var data interface{}
	if parsedJSON.Get("message").Value() != nil {
		data = parsedJSON.Get("message").Value()
	} else {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error :: message from payload not found"
		return 400, respStatus, responseMessage
	}
	value, err := jsonrs.Marshal(data)
	if err != nil {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error  :: " + err.Error()
		pkgLogger.Errorf("[GooglePubSub] error  :: %v", err)
		statusCode := 400
		return statusCode, respStatus, responseMessage
	}

	if parsedJSON.Get("topicId").Value() == nil {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error  :: Topic Id not found"
		return 400, respStatus, responseMessage
	}
	topicIdString, ok := parsedJSON.Get("topicId").Value().(string)
	if !ok {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error :: Could not parse topic id to string"
		pkgLogger.Error(responseMessage)
		statusCode := 400
		return statusCode, respStatus, responseMessage
	}
	if topicIdString == "" {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error :: empty topic id string"
		return 400, respStatus, responseMessage
	}
	topic := pbs.topicMap[topicIdString]
	if topic == nil {
		statusCode = 400
		responseMessage = "[GooglePubSub] error :: Topic not found in project"
		respStatus = "Failure"
		return statusCode, respStatus, responseMessage
	}

	attributes := parsedJSON.Get("attributes").Map()
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
	} else {
		responseMessage = "Message publish with serverID" + serverID
	}
	respStatus = "Success"
	return 200, respStatus, responseMessage
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

package googlepubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/rudderlabs/rudder-server/utils/googleutils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Config struct {
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
	opts     Opts
}

type Opts struct {
	Timeout time.Duration
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("googlepubsub")
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}, o Opts) (*PubsubClient, error) {
	var config Config
	ctx := context.Background()
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return nil, fmt.Errorf("[GooglePubSub] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return nil, fmt.Errorf("[GooglePubSub] error  :: error in GooglePubSub while unmarshelling destination config:: %w", err)
	}
	if config.ProjectId == "" {
		return nil, fmt.Errorf("invalid configuration provided, missing projectId")
	}
	var client *pubsub.Client
	if config.Credentials != "" { // Normal configuration requires credentials
		if err = googleutils.CompatibleGoogleCredentialsJSON([]byte(config.Credentials)); err != nil {
			return nil, err
		}
		if client, err = pubsub.NewClient(ctx, config.ProjectId, option.WithCredentialsJSON([]byte(config.Credentials))); err != nil {
			return nil, err
		}
	} else if config.TestConfig.Endpoint != "" { // Test configuration requires a custom endpoint
		if client, err = pubsub.NewClient(ctx, config.ProjectId, option.WithoutAuthentication(), option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())), option.WithEndpoint(config.TestConfig.Endpoint)); err != nil {
			return nil, err
		}
	} else { // No configuration
		return nil, fmt.Errorf("invalid configuration provided, missing credentials")
	}
	topicMap := make(map[string]*pubsub.Topic, len(config.EventToTopicMap))
	for _, s := range config.EventToTopicMap {
		topic := client.Topic(s["to"])
		topic.PublishSettings.DelayThreshold = 0
		topicMap[s["to"]] = topic
	}
	pbsClient := &PubsubClient{client, topicMap, o}
	return pbsClient, nil
}

func Produce(jsonData json.RawMessage, producer, _ interface{}) (statusCode int, respStatus, responseMessage string) {
	parsedJSON := gjson.ParseBytes(jsonData)
	pbs, ok := producer.(*PubsubClient)
	ctx, cancel := context.WithTimeout(context.Background(), pbs.opts.Timeout)
	defer cancel()
	if !ok {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error :: Could not create producer"
		return 400, respStatus, responseMessage
	}
	var data interface{}
	if parsedJSON.Get("message").Value() != nil {
		data = parsedJSON.Get("message").Value()
	} else {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error :: message from payload not found"
		return 400, respStatus, responseMessage
	}
	value, err := json.Marshal(data)
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

// CloseProducer closes a given producer
func CloseProducer(producer interface{}) error {
	client, ok := producer.(*PubsubClient)
	if ok {
		var err error
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
	return fmt.Errorf("error while closing producer")
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

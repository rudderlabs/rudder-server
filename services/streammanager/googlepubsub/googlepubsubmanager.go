package googlepubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/cenkalti/backoff/v4"
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
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
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
	// Retry configuration
	enableRetry          *config.Reloadable[bool]
	retryInitialInterval *config.Reloadable[time.Duration]
	retryMaxInterval     *config.Reloadable[time.Duration]
	retryMaxElapsedTime  *config.Reloadable[time.Duration]
	retryMaxRetries      *config.Reloadable[int]
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
		topic.PublishSettings.NumGoroutines = config.GetIntVar(25, 1, "Router.GOOGLEPUBSUB.Client.NumGoroutines")
		topic.PublishSettings.DelayThreshold = config.GetDurationVar(10, time.Millisecond, "Router.GOOGLEPUBSUB.Client.DelayThreshold")
		topic.PublishSettings.CountThreshold = config.GetIntVar(64, 1, "Router.GOOGLEPUBSUB.Client.CountThreshold", "Router.GOOGLEPUBSUB.noOfWorkers", "Router.noOfWorkers")
		topic.PublishSettings.ByteThreshold = config.GetIntVar(int(10*bytesize.MB), 1, "Router.GOOGLEPUBSUB.Client.ByteThreshold")
		topic.PublishSettings.FlowControlSettings = pubsub.FlowControlSettings{
			LimitExceededBehavior: pubsub.FlowControlBlock,
		}
		topic.PublishSettings.FlowControlSettings.MaxOutstandingMessages = config.GetIntVar(1000, 1, "Router.GOOGLEPUBSUB.Client.MaxOutstandingMessages")
		topic.PublishSettings.FlowControlSettings.MaxOutstandingBytes = config.GetIntVar(-1, 1, "Router.GOOGLEPUBSUB.Client.MaxOutstandingBytes")
		topicMap[s["to"]] = topic
	}

	return &GooglePubSubProducer{
		client: &PubsubClient{client, topicMap, o},
		conf:   config.Default,
		// Initialize retry configuration
		enableRetry:          config.GetReloadableBoolVar(false, "Router.GOOGLEPUBSUB.Client.EnableRetries"),
		retryInitialInterval: config.GetReloadableDurationVar(10, time.Millisecond, "Router.GOOGLEPUBSUB.Client.Retry.InitialInterval"),
		retryMaxInterval:     config.GetReloadableDurationVar(500, time.Millisecond, "Router.GOOGLEPUBSUB.Client.Retry.MaxInterval"),
		retryMaxElapsedTime:  config.GetReloadableDurationVar(2, time.Second, "Router.GOOGLEPUBSUB.Client.Retry.MaxElapsedTime"),
		retryMaxRetries:      config.GetReloadableIntVar(3, 1, "Router.GOOGLEPUBSUB.Client.Retry.MaxRetries"),
	}, nil
}

func (producer *GooglePubSubProducer) publish(ctx context.Context, topic *pubsub.Topic, message *pubsub.Message) (string, error) {
	return topic.Publish(ctx, message).Get(ctx)
}

// publishWithRetry publishes a message with retry logic for intermittent authentication errors
func (producer *GooglePubSubProducer) publishWithRetry(ctx context.Context, topic *pubsub.Topic, message *pubsub.Message) (string, error) {
	var serverID string

	operation := func() error {
		var err error
		serverID, err = producer.publish(ctx, topic, message)
		if err == nil {
			return nil
		}

		// Only retry on specific authentication error
		if strings.Contains(err.Error(), "Request had invalid authentication credentials. Expected OAuth 2 access token, login cookie or other valid authentication credential") {
			pkgLogger.Warnf("[GooglePubSub] Authentication error, retrying: %v", err)
			return err // Return error to trigger retry
		}

		// For non-authentication errors, mark as permanent to avoid retry
		return backoff.Permanent(err)
	}

	// Use configurable exponential backoff
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = producer.retryInitialInterval.Load()
	bo.MaxInterval = producer.retryMaxInterval.Load()
	bo.MaxElapsedTime = producer.retryMaxElapsedTime.Load()
	bo.Multiplier = 2

	// Limit to configured max retries
	boWithMaxRetries := backoff.WithMaxRetries(bo, uint64(producer.retryMaxRetries.Load()))

	err := backoff.RetryNotify(operation, backoff.WithContext(boWithMaxRetries, ctx), func(err error, t time.Duration) {
		pkgLogger.Warnf("[GooglePubSub] Retrying after %v due to authentication error: %v", t, err)
	})

	return serverID, err
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
		pkgLogger.Errorn("[GooglePubSub] error", obskit.Error(err))
		statusCode := 400
		responseMessage = "[GooglePubSub] error  :: " + err.Error()
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
		pkgLogger.Errorn("[GooglePubSub] response error", logger.NewStringField("responseMessage", responseMessage))
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
	var message *pubsub.Message

	if len(attributes) != 0 {
		attributesMap := make(map[string]string)
		for k, v := range attributes {
			attributesMap[k] = v.Str
		}
		message = &pubsub.Message{
			Data:       value,
			Attributes: attributesMap,
		}
	} else {
		message = &pubsub.Message{
			Data: value,
		}
	}

	var serverID string
	if producer.enableRetry.Load() {
		serverID, err = producer.publishWithRetry(ctx, topic, message)
	} else {
		serverID, err = producer.publish(ctx, topic, message)
	}

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
			pkgLogger.Errorn("error in closing Google Pub/Sub producer", obskit.Error(err))
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

package googlepubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	Credentials     string              `json:"credentials"`
	ProjectId       string              `json:"projectId"`
	EventToTopicMap []map[string]string `json:"eventToTopicMap"`
}

type PubsubClient struct {
	Pbs      *pubsub.Client
	TopicMap map[string]*pubsub.Topic
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("googlepubsub")
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (*PubsubClient, error) {
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
	var client *pubsub.Client
	if config.Credentials != "" && config.ProjectId != "" {
		client, err = pubsub.NewClient(ctx, config.ProjectId, option.WithCredentialsJSON([]byte(config.Credentials)))
	}
	if err != nil {
		return nil, err
	}
	var topicMap = make(map[string]*pubsub.Topic, len(config.EventToTopicMap))
	for _, s := range config.EventToTopicMap {
		topic := client.Topic(s["to"])
		topic.PublishSettings.DelayThreshold = 0
		topicMap[s["to"]] = topic
	}
	pbsClient := &PubsubClient{client, topicMap}
	return pbsClient, nil
}
func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (statusCode int, respStatus string, responseMessage string) {
	parsedJSON := gjson.ParseBytes(jsonData)
	pbs, ok := producer.(*PubsubClient)
	ctx := context.Background()
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
	if parsedJSON.Get("topicId").Value() != nil {
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
		topic := pbs.TopicMap[topicIdString]
		if topic == nil {
			statusCode = 400
			responseMessage = "[GooglePubSub] error :: Topic not found in project"
			respStatus = "Failure"
			return statusCode, respStatus, responseMessage
		}
		result := topic.Publish(ctx, &pubsub.Message{Data: []byte(value)})
		serverID, err := result.Get(ctx)
		if err != nil {
			statusCode = getError(err)
			responseMessage = "[GooglePubSub] error :: Failed to publish:" + err.Error()
			respStatus = "Failure"
			return statusCode, respStatus, responseMessage
		} else {
			responseMessage = "Message publish with serverID" + serverID
		}
		respStatus = "Success"
		return 200, respStatus, responseMessage
	} else {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error  :: Topic Id not found"
		return 400, respStatus, responseMessage
	}
}

//CloseProducer closes a given producer
func CloseProducer(producer interface{}) error {
	pbs, ok := producer.(*PubsubClient)
	if ok {
		var err error
		if pbs != nil {
			for _, s := range pbs.TopicMap {
				s.Stop()
			}
			err := pbs.Pbs.Close()
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

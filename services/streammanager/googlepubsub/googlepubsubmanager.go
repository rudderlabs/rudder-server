package googlepubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/tidwall/gjson"
	"google.golang.org/api/option"
)

type Config struct {
	Credentials string
	ProjectId   string
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}) (pubsub.Client, error) {
	var config Config
	ctx := context.Background()
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return pubsub.Client{}, fmt.Errorf("[GooglePubSub] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return pubsub.Client{}, fmt.Errorf("[GooglePubSub] error  :: error in GooglePubSub while unmarshelling destination config:: %w", err)
	}
	var client *pubsub.Client
	if config.Credentials != "" && config.ProjectId != "" {
		client, err = pubsub.NewClient(ctx, config.ProjectId, option.WithCredentialsJSON([]byte(config.Credentials)))
	}
	if err != nil {
		return pubsub.Client{}, err
	}
	return *client, nil
}
func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (statusCode int, respStatus string, responseMessage string) {
	parsedJSON := gjson.ParseBytes(jsonData)
	pbs, ok := producer.(pubsub.Client)
	ctx := context.Background()
	if !ok {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error :: Could not create producer"
		return 400, respStatus, responseMessage
	}
	var config Config
	jsonConfig, err := json.Marshal(destConfig)
	if err != nil {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error :: " + err.Error()
		logger.Errorf("[GooglePubSub] error  :: %w", err)
		statusCode := 400
		return statusCode, respStatus, responseMessage
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error  :: " + err.Error()
		logger.Errorf("[GooglePubSub] error  :: %w", err)
		statusCode := 400
		return statusCode, respStatus, responseMessage
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
		logger.Errorf("[GooglePubSub] error  :: %w", err)
		statusCode := 400
		return statusCode, respStatus, responseMessage
	}
	if parsedJSON.Get("topicId").Value() != nil {
		topicIdString, ok := parsedJSON.Get("topicId").Value().(string)
		if !ok {
			respStatus = "Failure"
			responseMessage = "[GooglePubSub] error :: Could not parse delivery stream to string"
			logger.Error(responseMessage)
			statusCode := 400
			return statusCode, respStatus, responseMessage
		}
		if topicIdString == "" {
			respStatus = "Failure"
			responseMessage = "[GooglePubSub] error :: empty delivery stream"
			return 400, respStatus, responseMessage
		}
		topic := pbs.Topic(topicIdString)
		result := topic.Publish(ctx, &pubsub.Message{Data: []byte(value)})
		result.Ready()

		serverID, err := result.Get(ctx)
		if err != nil {
			respStatus = "Failure"
			responseMessage = "[GooglePubSub] error :: Failed to publish:" + err.Error()
			return 500, respStatus, responseMessage
		} else {
			responseMessage = "Message publish with id %v" + serverID
		}

		respStatus = "Success"
		return 200, respStatus, responseMessage
	} else {
		respStatus = "Failure"
		responseMessage = "[GooglePubSub] error  :: Delivery Stream not found"
		return 400, respStatus, responseMessage
	}
}

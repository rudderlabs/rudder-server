//go:generate mockgen -destination=../../../mocks/services/streammanager/personalize/mock_personalize.go -package mock_personalize github.com/rudderlabs/rudder-server/services/streammanager/personalize PersonalizeClient

package personalize

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/personalizeevents"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

var pkgLogger logger.Logger

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("personalize")
}

type PersonalizeProducer struct {
	client PersonalizeClient
}

type PersonalizeClient interface {
	PutEvents(input *personalizeevents.PutEventsInput) (*personalizeevents.PutEventsOutput, error)
	PutUsers(input *personalizeevents.PutUsersInput) (*personalizeevents.PutUsersOutput, error)
	PutItems(input *personalizeevents.PutItemsInput) (*personalizeevents.PutItemsOutput, error)
}

func NewProducer(destination *backendconfig.DestinationT, o common.Opts) (*PersonalizeProducer, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestination(destination, o.Timeout, personalizeevents.ServiceName)
	if err != nil {
		return nil, err
	}
	awsSession, err := awsutil.CreateSession(sessionConfig)
	if err != nil {
		return nil, err
	}
	return &PersonalizeProducer{client: personalizeevents.New(awsSession)}, nil
}

func (producer *PersonalizeProducer) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessag string) {
	client := producer.client
	if client == nil {
		return 400, "Could not create producer for Personalize", "Could not create producer for Personalize"
	}
	var response interface{}
	var err error

	parsedJSON := gjson.ParseBytes(jsonData)
	eventChoice := parsedJSON.Get("choice").String()
	eventPayload := []byte(parsedJSON.Get("payload").String())

	switch eventChoice {
	case "PutEvents":
		input := personalizeevents.PutEventsInput{}
		err = json.Unmarshal(eventPayload, &input)
		if err != nil {
			return 400, err.Error(), "Could not unmarshal jsonData according to PutEvents input structure"
		}

		if err = input.Validate(); err != nil {
			return 400, err.Error(), "input does not have required fields"
		}
		response, err = client.PutEvents(&input)
	case "PutUsers":
		input := personalizeevents.PutUsersInput{}
		err = json.Unmarshal(eventPayload, &input)
		if err != nil {
			return 400, err.Error(), "Could not unmarshal jsonData according to PutUsers input structure"
		}
		if err = input.Validate(); err != nil {
			return 400, err.Error(), "input does not have required fields"
		}
		response, err = client.PutUsers(&input)
	case "PutItems":
		input := personalizeevents.PutItemsInput{}
		err = json.Unmarshal(eventPayload, &input)
		if err != nil {
			return 400, err.Error(), "Could not unmarshal jsonData according to PutItems input structure"
		}
		if err = input.Validate(); err != nil {
			return 400, err.Error(), "input does not have required fields"
		}
		response, err = client.PutItems(&input)
	default:
		input := personalizeevents.PutEventsInput{}
		err = json.Unmarshal(jsonData, &input)
		if err != nil {
			return 400, err.Error(), "Could not unmarshal jsonData according to PutEvents input structure"
		}
		if err = input.Validate(); err != nil {
			return 400, err.Error(), "input does not have required fields"
		}
		response, err = client.PutEvents(&input)
	}

	if err != nil {
		statusCode, respStatus, responseMessage := common.ParseAWSError(err)
		pkgLogger.Errorf("[Personalize] error  :: %d : %s : %s", statusCode, respStatus, responseMessage)
		return statusCode, respStatus, responseMessage
	}

	return 200, "Success", fmt.Sprintf("Message delivered with Record information %v", response)
}

func (*PersonalizeProducer) Close() error {
	// no-op
	return nil
}

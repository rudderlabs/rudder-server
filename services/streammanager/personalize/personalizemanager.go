//go:generate mockgen -destination=../../../mocks/services/streammanager/personalize_v1/mock_personalize_v1.go -package mock_personalize_v1 github.com/rudderlabs/rudder-server/services/streammanager/personalize PersonalizeClientV1

package personalize

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/personalizeevents"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

type PersonalizeProducerV1 struct {
	client PersonalizeClientV1
}

type PersonalizeClientV1 interface {
	PutEvents(input *personalizeevents.PutEventsInput) (*personalizeevents.PutEventsOutput, error)
	PutUsers(input *personalizeevents.PutUsersInput) (*personalizeevents.PutUsersOutput, error)
	PutItems(input *personalizeevents.PutItemsInput) (*personalizeevents.PutItemsOutput, error)
}

func NewProducerV1(destination *backendconfig.DestinationT, o common.Opts) (common.Producer, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestination(destination, o.Timeout, personalizeevents.ServiceName)
	if err != nil {
		return nil, err
	}
	sessionConfig.MaxIdleConnsPerHost = config.GetIntVar(64, 1, "Router.PERSONALIZE.httpMaxIdleConnsPerHost", "Router.PERSONALIZE.noOfWorkers", "Router.noOfWorkers")
	awsSession, err := awsutil.CreateSession(sessionConfig)
	if err != nil {
		return nil, err
	}
	return &PersonalizeProducerV1{client: personalizeevents.New(awsSession)}, nil
}

func (producer *PersonalizeProducerV1) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessag string) {
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
		err = jsonrs.Unmarshal(eventPayload, &input)
		if err != nil {
			return 400, err.Error(), "Could not unmarshal jsonData according to PutEvents input structure"
		}

		if err = input.Validate(); err != nil {
			return 400, err.Error(), "input does not have required fields"
		}
		response, err = client.PutEvents(&input)
	case "PutUsers":
		input := personalizeevents.PutUsersInput{}
		err = jsonrs.Unmarshal(eventPayload, &input)
		if err != nil {
			return 400, err.Error(), "Could not unmarshal jsonData according to PutUsers input structure"
		}
		if err = input.Validate(); err != nil {
			return 400, err.Error(), "input does not have required fields"
		}
		response, err = client.PutUsers(&input)
	case "PutItems":
		input := personalizeevents.PutItemsInput{}
		err = jsonrs.Unmarshal(eventPayload, &input)
		if err != nil {
			return 400, err.Error(), "Could not unmarshal jsonData according to PutItems input structure"
		}
		if err = input.Validate(); err != nil {
			return 400, err.Error(), "input does not have required fields"
		}
		response, err = client.PutItems(&input)
	default:
		input := personalizeevents.PutEventsInput{}
		err = jsonrs.Unmarshal(jsonData, &input)
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
		pkgLogger.Errorn("[Personalize] error", logger.NewIntField("statusCode", int64(statusCode)), logger.NewStringField("respStatus", respStatus), logger.NewStringField("responseMessage", responseMessage))
		return statusCode, respStatus, responseMessage
	}

	return 200, "Success", fmt.Sprintf("Message delivered with Record information %v", response)
}

func (*PersonalizeProducerV1) Close() error {
	// no-op
	return nil
}

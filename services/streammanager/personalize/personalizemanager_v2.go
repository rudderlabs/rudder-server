//go:generate mockgen -destination=../../../mocks/services/streammanager/personalize_v2/mock_personalize_v2.go -package mock_personalize_v2 github.com/rudderlabs/rudder-server/services/streammanager/personalize PersonalizeClientV2

package personalize

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/personalizeevents"
	"github.com/tidwall/gjson"

	awsutil "github.com/rudderlabs/rudder-go-kit/awsutil_v2"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/streammanager/common"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
)

type PersonalizeProducerV2 struct {
	client PersonalizeClientV2
}

type PersonalizeClientV2 interface {
	PutEvents(ctx context.Context, input *personalizeevents.PutEventsInput, opts ...func(*personalizeevents.Options)) (*personalizeevents.PutEventsOutput, error)
	PutUsers(ctx context.Context, input *personalizeevents.PutUsersInput, opts ...func(*personalizeevents.Options)) (*personalizeevents.PutUsersOutput, error)
	PutItems(ctx context.Context, input *personalizeevents.PutItemsInput, opts ...func(*personalizeevents.Options)) (*personalizeevents.PutItemsOutput, error)
}

func NewProducerV2(destination *backendconfig.DestinationT, o common.Opts) (common.Producer, error) {
	sessionConfig, err := awsutils.NewSessionConfigForDestinationV2(destination, o.Timeout, "personalize")
	if err != nil {
		return nil, err
	}
	sessionConfig.MaxIdleConnsPerHost = config.GetIntVar(64, 1, "Router.PERSONALIZE.httpMaxIdleConnsPerHost", "Router.PERSONALIZE.noOfWorkers", "Router.noOfWorkers")
	awsConfig, err := awsutil.CreateAWSConfig(context.Background(), sessionConfig)
	if err != nil {
		return nil, err
	}
	return &PersonalizeProducerV2{client: personalizeevents.NewFromConfig(awsConfig)}, nil
}

func (producer *PersonalizeProducerV2) Produce(jsonData json.RawMessage, _ interface{}) (statusCode int, respStatus, responseMessag string) {
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
		input := &PersonalizeEvent{}
		err = jsonrs.Unmarshal(eventPayload, input)
		if err != nil {
			return 400, err.Error(), "Could not unmarshal jsonData according to PutEvents input structure"
		}
		response, err = client.PutEvents(context.Background(), input.ToPutEventsInput())
	case "PutUsers":
		input := &Users{}
		err = jsonrs.Unmarshal(eventPayload, input)
		if err != nil {
			return 400, err.Error(), "Could not unmarshal jsonData according to PutUsers input structure"
		}
		response, err = client.PutUsers(context.Background(), input.ToPutUsersInput())
	case "PutItems":
		input := &Items{}
		err = jsonrs.Unmarshal(eventPayload, input)
		if err != nil {
			return 400, err.Error(), "Could not unmarshal jsonData according to PutItems input structure"
		}
		response, err = client.PutItems(context.Background(), input.ToPutItemsInput())
	default:
		input := &PersonalizeEvent{}
		err = jsonrs.Unmarshal(jsonData, input)
		if err != nil {
			return 400, err.Error(), "Could not unmarshal jsonData according to PutEvents input structure"
		}
		response, err = client.PutEvents(context.Background(), input.ToPutEventsInput())
	}

	if err != nil {
		statusCode, respStatus, responseMessage := common.ParseAWSErrorV2(err)
		pkgLogger.Errorn("[Personalize] error",
			logger.NewIntField("statusCode", int64(statusCode)),
			logger.NewStringField("respStatus", respStatus),
			logger.NewStringField("responseMessage", responseMessage))
		return statusCode, respStatus, responseMessage
	}

	return 200, "Success", fmt.Sprintf("Message delivered with Record information %v", response)
}

func (*PersonalizeProducerV2) Close() error {
	// no-op
	return nil
}

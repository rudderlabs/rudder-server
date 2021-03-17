package personalize

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/personalizeevents"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

type Config struct {
	Region          string
	AccessKeyID     string
	SecretAccessKey string
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("personalize")
}
func NewProducer(destinationConfig interface{}) (personalizeevents.PersonalizeEvents, error) {
	var config Config
	jsonConfig, err := json.Marshal(destinationConfig) // produces json
	if err != nil {
		return personalizeevents.PersonalizeEvents{}, fmt.Errorf("[Personalize] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return personalizeevents.PersonalizeEvents{}, fmt.Errorf("[Personalize] Error while unmarshalling destination config :: %w", err)
	}
	var s *session.Session
	if config.AccessKeyID == "" || config.SecretAccessKey == "" {
		s = session.Must(session.NewSession(&aws.Config{
			Region: aws.String(config.Region),
		}))
	} else {
		s = session.Must(session.NewSession(&aws.Config{
			Region:      aws.String(config.Region),
			Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.SecretAccessKey, "")}))
	}
	var client *personalizeevents.PersonalizeEvents = personalizeevents.New(s)
	return *client, nil
}
func Produce(jsonData json.RawMessage, producer interface{}, destConfig interface{}) (statusCode int, respStatus string, responseMessag string) {

	client, ok := producer.(personalizeevents.PersonalizeEvents)
	if (!ok || client == personalizeevents.PersonalizeEvents{}) {
		// return 400 if producer is invalid
		return 400, "Could not create producer for Personalize", "Could not create producer for Personalize"
	}
	input := personalizeevents.PutEventsInput{}
	bytes := []byte(jsonData)
	err := json.Unmarshal(bytes, &input)
	if err != nil {
		return 400, err.Error(), "Could not unmarshal jsonData according to putEvents input structure"
	}
	res, err := client.PutEvents(&input)
	if err != nil {
		pkgLogger.Errorf("Personalize Error while sending event :: %w", err)
		// set default status code as 500
		statusCode := 500
		// fetching status code from response
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			statusCode = reqErr.StatusCode()
		}
		return statusCode, err.Error(), err.Error()
	} else {
		responseMessage := fmt.Sprintf("Message delivered with Record information %v", res)
		respStatus = "Success"
		return 200, respStatus, responseMessage
	}
}

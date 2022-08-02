package lambda

import (
	"encoding/json"
	"fmt"
	"github.com/tidwall/gjson"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/rudderlabs/rudder-server/utils/logger"
)

// Config is the config that is required to send data to Lambda
type Config struct {
	Region      string
	AccessKeyID string
	AccessKey   string
}

type Opts struct {
	Timeout time.Duration
}

var pkgLogger logger.LoggerI

func init() {
	pkgLogger = logger.NewLogger().Child("streammanager").Child("firehose")
}

// NewProducer creates a producer based on destination config
func NewProducer(destinationConfig interface{}, o Opts) (lambda.Lambda, error) {
	var config Config
	jsonConfig, err := json.Marshal(destinationConfig)
	if err != nil {
		return lambda.Lambda{}, fmt.Errorf("[Lambda] Error while marshalling destination config :: %w", err)
	}
	err = json.Unmarshal(jsonConfig, &config)
	if err != nil {
		return lambda.Lambda{}, fmt.Errorf("[Lambda] error  :: error in Lambda while unmarshelling destination config:: %w", err)
	}
	var s *session.Session
	httpClient := &http.Client{
		Timeout: o.Timeout,
	}

	if config.AccessKeyID == "" || config.AccessKey == "" {
		s = session.Must(session.NewSession(&aws.Config{
			Region:     aws.String(config.Region),
			HTTPClient: httpClient,
		}))
	} else {
		s = session.Must(session.NewSession(&aws.Config{
			HTTPClient:  httpClient,
			Region:      aws.String(config.Region),
			Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.AccessKey, ""),
		}))
	}
	var lambdaClient *lambda.Lambda = lambda.New(s)
	return *lambdaClient, nil
}

// Produce creates a producer and send data to Lambda.
func Produce(jsonData json.RawMessage, producer, destConfig interface{}) (statusCode int, respStatus, responseMessage string) {
	client, ok := producer.(lambda.Lambda)
	if !ok {
		respStatus = "Failure"
		responseMessage = "[Lambda] error :: Could not create producer"
		pkgLogger.Errorf("[Lambda] error :: Could not create producer")
		return 400, respStatus, responseMessage
	}
	parsedJSON := gjson.ParseBytes(jsonData)
	data := parsedJSON.Get("message").String()
	//clientContext := parsedJSON.Get("clientContext").String()
	invocationType := parsedJSON.Get("invocationType").String()
	lambdaFunction := parsedJSON.Get("lambda").String()

	var input lambda.InvokeInput
	input.FunctionName = aws.String(lambdaFunction)
	input.Payload = []byte(data)
	input.InvocationType = aws.String(invocationType)

	res, err := client.Invoke(&input)
	if err != nil {
		statusCode = int(*res.StatusCode)
		respStatus = "Failure"
		responseMessage = "[Lambda] error while Invoking the function ::" + err.Error()
	}
	statusCode = int(*res.StatusCode)
	respStatus = "Success"
	responseMessage = "Message delivered"
	return statusCode, respStatus, responseMessage
}

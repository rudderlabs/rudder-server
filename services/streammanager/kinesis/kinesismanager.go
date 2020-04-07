package kinesis

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/tidwall/gjson"
)

// Produce creates a producer and send data to Kinesis
func Produce(jsonData json.RawMessage) (int, string, string) {

	//config := jsonData.
	parsedJSON := gjson.ParseBytes(jsonData)
	if parsedJSON.Get("output").Exists() {
		parsedJSON = parsedJSON.Get("output")
	}
	config := parsedJSON.Get("config")
	region, ok := config.Get("region").Value().(string)
	if !ok {
		panic(fmt.Errorf("typecast of config.Get(\"region\") to string failed"))
	}
	stream, ok := config.Get("stream").Value().(string)
	if !ok {
		panic(fmt.Errorf("typecast of config.Get(\"stream\") to string failed"))
	}
	accessKeyID, ok := config.Get("accessKeyID").Value().(string)
	if !ok {
		panic(fmt.Errorf("typecast of config.Get(\"accessKeyID\") to string failed"))
	}
	accessKey, ok := config.Get("accessKey").Value().(string)
	if !ok {
		panic(fmt.Errorf("typecast of config.Get(\"accessKey\") to string failed"))
	}
	useMessageID, ok := config.Get("useMessageId").Value().(bool)
	if !ok {
		panic(fmt.Errorf("typecast of config.Get(\"useMessageId\") to bool failed"))
	}

	s := session.New(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(accessKeyID, accessKey, "")})
	kc := kinesis.New(s)

	streamName := aws.String(stream)

	data := parsedJSON.Get("message").Value().(interface{})
	value, err := json.Marshal(data)
	userID := parsedJSON.Get("userId").Value().(string)

	partitionKey := aws.String(userID)

	if useMessageID {
		messageID := parsedJSON.Get("message").Get("messageId").Value().(string)
		partitionKey = aws.String(messageID)
	}

	putOutput, err := kc.PutRecord(&kinesis.PutRecordInput{
		Data:         []byte(value),
		StreamName:   streamName,
		PartitionKey: partitionKey,
	})
	if err != nil {
		return 400, err.Error(), ""
	}
	message := fmt.Sprintf("Message delivered at SequenceNumber: %v , shard Id: %v", putOutput.SequenceNumber, putOutput.ShardId)
	return 200, "Success", message
}

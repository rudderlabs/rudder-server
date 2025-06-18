package wunderkind

import (
	"errors"

	"github.com/rudderlabs/rudder-go-kit/config"
)

const (
	InvocationType       = "RequestResponse"
	WunderkindRegion     = "WUNDERKIND_REGION"
	WunderkindIamRoleArn = "WUNDERKIND_IAM_ROLE_ARN"
	WunderkindExternalId = "WUNDERKIND_EXTERNAL_ID"
	WunderkindLambda     = "WUNDERKIND_LAMBDA"
)

type inputData struct {
	Payload string `json:"payload"`
}

func validate(conf *config.Config) error {
	if conf.GetString(WunderkindRegion, "") == "" {
		return errors.New("region cannot be empty")
	}

	if conf.GetString(WunderkindIamRoleArn, "") == "" {
		return errors.New("iam role arn cannot be empty")
	}

	if conf.GetString(WunderkindExternalId, "") == "" {
		return errors.New("external id cannot be empty")
	}

	if conf.GetString(WunderkindLambda, "") == "" {
		return errors.New("lambda function cannot be empty")
	}

	return nil
}

package maxcompute

import (
	"errors"
	"fmt"
	"strings"
)

// RoleMode selects MaxCompute authentication from destination account fields.
func RoleMode(conf map[string]any) (bool, error) {
	if conf == nil {
		return false, fmt.Errorf("MaxCompute OSS configuration is required")
	}
	for _, field := range []string{"accessKeyId", "accessKeySecret", "iamRoleARN"} {
		if value, present := conf[field]; present && value != nil {
			if _, ok := value.(string); !ok {
				return false, fmt.Errorf("%s must be a string for MaxCompute", field)
			}
		}
	}
	keyID, _ := conf["accessKeyId"].(string)
	secret, _ := conf["accessKeySecret"].(string)
	arn, _ := conf["iamRoleARN"].(string)
	role := strings.TrimSpace(arn) != ""
	hasID, hasSecret := strings.TrimSpace(keyID) != "", strings.TrimSpace(secret) != ""
	if role && (hasID || hasSecret) {
		return false, fmt.Errorf("iamRoleARN and access keys cannot be combined for MaxCompute")
	}
	if hasID != hasSecret {
		return false, fmt.Errorf("accessKeyId and accessKeySecret must be a complete pair for MaxCompute")
	}
	if !role && !hasID {
		return false, fmt.Errorf("iamRoleARN or accessKeyId and accessKeySecret are required for MaxCompute")
	}
	return role, nil
}

// ValidateOSSConfig checks fields required by both OSS staging and ODPS LOAD.
func ValidateOSSConfig(conf map[string]any) error {
	if _, err := RoleMode(conf); err != nil {
		return err
	}
	for _, field := range []string{"ossRoleArn", "bucketName", "region"} {
		value, ok := conf[field].(string)
		if !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required for MaxCompute OSS", field)
		}
	}
	return nil
}

func ValidateSettings(settings map[string]any, workspaceID string) error {
	if err := ValidateOSSConfig(settings); err != nil {
		return err
	}
	roleARN, _ := settings["iamRoleARN"].(string)
	if strings.TrimSpace(roleARN) != "" && strings.TrimSpace(workspaceID) == "" {
		return errors.New("MaxCompute role authentication requires workspace ID")
	}
	return nil
}

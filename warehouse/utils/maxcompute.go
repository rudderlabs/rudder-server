package warehouseutils

import (
	"fmt"
	"strings"
)

// MaxComputeRoleMode selects authentication from the inlined account fields.
// It rejects mixed and incomplete configurations before credentials are used.
func MaxComputeRoleMode(conf map[string]any) (bool, error) {
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
	key, _ := conf["accessKeySecret"].(string)
	arn, _ := conf["iamRoleARN"].(string)
	role := strings.TrimSpace(arn) != ""
	hasID, hasSecret := strings.TrimSpace(keyID) != "", strings.TrimSpace(key) != ""
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

// ValidateMaxComputeOSSConfig checks storage fields and authentication mode.
func ValidateMaxComputeOSSConfig(conf map[string]any) error {
	if _, err := MaxComputeRoleMode(conf); err != nil {
		return err
	}
	readRole, ok := conf["ossRoleArn"].(string)
	if !ok || strings.TrimSpace(readRole) == "" {
		return fmt.Errorf("ossRoleArn is required for MaxCompute")
	}
	for _, field := range []string{"bucketName", "region"} {
		value, ok := conf[field].(string)
		if !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required for MaxCompute OSS", field)
		}
	}
	return nil
}

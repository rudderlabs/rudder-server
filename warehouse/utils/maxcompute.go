package warehouseutils

import (
	"fmt"
	"strings"
)

// ValidateMaxComputeOSSConfig checks OSS and ODPS authentication requirements.
// Role assumption uses server-configured caller credentials, never destination
// access keys. This catches mixed and incomplete settings before network access.
func ValidateMaxComputeOSSConfig(conf map[string]any) error {
	if conf == nil {
		return fmt.Errorf("MaxCompute OSS configuration is required")
	}
	for _, field := range []string{"bucketName", "region"} {
		value, ok := conf[field].(string)
		if !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required for MaxCompute OSS", field)
		}
	}
	role, roleOK := conf["roleBasedAuth"].(bool)
	if conf["roleBasedAuth"] != nil && !roleOK {
		return fmt.Errorf("roleBasedAuth must be a boolean for MaxCompute OSS")
	}
	for _, field := range []string{"accessKeyID", "accessKey", "iamRoleARN"} {
		if value, present := conf[field]; present && value != nil {
			if _, ok := value.(string); !ok {
				return fmt.Errorf("%s must be a string for MaxCompute OSS", field)
			}
		}
	}
	keyID, _ := conf["accessKeyID"].(string)
	key, _ := conf["accessKey"].(string)
	arn, _ := conf["iamRoleARN"].(string)
	if role {
		if strings.TrimSpace(arn) == "" {
			return fmt.Errorf("iamRoleARN is required for MaxCompute OSS role-based authentication")
		}
		if strings.TrimSpace(keyID) != "" || strings.TrimSpace(key) != "" {
			return fmt.Errorf("accessKeyID and accessKey cannot be combined with MaxCompute role-based authentication")
		}
	} else {
		if strings.TrimSpace(arn) != "" {
			return fmt.Errorf("roleBasedAuth is required when iamRoleARN is set for MaxCompute OSS")
		}
		if strings.TrimSpace(keyID) == "" || strings.TrimSpace(key) == "" {
			return fmt.Errorf("accessKeyID and accessKey are required for MaxCompute OSS static authentication")
		}
	}
	return nil
}

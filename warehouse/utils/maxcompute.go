package warehouseutils

import (
	"fmt"
	"strings"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

// MaxComputeRoleMode selects authentication from the inlined account fields.
// It rejects mixed and incomplete configurations before credentials are used.
func MaxComputeRoleMode(conf map[string]any) (bool, error) {
	return misc.AlibabaOSSRoleMode(conf)
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

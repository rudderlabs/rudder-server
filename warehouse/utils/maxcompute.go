package warehouseutils

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type maxComputeValidationFileSizeKey struct{}

// WithMaxComputeValidationFileSize carries the byte count of the temporary
// fixture written by validation without adding a non-row field to its payload.
func WithMaxComputeValidationFileSize(ctx context.Context, size int64) context.Context {
	return context.WithValue(ctx, maxComputeValidationFileSizeKey{}, size)
}

func MaxComputeValidationFileSize(ctx context.Context) (int64, bool) {
	size, ok := ctx.Value(maxComputeValidationFileSizeKey{}).(int64)
	return size, ok && size > 0
}

// MaxComputeRoleMode selects MaxCompute authentication from destination account fields.
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

// ValidateMaxComputeOSSConfig checks fields required by both OSS staging and ODPS LOAD.
func ValidateMaxComputeOSSConfig(conf map[string]any) error {
	if _, err := MaxComputeRoleMode(conf); err != nil {
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

func ValidateMaxComputeSettings(settings map[string]any, workspaceID string) error {
	if err := ValidateMaxComputeOSSConfig(settings); err != nil {
		return err
	}
	roleARN, _ := settings["iamRoleARN"].(string)
	if strings.TrimSpace(roleARN) != "" && strings.TrimSpace(workspaceID) == "" {
		return errors.New("MaxCompute role authentication requires workspace ID")
	}
	return nil
}

// MaxComputeErrorReason classifies only known, actionable diagnostics. The
// returned text is fixed so raw OSS errors never enter validation logs.
func MaxComputeErrorReason(message string, role bool) (reason, guidance string) {
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "caller credentials unavailable"):
		return "caller_credentials_missing", "Configure the server caller access keys for MaxCompute role authentication."
	case strings.Contains(lower, "odps-0420095"):
		return "project_membership", "Add this identity to the MaxCompute project and grant the required permissions."
	case strings.Contains(lower, "odps-0030001") && strings.Contains(lower, "is not a valid account"):
		return "project_membership", "Add the RAM user by numeric UID, not by user name."
	case strings.Contains(lower, "odps-0110061") && strings.Contains(lower, "support schema"):
		return "schema_not_enabled", "Enable schemas on the MaxCompute project, then retry after a few minutes."
	case strings.Contains(lower, "odps-0130071") && (strings.Contains(lower, "too long") || strings.Contains(lower, "max length is 128")):
		return "identifier_length", "Shorten the schema, table or column identifier to at most 128 characters."
	case strings.Contains(lower, "odps-0130071") && (strings.Contains(lower, "full scan") || strings.Contains(lower, "fullscan") || strings.Contains(lower, "all partitions")):
		return "fullscan_guard", "Add a bounded pt partition filter to the query."
	case strings.Contains(lower, "invalidaccesskeyid") || strings.Contains(lower, "signaturedoesnotmatch") ||
		(!role && strings.Contains(lower, "odps authentication or permission denied")):
		return "access_key_authentication", "Check the AccessKey ID and secret and allow time for a new key to become active."
	case role && (strings.Contains(lower, "nopermission") || strings.Contains(lower, "assumerole") || strings.Contains(lower, "externalid")):
		return "role_assumption", "Check the role trust policy and workspace-bound ExternalId."
	}
	return "", ""
}

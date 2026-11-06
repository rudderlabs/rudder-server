package maxcompute

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/client"
	"github.com/rudderlabs/rudder-server/warehouse/encoding"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

// roleMode selects MaxCompute authentication from destination account fields.
func roleMode(conf map[string]any) (bool, error) {
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

func validateSettings(settings map[string]any, workspaceID string) error {
	if _, err := roleMode(settings); err != nil {
		return err
	}
	for _, field := range []string{"ossRoleArn", "bucketName", "region"} {
		value, ok := settings[field].(string)
		if !ok || strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required for MaxCompute OSS", field)
		}
	}
	roleARN, _ := settings["iamRoleARN"].(string)
	if strings.TrimSpace(roleARN) != "" && strings.TrimSpace(workspaceID) == "" {
		return errors.New("MaxCompute role authentication requires workspace ID")
	}
	return nil
}

// errorReason classifies only known, actionable diagnostics. The
// returned text is fixed so raw OSS errors never enter validation logs.
func errorReason(message string, role bool) (reason, guidance string) {
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
	case strings.Contains(lower, "oss") && strings.Contains(lower, "accessdenied"):
		return "oss_access_denied", "The OSS policy denies this bucket or prefix; check the bucket name, the OSS prefix and the policy's resource scope."
	}
	return "", ""
}

// ValidationError returns a fixed customer action for known MaxCompute errors.
func ValidationError(step string, conf map[string]any, err error) error {
	role, _ := roleMode(conf)
	reason, guidance := errorReason(err.Error(), role)
	if reason != "" {
		if strings.Contains(err.Error(), "ODPS-") {
			return fmt.Errorf("%s: %s (%s)", reason, guidance, client.ODPSSafeError(err))
		}
		return fmt.Errorf("%s: %s", reason, guidance)
	}
	if step == model.VerifyingObjectStorage {
		return errors.New("MaxCompute OSS validation failed")
	}
	return err
}

// NormalizeDateTime converts source timestamps and already normalized load-file
// values to UTC. Retrying a load does not alter a valid datetime.
func NormalizeDateTime(value any) (string, bool) {
	text, ok := value.(string)
	if !ok {
		return "", false
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700",
		encoding.MaxComputeTimeFormat,
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return parsed.UTC().Format(encoding.MaxComputeTimeFormat), true
		}
	}
	return "", false
}

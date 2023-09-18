package filemanager

import (
	"os"

	"github.com/rudderlabs/rudder-go-kit/config"
)

type ProviderConfigOpts struct {
	Provider           string
	Bucket             string
	Prefix             string
	Config             *config.Config
	ExternalIDSupplier func() string
}

func GetProviderConfigFromEnv(opts ProviderConfigOpts) map[string]interface{} {
	if opts.Config == nil {
		opts.Config = config.Default
	}
	if opts.ExternalIDSupplier == nil {
		opts.ExternalIDSupplier = func() string { return "" }
	}
	config := opts.Config
	providerConfig := make(map[string]interface{})
	switch opts.Provider {
	case "S3":
		providerConfig["bucketName"] = opts.Bucket
		providerConfig["prefix"] = opts.Prefix
		providerConfig["accessKeyID"] = config.GetString("AWS_ACCESS_KEY_ID", "")
		providerConfig["accessKey"] = config.GetString("AWS_SECRET_ACCESS_KEY", "")
		providerConfig["enableSSE"] = config.GetBool("AWS_ENABLE_SSE", false)
		providerConfig["regionHint"] = config.GetString("AWS_S3_REGION_HINT", "us-east-1")
		providerConfig["iamRoleArn"] = config.GetString("BACKUP_IAM_ROLE_ARN", "")
		if providerConfig["iamRoleArn"] != "" {
			providerConfig["externalID"] = opts.ExternalIDSupplier()
		}
	case "GCS":
		providerConfig["bucketName"] = opts.Bucket
		providerConfig["prefix"] = opts.Prefix
		credentials, err := os.ReadFile(config.GetString("GOOGLE_APPLICATION_CREDENTIALS", ""))
		if err == nil {
			providerConfig["credentials"] = string(credentials)
		}
	case "AZURE_BLOB":
		providerConfig["containerName"] = opts.Bucket
		providerConfig["prefix"] = opts.Prefix
		providerConfig["accountName"] = config.GetString("AZURE_STORAGE_ACCOUNT", "")
		providerConfig["accountKey"] = config.GetString("AZURE_STORAGE_ACCESS_KEY", "")
	case "MINIO":
		providerConfig["bucketName"] = opts.Bucket
		providerConfig["prefix"] = opts.Prefix
		providerConfig["endPoint"] = config.GetString("MINIO_ENDPOINT", "localhost:9000")
		providerConfig["accessKeyID"] = config.GetString("MINIO_ACCESS_KEY_ID", "minioadmin")
		providerConfig["secretAccessKey"] = config.GetString("MINIO_SECRET_ACCESS_KEY", "minioadmin")
		providerConfig["useSSL"] = config.GetBool("MINIO_SSL", false)
	case "DIGITAL_OCEAN_SPACES":
		providerConfig["bucketName"] = opts.Bucket
		providerConfig["prefix"] = opts.Prefix
		providerConfig["endPoint"] = config.GetString("DO_SPACES_ENDPOINT", "")
		providerConfig["accessKeyID"] = config.GetString("DO_SPACES_ACCESS_KEY_ID", "")
		providerConfig["accessKey"] = config.GetString("DO_SPACES_SECRET_ACCESS_KEY", "")
	}

	return providerConfig
}

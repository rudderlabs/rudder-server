package config

// RudderServerPathKey :
const RudderServerPathKey = "RUDDER_SERVER_PATH"

// RudderEnvFilePathKey :
const RudderEnvFilePathKey = "RUDDER_ENV_FILE_PATH"

const (
	JobsDBHostKey     = "JOBS_DB_HOST"
	JobsDBPortKey     = "JOBS_DB_PORT"
	JobsDBNameKey     = "JOBS_DB_DB_NAME"
	JobsDBUserKey     = "JOBS_DB_USER"
	JobsDBPasswordKey = "JOBS_DB_PASSWORD"
)

const (
	JobsS3BucketKey = "JOBS_BACKUP_BUCKET"
	AWSAccessKey    = "AWS_ACCESS_KEY_ID"
	AWSSecretKey    = "AWS_SECRET_ACCESS_KEY"
)

const DefaultEnvFile = ".env"

const DestTransformURLKey = "DEST_TRANSFORM_URL"

const (
	ConfigBackendURLKey         = "CONFIG_BACKEND_URL"
	ConfigBackendTokenKey       = "CONFIG_BACKEND_TOKEN"
	ConfigBackendWorkSpaceToken = "WORKSPACE_TOKEN"
)

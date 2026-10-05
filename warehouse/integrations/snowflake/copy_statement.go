package snowflake

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/misc"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// awsCredentialsClause is the CREDENTIALS clause a COPY statement carries when
// it reads from S3 with temporary credentials instead of a storage integration.
func awsCredentialsClause(accessKeyID, secretAccessKey, sessionToken string) string {
	return fmt.Sprintf(`CREDENTIALS = (AWS_KEY_ID='%s' AWS_SECRET_KEY='%s' AWS_TOKEN='%s')`, accessKeyID, secretAccessKey, sessionToken)
}

// copyCredentialsRegex masks the credentials awsCredentialsClause puts into a
// COPY statement. The middleware applies it to every query it logs, since it
// only ever sees the statement with the credentials already in place.
var copyCredentialsRegex = map[string]string{
	"AWS_KEY_ID='[^']*'":     "AWS_KEY_ID='***'",
	"AWS_SECRET_KEY='[^']*'": "AWS_SECRET_KEY='***'",
	"AWS_TOKEN='[^']*'":      "AWS_TOKEN='***'",
}

// maskedAWSCredentialsClause is what a logged COPY statement shows in place of
// awsCredentialsClause. It matches what copyCredentialsRegex turns it into.
var maskedAWSCredentialsClause = awsCredentialsClause("***", "***", "***")

// copyStatement is a COPY statement formatted twice: once with its real auth
// clause, which only ever goes to the database, and once with the credentials
// masked, for logging. Log it through String; run it through execContext or
// queryContext.
type copyStatement struct {
	loggableSQL string // formatted with the credentials masked
	secretSQL   string // formatted with the real auth clause; never log
}

// newCopyStatement formats the statement build returns once for each auth clause.
func newCopyStatement(build func(authClause string) string, secretAuthClause, loggableAuthClause string) copyStatement {
	return copyStatement{
		loggableSQL: build(loggableAuthClause),
		secretSQL:   build(secretAuthClause),
	}
}

func (c copyStatement) String() string {
	return c.loggableSQL
}

// GoString keeps %#v from printing secretSQL.
func (c copyStatement) GoString() string {
	return c.loggableSQL
}

func (c copyStatement) execContext(ctx context.Context, db *sqlmw.DB) (sql.Result, error) {
	return db.ExecContext(ctx, c.secretSQL)
}

func (c copyStatement) queryContext(ctx context.Context, db *sqlmw.DB) (*sqlmw.Rows, error) {
	return db.QueryContext(ctx, c.secretSQL)
}

// newCopyStatement builds a copyStatement with this destination's auth clause.
// It is the only place the auth clause is built, so the credentials never exist
// outside a copyStatement.
func (sf *Snowflake) newCopyStatement(build func(authClause string) string) (copyStatement, error) {
	if misc.IsConfiguredToUseRudderObjectStorage(sf.Warehouse.Destination.Config) || (sf.CloudProvider == "AWS" && sf.Warehouse.GetStringDestinationConfig(sf.conf, model.StorageIntegrationSetting) == "") {
		tempAccessKeyId, tempSecretAccessKey, token, err := whutils.GetTemporaryS3Cred(&sf.Warehouse.Destination)
		if err != nil {
			return copyStatement{}, fmt.Errorf("getting temporary s3 credentials: %w", err)
		}
		return newCopyStatement(build, awsCredentialsClause(tempAccessKeyId, tempSecretAccessKey, token), maskedAWSCredentialsClause), nil
	}
	// The storage integration name comes from the destination configuration, not from event data.
	// It is intentionally left unquoted: quoting would make it case-sensitive and break existing
	// configurations that rely on Snowflake resolving unquoted identifiers to uppercase.
	// It is not a secret, so the logged statement shows it as it is.
	storageIntegration := fmt.Sprintf(`STORAGE_INTEGRATION = %s`, sf.Warehouse.GetStringDestinationConfig(sf.conf, model.StorageIntegrationSetting))
	return newCopyStatement(build, storageIntegration, storageIntegration), nil
}

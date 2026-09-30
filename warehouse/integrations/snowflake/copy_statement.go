package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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

// secretAuthClausePlaceholder stands in for the auth clause in a copyStatement's
// SQL until the statement runs.
const secretAuthClausePlaceholder = "{{secret_auth_clause}}"

// copyStatement is a COPY statement whose auth clause is kept out of its SQL
// until it runs, so that logging the statement cannot print the credentials.
// Log it through String; run it through execContext or queryContext.
type copyStatement struct {
	sqlTemplate      string // COPY SQL with secretAuthClausePlaceholder; never holds credentials
	secretAuthClause string // CREDENTIALS = (...) or STORAGE_INTEGRATION = ...; never log
}

// newCopyStatement pairs sqlTemplate with its auth clause. The placeholder has
// to appear exactly once: a namespace or location carrying a copy of it would
// otherwise decide where the credentials land.
func newCopyStatement(sqlTemplate, secretAuthClause string) (copyStatement, error) {
	if n := strings.Count(sqlTemplate, secretAuthClausePlaceholder); n != 1 {
		return copyStatement{}, fmt.Errorf("copy statement has %d auth clause placeholders, want 1", n)
	}
	return copyStatement{sqlTemplate: sqlTemplate, secretAuthClause: secretAuthClause}, nil
}

// String returns the statement with the auth clause left as a placeholder.
func (c copyStatement) String() string {
	return c.sqlTemplate
}

// GoString keeps %#v from printing secretAuthClause.
func (c copyStatement) GoString() string {
	return c.String()
}

// unsafeSQL returns the runnable statement, credentials included. Pass it
// only to the database.
func (c copyStatement) unsafeSQL() string {
	return strings.Replace(c.sqlTemplate, secretAuthClausePlaceholder, c.secretAuthClause, 1)
}

func (c copyStatement) execContext(ctx context.Context, db *sqlmw.DB) (sql.Result, error) {
	return db.ExecContext(ctx, c.unsafeSQL())
}

func (c copyStatement) queryContext(ctx context.Context, db *sqlmw.DB) (*sqlmw.Rows, error) {
	return db.QueryContext(ctx, c.unsafeSQL())
}

// newCopyStatement builds a copyStatement for sqlTemplate with this
// destination's auth clause. It is the only place the auth clause is built, so
// the credentials never exist outside a copyStatement.
func (sf *Snowflake) newCopyStatement(sqlTemplate string) (copyStatement, error) {
	if misc.IsConfiguredToUseRudderObjectStorage(sf.Warehouse.Destination.Config) || (sf.CloudProvider == "AWS" && sf.Warehouse.GetStringDestinationConfig(sf.conf, model.StorageIntegrationSetting) == "") {
		tempAccessKeyId, tempSecretAccessKey, token, err := whutils.GetTemporaryS3Cred(&sf.Warehouse.Destination)
		if err != nil {
			return copyStatement{}, fmt.Errorf("getting temporary s3 credentials: %w", err)
		}
		return newCopyStatement(sqlTemplate, awsCredentialsClause(tempAccessKeyId, tempSecretAccessKey, token))
	}
	// The storage integration name comes from the destination configuration, not from event data.
	// It is intentionally left unquoted: quoting would make it case-sensitive and break existing
	// configurations that rely on Snowflake resolving unquoted identifiers to uppercase.
	return newCopyStatement(sqlTemplate, fmt.Sprintf(`STORAGE_INTEGRATION = %s`, sf.Warehouse.GetStringDestinationConfig(sf.conf, model.StorageIntegrationSetting)))
}

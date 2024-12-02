package router_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/router"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func TestErrorHandler_MatchUploadJobErrorType(t *testing.T) {
	t.Run("known errors", func(t *testing.T) {
		testCases := []struct {
			name                 string
			destType             string
			jobError             error
			expectedJobErrorType model.JobErrorType
		}{
			{
				"BigQuery access Denied", warehouseutils.BQ, errors.New("{\"fetching_remote_schema_failed\":{\"attempt\":2,\"errors\":[\"googleapi: Error 403: Access Denied: Table ***: User does not have permission to query table ***, or perhaps it does not exist in location ***., accessDenied\"]}}"), model.PermissionError,
			},
			{
				"BigQuery dataset not found", warehouseutils.BQ, errors.New("{\"exporting_data_failed\":{\"attempt\":1,\"errors\":[\"googleapi: Error 404: Not found: Dataset ***, notFound\"]}}"), model.ResourceNotFoundError,
			},
			{
				"BigQuery job exceeded rate limits", warehouseutils.BQ, errors.New("{\"exporting_data_failed\":{\"attempt\":1,\"errors\":[\"append: googleapi: Error 400: Job exceeded rate limits: Your project_and_region exceeded quota for concurrent queries. For more information, see https://cloud.google.com/bigquery/docs/troubleshoot-quotas, jobRateLimitExceeded\"]}}"), model.ConcurrentQueriesError,
			},
			{
				"BigQuery exceeded rate limits", warehouseutils.BQ, errors.New("{\"fetching_remote_schema_failed\":{\"errors\":[\"googleapi: Error 400: Exceeded rate limits: too many concurrent queries for this project_and_region. For more information, see https://cloud.google.com/bigquery/docs/troubleshoot-quotas, jobRateLimitExceeded\"],\"attempt\":5}}"), model.ConcurrentQueriesError,
			},
			{
				"BigQuery too many total leaf fields", warehouseutils.BQ, errors.New("{\"exporting_data_failed\":{\"attempt\":5,\"errors\":[\"1 errors occurred: update schema: adding columns to warehouse: failed to add columns for table device_data_event in namespace event of destination BQ: *** with error: googleapi: Error 400: Too many total leaf fields: ***, max allowed field count: 10000 , invalid\"]}}"), model.ColumnCountError,
			},

			{
				"ClickHouse authentication failed", warehouseutils.CLICKHOUSE, errors.New("{\"fetching_remote_schema_failed\":{\"attempt\":1,\"errors\":[\"code: 516, message: ***: Authentication failed: password is incorrect, or there is no user with such name.\"]}}"), model.PermissionError,
			},
			{
				"ClickHouse exceeded memory limits", warehouseutils.CLICKHOUSE, errors.New("{\"exporting_data_failed\":{\"attempt\":1,\"errors\":[\"Error occurred while committing with error: Error while committing transaction as there was error while loading in table with error:code: 241, message: Memory limit (total) exceeded: would use 27.01 GiB (attempt to allocate chunk of 4606752 bytes), maximum: 27.00 GiB: (avg_value_size_hint = 45, avg_chars_size = 44.4, limit = 8192)\"]}}"), model.InsufficientResourceError,
			},

			{
				"S3Datalake insufficient Lake Formation permission", warehouseutils.S3Datalake, errors.New("{\"creating_remote_schema_failed\":{\"attempt\":6,\"errors\":[\"AccessDeniedException: Insufficient Lake Formation permission(s): Required Create Database on Catalog\"]}}"), model.PermissionError,
			},
			{
				"S3Datalake user is not authorized", warehouseutils.S3Datalake, errors.New("{\"fetching_remote_schema_failed\":{\"errors\":[\"AccessDeniedException: User: *** is not authorized to perform: *** on resource: *** because no identity-based policy allows the *** action\"],\"attempt\":5}}"), model.PermissionError,
			},

			{
				"Deltalake user does not have permission to read files", warehouseutils.DELTALAKE, errors.New("{\"exporting_data_failed\":{\"attempt\":5,\"errors\":[\"error while executing with response: [42000] [Simba][Hardy] (80) Syntax or semantic analysis error thrown in server while executing query. Error message from server: org.apache.hive.service.cli.HiveSQLException: Error running query: com.databricks.sql.managedcatalog.acl.UnauthorizedAccessException: PERMISSION_DENIED: User does not have READ FILES on External Location ***.\tat org.apache.spark.sql.hive.t (80) (SQLExecDirectW)\"]}}"), model.PermissionError,
			},
			{
				"Deltalake user does not have permission to create on catalog", warehouseutils.DELTALAKE, errors.New("{\"creating_remote_schema_failed\":{\"attempt\":2,\"errors\":[\"error while executing with response: [42000] [Simba][Hardy] (80) Syntax or semantic analysis error thrown in server while executing query. Error message from server: org.apache.hive.service.cli.HiveSQLException: Error running query: java.lang.SecurityException: User does not have permission CREATE on CATALOG.\tat org.apache.spark.sql.hive.thriftserver.HiveThriftServerErrors$.runningQueryError(HiveThriftServerErrors.sc (80) (SQLExecDirectW)\"]}}"), model.PermissionError,
			},
			{
				"Deltalake endpoint not found", warehouseutils.DELTALAKE, errors.New("{\"fetching_remote_schema_failed\":{\"attempt\":5,\"errors\":[\"fetching schema from warehouse: fetching schema: creating schema: checking if schema exists: schema exists: databricks: request error: error connecting: host=*** port=***, httpPath=***: databricks: request error: open session request error: Post ***: ENDPOINT_NOT_FOUND: SQL warehouse *** does not exist at all in the database: request error after 1 attempt(s): unexpected HTTP status 404 Not Found\"]}}"), model.ResourceNotFoundError,
			},
			{
				"Deltalake resource does not exist", warehouseutils.DELTALAKE, errors.New("{\"fetching_remote_schema_failed\":{\"attempt\":2,\"errors\":[\"fetching schema from warehouse: fetching schema: creating schema: checking if schema exists: schema exists: databricks: request error: error connecting: host=*** port=443, httpPath=***: databricks: request error: open session request error: Post ***: RESOURCE_DOES_NOT_EXIST: No cluster found matching: ***: request error after 1 attempt(s): unexpected HTTP status 404 Not Found\"]}}"), model.ResourceNotFoundError,
			},

			{
				"MSSQL unable to open tcp connection", warehouseutils.MSSQL, errors.New("{\"fetching_remote_schema_failed\":{\"attempt\":2,\"errors\":[\"unable to open tcp connection with host ***: dial tcp ***: i/o timeout\"]}}"), model.PermissionError,
			},

			{
				"Postgres no such host", warehouseutils.POSTGRES, errors.New("{\"fetching_remote_schema_failed\":{\"attempt\":2,\"errors\":[\"dial tcp: lookup *** on ***: no such host\"]}}"), model.ResourceNotFoundError,
			},
			{
				"Postgres connection refused", warehouseutils.POSTGRES, errors.New("{\"fetching_remote_schema_failed\":{\"attempt\":4,\"errors\":[\"dial tcp ***: connect: connection refused\"]}}"), model.PermissionError,
			},
			{
				"Postgres database does not exist", warehouseutils.POSTGRES, errors.New("{\"fetching_remote_schema_failed\":{\"attempt\":5,\"errors\":[\"pq: database *** does not exist\"]}}"), model.ResourceNotFoundError,
			},
			{
				"Postgres database system is starting up", warehouseutils.POSTGRES, errors.New("{\"exporting_data_failed\":{\"attempt\":1,\"errors\":[\"pq: the database system is starting up\"]}}"), model.ResourceNotFoundError,
			},
			{
				"Postgres database system is shutting down", warehouseutils.POSTGRES, errors.New("{\"exporting_data_failed\":{\"attempt\":1,\"errors\":[\"pq: the database system is shutting down\"]}}"), model.ResourceNotFoundError,
			},
			{
				"Postgres relation does not exist", warehouseutils.POSTGRES, errors.New("{\"exporting_data_failed\":{\"attempt\":1,\"errors\":[\"pq: relation *** does not exist\"]}}"), model.ResourceNotFoundError,
			},
			{
				"Postgres transaction read-write mode during recovery", warehouseutils.POSTGRES, errors.New("{\"exporting_data_failed\":{\"attempt\":1,\"errors\":[\"pq: cannot set transaction read-write mode during recovery\"]}}"), model.ResourceNotFoundError,
			},
			{
				"Postgres tables can have at most 1600 columns", warehouseutils.POSTGRES, errors.New("{\"exporting_data_failed\":{\"attempt\":1,\"errors\":[\"failed to add columns for table *** in namespace *** of destination POSTGRES:*** with error: pq: tables can have at most 1600 columns\"]}}"), model.ColumnCountError,
			},
			{
				"Postgres password authentication failed for user", warehouseutils.POSTGRES, errors.New("{\"fetching_remote_schema_failed\":{\"attempt\":10,\"errors\":[\"pq: password authentication failed for user ***\"]}}"), model.PermissionError,
			},
			{
				"Postgres permission denied", warehouseutils.POSTGRES, errors.New("{\"fetching_remote_schema_failed\":{\"attempt\":10,\"errors\":[\"pq: permission denied for user ***\"]}}"), model.PermissionError,
			},

			{
				"RedShift cannot alter type of a column used by a view or rule", warehouseutils.RS, errors.New("{\"exporting_data_failed\":{\"attempt\":5,\"errors\":[\"pq: cannot alter type of a column used by a view or rule\"]}}"), model.AlterColumnError,
			},
			{
				"RedShift disk Full", warehouseutils.RS, errors.New("{\"exporting_data_failed\":{\"attempt\":1,\"errors\":[\"pq: Disk Full\"]}}"), model.InsufficientResourceError,
			},
			{
				"RedShift eof", warehouseutils.RS, errors.New("{\"internal_processing_failed\":{\"attempt\":5,\"errors\":[\"redshift set query_group error : EOF\"]}}"), model.PermissionError,
			},
			{
				"RedShift 1023", warehouseutils.RS, errors.New("{\"exporting_data_failed\":{\"attempt\":1,\"errors\":[\"pq: 1023\"]}}"), model.ConcurrentQueriesError,
			},
			{
				"RedShift value too long for character type", warehouseutils.RS, errors.New("{\"exporting_data_failed\":{\"attempt\":2,\"errors\":[\"pq: Value too long for character type\"]}}"), model.ColumnSizeError,
			},
			{
				"RedShift permission denied for database", warehouseutils.RS, errors.New("{\"creating_remote_schema_failed\":{\"attempt\":5,\"errors\":[\"pq: permission denied for database ***\"]}}"), model.PermissionError,
			},
			{
				"RedShift must be owner of relation", warehouseutils.RS, errors.New("{\"exporting_data_failed\":{\"attempt\":5,\"errors\":[\"pq: must be owner of relation ***\"]}}"), model.PermissionError,
			},
			{
				"RedShift cannot execute write query because system is in resize mode", warehouseutils.RS, errors.New("{\"exporting_data_failed\":{\"attempt\":2,\"errors\":[\"pq: Cannot execute write query because system is in resize mode\"]}}"), model.ResourceNotFoundError,
			},
			{
				"RedShift ssl is not enabled on the server", warehouseutils.RS, errors.New("{\"exporting_data_failed\":{\"attempt\":1,\"errors\":[\"pq: SSL is not enabled on the server\"]}}"), model.PermissionError,
			},
			{
				"RedShift bucket not found", warehouseutils.RS, errors.New("{\"exporting_data_failed\":{\"attempt\":1,\"errors\":[\"Bucket *** not found.\"]}}"), model.ResourceNotFoundError,
			},
			{
				"RedShift tables can have at most 1600 columns", warehouseutils.RS, errors.New("{\"exporting_data_failed\":{\"attempt\":11,\"errors\":[\"update schema: adding columns to warehouse: failed to add columns for table *** in namespace *** of destination *** with error: pq: tables can have at most 1600 columns\"]}}"), model.ColumnCountError,
			},

			{
				"Snowflake requested warehouse does not exist or not authorized", warehouseutils.SNOWFLAKE, errors.New("{\"internal_processing_failed\":{\"attempt\":1,\"errors\":[\"SF: snowflake alter session error : (390201 (08004): The requested warehouse does not exist or not authorized.)\"]}}"), model.PermissionError,
			},
			{
				"Snowflake requested database does not exist or not authorized", warehouseutils.SNOWFLAKE, errors.New("{\"internal_processing_failed\":{\"attempt\":9,\"errors\":[\"SF: snowflake alter session error : (390420 (08004): IP *** is not allowed to access Snowflake. Contact your local security administrator or please create a case with Snowflake Support or reach us on our support line\"]}}"), model.PermissionError,
			},
			{
				"Snowflake verify account name is correct", warehouseutils.SNOWFLAKE, errors.New("{\"internal_processing_failed\":{\"attempt\":5,\"errors\":[\"SF: snowflake alter session error : (390201 (08004): The requested database does not exist or not authorized.)\"]}}"), model.PermissionError,
			},
			{
				"Snowflake incorrect username or password was specified", warehouseutils.SNOWFLAKE, errors.New("{\"internal_processing_failed\":{\"attempt\":3,\"errors\":[\"SF: snowflake alter session error : (260008 (08004): failed to connect to db. verify account name is correct.)\"]}}"), model.PermissionError,
			},
			{
				"Snowflake insufficient privileges to operate on table", warehouseutils.SNOWFLAKE, errors.New("{\"exporting_data_failed\":{\"attempt\":5,\"errors\":[\"failed to add columns for table *** in namespace *** of destination *** with error: 003001 (42501): SQL access control error:\nInsufficient privileges to operate on table\"]}}"), model.PermissionError,
			},
			{
				"Snowflake ip is not allowed to access Snowflake", warehouseutils.SNOWFLAKE, errors.New("{\"internal_processing_failed\":{\"attempt\":6,\"errors\":[\"SF: snowflake alter session error : (390100 (08004): Incorrect username or password was specified.)\"]}}"), model.PermissionError,
			},
			{
				"Snowflake user temporarily locked", warehouseutils.SNOWFLAKE, errors.New("{\"internal_processing_failed\":{\"attempt\":4,\"errors\":[\"SF: snowflake alter session error : (390102 (08004): User temporarily locked. Contact your local system administrator or please create a case with Snowflake Support or reach us on our support line\"]}}"), model.PermissionError,
			},
			{
				"Snowflake schema already exists, but current role has no privileges on it", warehouseutils.SNOWFLAKE, errors.New("{\"creating_remote_schema_failed\":{\"attempt\":5,\"errors\":[\"003041 (42710): SQL compilation error:Schema *** already exists, but current role has no privileges on it. If this is unexpected and you cannot resolve this problem, contact your system administrator. ACCOUNTADMIN role may be required to manage the privileges on the object.\"]}}"), model.PermissionError,
			},
			{
				"Snowflake aws access key id you provided is not valid", warehouseutils.SNOWFLAKE, errors.New("{\"exporting_data_failed\":{\"attempt\":1,\"errors\":[\"091003 (22000): Failure using stage area. Cause: [The AWS Access Key Id you provided is not valid.]\"]}}"), model.PermissionError,
			},
			{
				"Snowflake location is not allowed by integration", warehouseutils.SNOWFLAKE, errors.New("{\"exporting_data_failed\":{\"attempt\":4,\"errors\":[\"003127 (42601): SQL compilation error:Location *** is not allowed by integration ***. Please use DESC INTEGRATION to check out allowed and blocked locations.\"]}}"), model.PermissionError,
			},
			{
				"Snowflake quota exceeded", warehouseutils.SNOWFLAKE, errors.New("{\"fetching_remote_schema_failed\":{\"attempt\":5,\"errors\":[\"090073 (22000): Warehouse *** cannot be resumed because resource monitor *** has exceeded its quota.\"]}}"), model.InsufficientResourceError,
			},
			{
				"Snowflake free trial has ended", warehouseutils.SNOWFLAKE, errors.New("{\"internal_processing_failed\":{\"attempt\":8,\"errors\":[\"SF: snowflake alter session error : (390913 (08004): Your free trial has ended and all of your virtual warehouses have been suspended. Add billing information in the Snowflake web UI to continue using the full set of Snowflake features.)\"]}}"), model.InsufficientResourceError,
			},
			{
				"Snowflake table does not exist", warehouseutils.SNOWFLAKE, errors.New("{\"exporting_data_failed\":{\"attempt\":2,\"errors\":[\"001757 (42601): SQL compilation error:\nTable *** does not exist]}}"), model.ResourceNotFoundError,
			},
			{
				"Snowflake operation failed because soft limit on objects ", warehouseutils.SNOWFLAKE, errors.New("{\"exporting_data_failed\":{\"attempt\":4,\"errors\":[\"load table: copy into table: 090084 (22000): Operation failed because soft limit on objects of type 'Column' per table was exceeded. Please reduce number of 'Column's or contact Snowflake support about raising the limit.\"]}}"), model.ColumnCountError,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				m, err := manager.New(tc.destType, config.New(), logger.NOP, stats.NOP)
				require.NoError(t, err)

				er := &router.ErrorHandler{Mapper: m}

				jobErrorType := er.MatchUploadJobErrorType(tc.jobError)
				require.Equal(t, jobErrorType, tc.expectedJobErrorType)
			})
		}
	})

	t.Run("UnKnown errors", func(t *testing.T) {
		m, err := manager.New(warehouseutils.RS, config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)

		er := &router.ErrorHandler{Mapper: m}
		jobErrorType := er.MatchUploadJobErrorType(errors.New("unknown error"))
		require.Equal(t, jobErrorType, model.UncategorizedError)
	})

	t.Run("Nil manager", func(t *testing.T) {
		er := &router.ErrorHandler{Mapper: nil}
		jobErrorType := er.MatchUploadJobErrorType(errors.New("unknown error"))
		require.Equal(t, jobErrorType, model.UncategorizedError)
	})

	t.Run("Nil error: ", func(t *testing.T) {
		m, err := manager.New(warehouseutils.RS, config.New(), logger.NOP, stats.NOP)
		require.NoError(t, err)

		er := &router.ErrorHandler{Mapper: m}
		jobErrorType := er.MatchUploadJobErrorType(nil)
		require.Equal(t, jobErrorType, model.UncategorizedError)
	})
}

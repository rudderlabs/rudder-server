package configuration_testing

import (
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/rudderlabs/rudder-server/warehouse/clickhouse"
	"github.com/rudderlabs/rudder-server/warehouse/deltalake"
	"github.com/rudderlabs/rudder-server/warehouse/mssql"
	"github.com/rudderlabs/rudder-server/warehouse/postgres"
	"github.com/rudderlabs/rudder-server/warehouse/redshift"
	"github.com/rudderlabs/rudder-server/warehouse/snowflake"
	"github.com/rudderlabs/rudder-server/warehouse/utils"
	"strings"
)

// CreateSchemaQuery create schema query for warehouse
// It does not include:
// 1. BQ
// 2. S3_DATALAKE
// 3. AZURE_DATALAKE
// 4. GCS_DATALAKE
func (ct *CTHandleT) CreateSchemaQuery() (sqlStatement string) {
	defer func() {
		pkgLogger.Infof("[DCT]  Create schema query with sqlStatement: %s", sqlStatement)
	}()
	switch ct.warehouse.Type {
	case warehouseutils.POSTGRES, warehouseutils.SNOWFLAKE, warehouseutils.RS:
		sqlStatement = fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, ct.warehouse.Namespace)
	case warehouseutils.DELTALAKE:
		sqlStatement = fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, ct.warehouse.Namespace)
	case warehouseutils.AZURE_SYNAPSE, warehouseutils.MSSQL:
		sqlStatement = fmt.Sprintf(`IF NOT EXISTS ( SELECT * FROM sys.schemas WHERE name = N'%s' ) EXEC('CREATE SCHEMA [%s]');`,
			ct.warehouse.Namespace, ct.warehouse.Namespace)
	case warehouseutils.CLICKHOUSE:
		cluster := warehouseutils.GetConfigValue(clickhouse.Cluster, ct.warehouse)
		clusterClause := ""
		if len(strings.TrimSpace(cluster)) > 0 {
			clusterClause = fmt.Sprintf(`ON CLUSTER "%s"`, cluster)
		}
		sqlStatement = fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS "%s" %s`, ct.warehouse.Namespace, clusterClause)
	}
	return
}

// CreateTableQuery create table query for warehouse
// It does not include:
// 1. BQ
// 2. S3_DATALAKE
// 3. AZURE_DATALAKE
// 4. GCS_DATALAKE
func (ct *CTHandleT) CreateTableQuery() (sqlStatement string) {
	defer func() {
		pkgLogger.Infof("[DCT]  Create table query with sqlStatement: %s", sqlStatement)
	}()
	// preparing staging table name
	ct.stagingTableName = fmt.Sprintf(`%s%s`,
		StagingTablePrefix,
		strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", ""),
	)

	switch ct.warehouse.Type {
	case warehouseutils.AZURE_SYNAPSE, warehouseutils.MSSQL:
		sqlStatement = fmt.Sprintf(`IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'%[1]s') AND type = N'U')
			CREATE TABLE %[1]s ( %v )`,
			ct.stagingTableName,
			mssql.ColumnsWithDataTypes(TestTableSchemaMap, ""),
		)
	case warehouseutils.POSTGRES:
		sqlStatement = fmt.Sprintf(`CREATE TABLE "%[1]s"."%[2]s" ( %v ) `,
			ct.warehouse.Namespace,
			ct.stagingTableName,
			postgres.ColumnsWithDataTypes(TestTableSchemaMap, ""),
		)
	case warehouseutils.RS:
		sqlStatement = fmt.Sprintf(`CREATE TABLE "%[1]s"."%[2]s" ( %v ) `,
			ct.warehouse.Namespace,
			ct.stagingTableName,
			redshift.ColumnsWithDataTypes(TestTableSchemaMap, ""),
		)
	case warehouseutils.SNOWFLAKE:
		sqlStatement = fmt.Sprintf(`CREATE TABLE "%[1]s"."%[2]s" ( %v ) `,
			ct.warehouse.Namespace,
			ct.stagingTableName,
			snowflake.ColumnsWithDataTypes(TestTableSchemaMap, ""),
		)
	case warehouseutils.DELTALAKE:
		sqlStatement = fmt.Sprintf(`CREATE TABLE %[1]s.%[2]s ( %v ) USING DELTA;`,
			ct.warehouse.Namespace,
			ct.stagingTableName,
			deltalake.ColumnsWithDataTypes(TestTableSchemaMap, ""),
		)
	case warehouseutils.CLICKHOUSE:
		clusterClause := ""
		engine := "ReplacingMergeTree"
		engineOptions := ""
		cluster := warehouseutils.GetConfigValue(clickhouse.Cluster, ct.warehouse)
		if len(strings.TrimSpace(cluster)) > 0 {
			clusterClause = fmt.Sprintf(`ON CLUSTER "%s"`, cluster)
			engine = fmt.Sprintf(`%s%s`, "Replicated", engine)
			engineOptions = `'/clickhouse/{cluster}/tables/{database}/{table}', '{replica}'`
		}
		sqlStatement = fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" %s ( %v ) ENGINE = %s(%s) ORDER BY id PARTITION BY id`,
			ct.warehouse.Namespace,
			ct.stagingTableName,
			clusterClause,
			clickhouse.ColumnsWithDataTypes(ct.stagingTableName, TestTableSchemaMap, []string{"id"}),
			engine,
			engineOptions,
		)
	}
	return
}

// DropTableQuery drop table query for warehouse
// It does not include:
// 1. BQ
// 2. S3_DATALAKE
// 3. AZURE_DATALAKE
// 4. GCS_DATALAKE
func (ct *CTHandleT) DropTableQuery() (sqlStatement string) {
	defer func() {
		pkgLogger.Infof("[DCT] drop table query with sqlStatement: %s", sqlStatement)
	}()
	switch ct.warehouse.Type {
	case warehouseutils.POSTGRES, warehouseutils.SNOWFLAKE, warehouseutils.RS, warehouseutils.AZURE_SYNAPSE, warehouseutils.MSSQL:
		sqlStatement = fmt.Sprintf(`DROP TABLE "%[1]s"."%[2]s"`, ct.warehouse.Namespace, ct.stagingTableName)
	case warehouseutils.DELTALAKE:
		sqlStatement = fmt.Sprintf(`DROP TABLE %[1]s.%[2]s`, ct.warehouse.Namespace, ct.stagingTableName)
	case warehouseutils.CLICKHOUSE:
		cluster := warehouseutils.GetConfigValue(clickhouse.Cluster, ct.warehouse)
		clusterClause := ""
		if len(strings.TrimSpace(cluster)) > 0 {
			clusterClause = fmt.Sprintf(`ON CLUSTER "%s"`, cluster)
		}
		sqlStatement = fmt.Sprintf(`DROP TABLE "%s"."%s" %s `, ct.warehouse.Namespace, ct.stagingTableName, clusterClause)
	}
	return
}

package configuration_testing

import (
	"fmt"
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
	pkgLogger.Infof("[DCT] Create schema query with sqlStatement: %s", sqlStatement)
	return
}

// CreateTableQuery create table query for warehouse
// It does not include:
// 1. BQ
// 2. S3_DATALAKE
// 3. AZURE_DATALAKE
// 4. GCS_DATALAKE
func (ct *CTHandleT) CreateTableQuery() (sqlStatement string) {
	switch ct.warehouse.Type {
	case warehouseutils.AZURE_SYNAPSE, warehouseutils.MSSQL:
		sqlStatement = fmt.Sprintf(`IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'%[1]s') AND type = N'U')
			CREATE TABLE %[1]s ( %v )`,
			fmt.Sprintf("%s.%s", ct.warehouse.Namespace, ct.stagingTableName),
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
	pkgLogger.Infof("[DCT] Create table query with sqlStatement: %s", sqlStatement)
	return
}

// FetchSchemaQuery fetches schema for required namespace and staging table.
// It does not include:
// 1. S3_DATALAKE
// 2. AZURE_DATALAKE
// 3. GCS_DATALAKE
// 4. DELTALAKE
//
func (ct *CTHandleT) FetchSchemaQuery() (sqlStatement string) {
	switch ct.warehouse.Type {
	case warehouseutils.AZURE_SYNAPSE, warehouseutils.MSSQL:
		sqlStatement = fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s'`, ct.warehouse.Namespace)
	case warehouseutils.POSTGRES:
		sqlStatement = fmt.Sprintf(`select t.table_name, c.column_name, c.data_type from INFORMATION_SCHEMA.TABLES t LEFT JOIN INFORMATION_SCHEMA.COLUMNS c ON (t.table_name = c.table_name and t.table_schema = c.table_schema) WHERE t.table_schema = '%s'`, ct.warehouse.Namespace)
	case warehouseutils.SNOWFLAKE:
		sqlStatement = fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type FROM INFORMATION_SCHEMA.TABLES as t JOIN INFORMATION_SCHEMA.COLUMNS as c ON t.table_schema = c.table_schema and t.table_name = c.table_name WHERE t.table_schema = '%s'`, ct.warehouse.Namespace)
	case warehouseutils.CLICKHOUSE:
		sqlStatement = fmt.Sprintf(`select table, name, type from system.columns where database = '%s'`, ct.warehouse.Namespace)
	case warehouseutils.BQ:
		sqlStatement = fmt.Sprintf(`SELECT t.table_name, c.column_name, c.data_type FROM %[1]s.INFORMATION_SCHEMA.TABLES as t LEFT JOIN %[1]s.INFORMATION_SCHEMA.COLUMNS as c ON (t.table_name = c.table_name) and (t.table_type != 'VIEW') and (c.column_name != '_PARTITIONTIME' OR c.column_name IS NULL)`, ct.warehouse.Namespace)
	case warehouseutils.RS:
		sqlStatement = fmt.Sprintf(`SELECT table_name, column_name, data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '%s'`, ct.warehouse.Namespace)
	}
	pkgLogger.Infof("[DCT] Fetch schema query with sqlStatement: %s", sqlStatement)
	return
}

// AlterTableQuery alter table query for warehouse
// It does not include:
// 1. S3_DATALAKE
// 2. AZURE_DATALAKE
// 3. GCS_DATALAKE
// 4. BQ
func (ct *CTHandleT) AlterTableQuery() (sqlStatement string) {
	switch ct.warehouse.Type {
	case warehouseutils.AZURE_SYNAPSE, warehouseutils.MSSQL:
		sqlStatement = fmt.Sprintf(`IF NOT EXISTS (SELECT 1  FROM SYS.COLUMNS WHERE OBJECT_ID = OBJECT_ID(N'%[1]s') AND name = '%[2]s')
			ALTER TABLE %[1]s.%[2]s ADD %[3]s`, ct.warehouse.Namespace, ct.stagingTableName, mssql.ColumnsWithDataTypes(AlterColumnMap, ""))
	case warehouseutils.POSTGRES:
		sqlStatement = fmt.Sprintf(`ALTER TABLE %[1]s.%[2]s ADD COLUMN IF NOT EXISTS %s`, ct.warehouse.Namespace, ct.stagingTableName, postgres.ColumnsWithDataTypes(AlterColumnMap, ""))
	case warehouseutils.SNOWFLAKE:
		sqlStatement = fmt.Sprintf(`ALTER TABLE "%[1]s"."%[2]s" ADD COLUMN %[3]s`, ct.warehouse.Namespace, ct.stagingTableName, snowflake.ColumnsWithDataTypes(AlterColumnMap, ""))
	case warehouseutils.CLICKHOUSE:
		cluster := warehouseutils.GetConfigValue(clickhouse.Cluster, ct.warehouse)
		clusterClause := ""
		if len(strings.TrimSpace(cluster)) > 0 {
			clusterClause = fmt.Sprintf(`ON CLUSTER "%s"`, cluster)
		}
		sqlStatement = fmt.Sprintf(`ALTER TABLE "%s"."%s" %s ADD COLUMN IF NOT EXISTS %s`, ct.warehouse.Namespace, ct.stagingTableName, clusterClause, clickhouse.ColumnsWithDataTypes(ct.stagingTableName, AlterColumnMap, []string{}))
	case warehouseutils.RS:
		sqlStatement = fmt.Sprintf(`ALTER TABLE "%[1]s"."%[2]s" ADD COLUMN %s`, ct.warehouse.Namespace, ct.stagingTableName, redshift.ColumnsWithDataTypes(AlterColumnMap, ""))
	case warehouseutils.DELTALAKE:
		sqlStatement = fmt.Sprintf(`ALTER TABLE %[1]s.%[2]s ADD COLUMNS ( %s );`, ct.warehouse.Namespace, ct.stagingTableName, deltalake.ColumnsWithDataTypes(AlterColumnMap, ""))
	}
	pkgLogger.Infof("[DCT] Alter table query with sqlStatement: %s", sqlStatement)
	return
}

// DropTableQuery drop table query for warehouse
// It does not include:
// 1. BQ
// 2. S3_DATALAKE
// 3. AZURE_DATALAKE
// 4. GCS_DATALAKE
func (ct *CTHandleT) DropTableQuery() (sqlStatement string) {
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
	pkgLogger.Infof("[DCT] Drop table query with sqlStatement: %s", sqlStatement)
	return
}

package model

type QueryType string

const (
	CreateTable             QueryType = "create_table"
	TableExists             QueryType = "table_exists"
	ColumnExists            QueryType = "column_exists"
	SchemaExists            QueryType = "schema_exists"
	CreateSchema            QueryType = "create_schema"
	Copy                    QueryType = "copy"
	Merge                   QueryType = "merge"
	AlterSession            QueryType = "alter_session"
	DropTable               QueryType = "drop_table"
	AddColumns              QueryType = "add_columns"
	AlterColumn             QueryType = "alter_column"
	FetchSchema             QueryType = "fetch_schema"
	FetchTables             QueryType = "fetch_schema"
	FetchTableAttributes    QueryType = "fetch_table_attributes"
	FetchTabTablePartitions QueryType = "fetch_table_partitions"
	TableCount              QueryType = "table_count"
	Insert                  QueryType = "insert"
	Delete                  QueryType = "delete"
)

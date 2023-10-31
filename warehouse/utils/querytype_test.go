package warehouseutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetQueryType(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		want     string
		expected bool
	}{
		{"select 1", "Select * from table", "SELECT", true},
		{"select 2", "\t\n\n  \t\n\n  seLeCt * from table", "SELECT", true},
		{"select 3", "\n\t\tWITH load_files as (\n\t\t  SELECT\n\t\t\tlocation,\n\t\t\tmetadata,\n\t\t\t  " +
			"FROM\n\t\t\tt1\n\t\t  WHERE\n\t\t\ta IN (1,2,3) \n\t\t)\n\t\tSELECT\n\t\t  location,\n\t\t  " +
			"metadata\n\t\tFROM\n\t\t  t2\n\t\tWHERE\n\t\t  x = 1\n\t\t;\n", "SELECT", true},
		{"update 1", "\t\n\n  \t\n\n  UpDaTe something SET some_column = 'x'", "UPDATE", true},
		{"update 2", "\n\t\tUPDATE\n\t\t  t1\n\t\tSET\n\t\t  a=$2,b=$3\n\t\tWHERE\n\t\t id = $1;", "UPDATE", true},
		{"delete", "\t\n\n  \t\n\n  DeLeTe FROm something", "DELETE_FROM", true},
		{"insert", "\t\n\n  \t\n\n  InSerT INTO something", "INSERT_INTO", true},
		{"copy", "\t\n\n  \t\n\n  cOpY t1 from t2", "COPY", true},
		{"merge into", "\t\n\n  \t\n\n  mErGe InTo t1", "MERGE_INTO", true},
		{"create temp table 1", "\t\n\n  \t\n\n  create temp table t1", "CREATE_TEMP_TABLE", true},
		{"create temp table 2", "\t\n\n  \t\n\n  create tempORARY table t1", "CREATE_TEMP_TABLE", true},
		{"create database", "\t\n\n  \t\n\n  creATE dataBASE db1", "CREATE_DATABASE", true},
		{"create schema", "\t\n\n  \t\n\n  creATE schEMA sch1", "CREATE_SCHEMA", true},
		{"create table 1", "\t\n\n  \t\n\n  creATE tABLE t1", "CREATE_TABLE", true},
		{"create table 2", "\t\n\n  \t\n\n  If not exists something then creATE tABLE t1", "CREATE_TABLE", true},
		{"create or replace table", "\t\n\n  \t\n\n  creATE or replace tABLE t1", "CREATE_TABLE", true},
		{"create index", "\t\n\n  \t\n\n  creATE inDeX idx1", "CREATE_INDEX", true},
		{"alter table", "\t\n\n  \t\n\n  ALTer tABLE t1", "ALTER_TABLE", true},
		{"alter session", "\t\n\n  \t\n\n  ALTer seSsIoN s1", "ALTER_SESSION", true},
		{"drop table 1", "\t\n\n  \t\n\n  dROp Table t1", "DROP_TABLE", true},
		{"drop table 2", "\t\n\n  \t\n\n  IF OBJECT_ID ('foo.qux','X') IS NOT NULL DROP TABLE foo.bar", "DROP_TABLE", true},
		{"show tables", "\t\n\n  \t\n\n  sHoW tAbLes FROM some_table", "SHOW_TABLES", true},
		{"show partitions", "\t\n\n  \t\n\n  sHoW pArtItiOns billing.tracks_t1", "SHOW_PARTITIONS", true},
		{"show schemas", "\t\n\n  \t\n\n  sHoW schemaS like foobar", "SHOW_SCHEMAS", true},
		{"describe table 1", "\t\n\n  \t\n\n  dEscrIbe tABLE t1", "DESCRIBE_TABLE", true},
		{"describe table 2", "\t\n\n  \t\n\n  dEscrIbe qUeRy tABLE t1", "DESCRIBE_TABLE", true},
		{"set", "\t\n\n  \t\n\n  sEt something TO something_else", "SET_TO", true},
		{"unexpected", "\t\n\n  \t\n\n  ", "UNKNOWN", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, expected := GetQueryType(tt.query)
			require.Equalf(t, tt.want, got, "GetQueryType() value = %v, want %v", got, tt.want)
			require.Equalf(t, tt.want, got, "GetQueryType() expected = %v, want %v", tt.expected, expected)
		})
	}
}

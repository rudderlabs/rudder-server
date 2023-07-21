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
		{"update", "\t\n\n  \t\n\n  UpDaTe something", "UPDATE", true},
		{"delete", "\t\n\n  \t\n\n  DeLeTe FROm something", "DELETE FROM", true},
		{"insert", "\t\n\n  \t\n\n  InSerT INTO something", "INSERT INTO", true},
		{"copy", "\t\n\n  \t\n\n  cOpY t1 from t2", "COPY", true},
		{"create temp table 1", "\t\n\n  \t\n\n  create temp table t1", "CREATE TEMP TABLE", true},
		{"create temp table 2", "\t\n\n  \t\n\n  create tempORARY table t1", "CREATE TEMP TABLE", true},
		{"create database", "\t\n\n  \t\n\n  creATE dataBASE db1", "CREATE DATABASE", true},
		{"create schema", "\t\n\n  \t\n\n  creATE schEMA sch1", "CREATE SCHEMA", true},
		{"create table", "\t\n\n  \t\n\n  creATE tABLE t1", "CREATE TABLE", true},
		{"create index", "\t\n\n  \t\n\n  creATE inDeX idx1", "CREATE INDEX", true},
		{"alter table", "\t\n\n  \t\n\n  ALTer tABLE t1", "ALTER TABLE", true},
		{"alter session", "\t\n\n  \t\n\n  ALTer seSsIoN s1", "ALTER SESSION", true},
		{"drop table", "\t\n\n  \t\n\n  dROp Table t1", "DROP TABLE", true},
		{"unexpected", "\t\n\n  \t\n\n  something unexpected", "SOMETHING", false},
		{"empty", "\t\n\n  \t\n\n  ", "UNKNOWN", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, expected := GetQueryType(tt.query)
			require.Equalf(t, tt.want, got, "GetQueryType() value = %v, want %v", got, tt.want)
			require.Equalf(t, tt.want, got, "GetQueryType() expected = %v, want %v", tt.expected, expected)
		})
	}
}

package warehouseutils

import "testing"

func TestGetQueryType(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"select 1", "Select * from table", "SELECT"},
		{"select 2", "\t\n\n  \t\n\n  seLeCt * from table", "SELECT"},
		{"update", "\t\n\n  \t\n\n  UpDaTe something", "UPDATE"},
		{"delete", "\t\n\n  \t\n\n  DeLeTe FROm something", "DELETE FROM"},
		{"insert", "\t\n\n  \t\n\n  InSerT INTO something", "INSERT INTO"},
		{"create temp table 1", "\t\n\n  \t\n\n  create temp table t1", "CREATE TEMP TABLE"},
		{"create temp table 2", "\t\n\n  \t\n\n  create tempORARY table t1", "CREATE TEMP TABLE"},
		{"create schema", "\t\n\n  \t\n\n  creATE schEMA sch1", "CREATE SCHEMA"},
		{"create table", "\t\n\n  \t\n\n  creATE tABLE t1", "CREATE TABLE"},
		{"create index", "\t\n\n  \t\n\n  creATE inDeX idx1", "CREATE INDEX"},
		{"alter table", "\t\n\n  \t\n\n  ALTer tABLE t1", "ALTER TABLE"},
		{"alter session", "\t\n\n  \t\n\n  ALTer seSsIoN s1", "ALTER SESSION"},
		{"drop table", "\t\n\n  \t\n\n  dROp Table t1", "DROP TABLE"},
		{"unexpected", "\t\n\n  \t\n\n  something unexpected", "SOMETHING"},
		{"empty", "\t\n\n  \t\n\n  ", "UNKNOWN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetQueryType(tt.query); got != tt.want {
				t.Errorf("GetQueryType() = %v, want %v", got, tt.want)
			}
		})
	}
}

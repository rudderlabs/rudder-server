package dbsql

import "database/sql/driver"

type result struct {
	AffectedRows int64
	InsertId     int64
}

var _ driver.Result = (*result)(nil)

// LastInsertId returns the database's auto-generated ID after an insert into a table.
// This is currently not really implemented for this driver and will always return 0.
func (res *result) LastInsertId() (int64, error) {
	return res.InsertId, nil
}

// RowsAffected returns the number of rows affected by the query.
func (res *result) RowsAffected() (int64, error) {
	return res.AffectedRows, nil
}

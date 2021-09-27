package clickhouse

import (
	"database/sql/driver"
	"reflect"
	"strconv"
)

func (stmt *stmt) ColumnConverter(idx int) driver.ValueConverter {
	return converter{}
}

func (c *conn) CheckNamedValue(nv *driver.NamedValue) (err error) {
	nv.Value, err = converter{}.ConvertValue(nv.Value)
	return
}

type converter struct{}

const maxAllowedUInt64 = 1<<63 - 1

func (c converter) ConvertValue(v interface{}) (driver.Value, error) {
	if driver.IsValue(v) {
		return v, nil
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		}
		return c.ConvertValue(rv.Elem().Interface())
	case reflect.Uint64:
		u64 := rv.Uint()
		if u64 > maxAllowedUInt64 {
			s := strconv.FormatUint(u64, 10)
			bytes := []byte(s)
			return bytes, nil
		}
		return u64, nil
	}

	return driver.DefaultParameterConverter.ConvertValue(v)
}

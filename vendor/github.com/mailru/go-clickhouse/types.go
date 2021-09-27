package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"net"
	"strconv"
	"time"
)

// Array wraps slice or array into driver.Valuer interface to allow pass through it from database/sql
func Array(v interface{}) driver.Valuer {
	return array{v: v}
}

type array struct {
	v interface{}
}

// Value implements driver.Valuer
func (a array) Value() (driver.Value, error) {
	return textEncode.Encode(a)
}

// Date returns date for t
func Date(t time.Time) driver.Valuer {
	return date(t)
}

type date time.Time

// Value implements driver.Valuer
func (d date) Value() (driver.Value, error) {
	return []byte(formatDate(time.Time(d))), nil
}

// UInt64 returns uint64
func UInt64(u uint64) driver.Valuer {
	return bigUint64(u)
}

type bigUint64 uint64

// Value implements driver.Valuer
func (u bigUint64) Value() (driver.Value, error) {
	return []byte(strconv.FormatUint(uint64(u), 10)), nil
}

// Decimal32 converts value to Decimal32 of precision S.
// The value can be a number or a string. The S (scale) parameter specifies the number of decimal places.
func Decimal32(v interface{}, s int32) driver.Valuer {
	return decimal{32, s, v}
}

// Decimal64 converts value to Decimal64 of precision S.
// The value can be a number or a string. The S (scale) parameter specifies the number of decimal places.
func Decimal64(v interface{}, s int32) driver.Valuer {
	return decimal{64, s, v}
}

// Decimal128 converts value to Decimal128 of precision S.
// The value can be a number or a string. The S (scale) parameter specifies the number of decimal places.
func Decimal128(v interface{}, s int32) driver.Valuer {
	return decimal{128, s, v}
}

type decimal struct {
	p int32
	s int32
	v interface{}
}

// Value implements driver.Valuer
func (d decimal) Value() (driver.Value, error) {
	return []byte(fmt.Sprintf("toDecimal%d(%v, %d)", d.p, d.v, d.s)), nil
}

// IP returns compatible database format for net.IP
func IP(i net.IP) driver.Valuer {
	return ip(i)
}

type ip net.IP

// Value implements driver.Valuer
func (i ip) Value() (driver.Value, error) {
	return net.IP(i).String(), nil
}

// Tuple converts a struct into a tuple
// struct{A string, B int}{"a", 1} -> ("a", 1)
func Tuple(v interface{}) driver.Valuer {
	return tuple{v: v}
}

type tuple struct {
	v interface{}
}

func (t tuple) Value() (driver.Value, error) {
	return textEncode.Encode(t)
}

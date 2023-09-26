package rowscanner

import (
	"database/sql/driver"
	"strings"
	"time"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	dbsqlerrint "github.com/databricks/databricks-sql-go/internal/errors"
)

// RowScanner is an interface defining the behaviours that are specific to
// the formats in which query results can be returned.
type RowScanner interface {
	// ScanRow is called to populate the provided slice with the
	// content of the current row. The provided slice will be the same
	// size as the number of columns.
	// The dest should not be written to outside of ScanRow. Care
	// should be taken when closing a RowScanner not to modify
	// a buffer held in dest.
	ScanRow(dest []driver.Value, rowIndex int64) dbsqlerr.DBError

	// NRows returns the number of rows in the current result page
	NRows() int64

	// Close any open resources
	Close()
}

// Expected formats for TIMESTAMP and DATE types when represented by a string value
var DateTimeFormats map[string]string = map[string]string{
	"TIMESTAMP": "2006-01-02 15:04:05.999999999",
	"DATE":      "2006-01-02",
}

// IsNull return true if the bit at the provided position is set
func IsNull(nulls []byte, position int64) bool {
	index := position / 8
	if int64(len(nulls)) > index {
		b := nulls[index]
		return (b & (1 << (uint)(position%8))) != 0
	}
	return false
}

var ErrRowsParseValue = "databricks: unable to parse %s value '%v' from column %s"

// handleDateTime will convert the passed val to a time.Time value if necessary
func HandleDateTime(val any, dbType, columnName string, location *time.Location) (result any, err error) {
	result = val
	// if there is a date/time format corresponding to the column type we need to
	// convert to time.Time
	if format, ok := DateTimeFormats[dbType]; ok {
		result, err = parseInLocation(format, val.(string), location)
		if err != nil {
			err = dbsqlerrint.WrapErrf(err, ErrRowsParseValue, dbType, val, columnName)
		}
	}

	return result, err
}

// parseInLocation parses a date/time string in the given format and using the provided
// location.
// This is, essentially, a wrapper around time.ParseInLocation to handle negative year
// values
func parseInLocation(format, dateTimeString string, loc *time.Location) (time.Time, error) {
	// we want to handle dates with negative year values and currently we only
	// support formats that start with the year so we can just strip a leading minus
	// sign
	var isNegative bool
	dateTimeString, isNegative = stripLeadingNegative(dateTimeString)

	date, err := time.ParseInLocation(format, dateTimeString, loc)
	if err != nil {
		return time.Time{}, err
	}

	if isNegative {
		date = date.AddDate(-2*date.Year(), 0, 0)
	}

	return date, nil
}

// stripLeadingNegative will remove a leading ascii or unicode minus
// if present. The possibly shortened string is returned and a flag indicating if
// the string was altered
func stripLeadingNegative(dateTimeString string) (string, bool) {
	if dateStartsWithNegative(dateTimeString) {
		// strip leading rune from dateTimeString
		// using range because it is supposed to be faster than utf8.DecodeRuneInString
		for i := range dateTimeString {
			if i > 0 {
				return dateTimeString[i:], true
			}
		}
	}

	return dateTimeString, false
}

// ISO 8601 allows for both the ascii and unicode characters for minus
const (
	// unicode minus sign
	uMinus string = "\u2212"
	// ascii hyphen/minus
	aMinus string = "\x2D"
)

// dateStartsWithNegative returns true if the date string starts with
// a minus sign
func dateStartsWithNegative(val string) bool {
	return strings.HasPrefix(val, aMinus) || strings.HasPrefix(val, uMinus)
}

// GetDBTypeName returns the database type name from a TColumnDesc
func GetDBTypeName(column *cli_service.TColumnDesc) string {
	entry := column.TypeDesc.Types[0].PrimitiveEntry
	dbtype := strings.TrimSuffix(entry.Type.String(), "_TYPE")

	return dbtype
}

func GetDBType(column *cli_service.TColumnDesc) cli_service.TTypeId {
	entry := column.TypeDesc.Types[0].PrimitiveEntry
	return entry.Type
}

// GetDBTypeID returns the database type ID from a TColumnDesc
func GetDBTypeID(column *cli_service.TColumnDesc) cli_service.TTypeId {
	// currently the thrift server returns all types using the primitive entry
	entry := column.TypeDesc.Types[0].PrimitiveEntry
	return entry.Type
}

// GetDBTypeQualifiers returns the TTypeQualifiers from a TColumnDesc.
// Return value may be nil.
func GetDBTypeQualifiers(column *cli_service.TColumnDesc) *cli_service.TTypeQualifiers {
	return column.TypeDesc.Types[0].PrimitiveEntry.TypeQualifiers
}

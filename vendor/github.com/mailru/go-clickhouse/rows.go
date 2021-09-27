package clickhouse

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

type dataReader interface {
	Read() (record []string, err error)
}

func newTextRows(c *conn, body io.ReadCloser, location *time.Location, useDBLocation bool) (*textRows, error) {
	tsvReader := newReader(body)

	columns, err := tsvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("newTextRows: failed to parse the list of columns: %w", err)
	}

	types, err := tsvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("newTextRows: failed to parse the list of column types: %w", err)
	}
	for i := range types {
		types[i], err = readUnquoted(strings.NewReader(types[i]), 0)
		if err != nil {
			return nil, fmt.Errorf("newTextRows: failed to read the type '%s': %w", types[i], err)
		}
	}

	parsers := make([]DataParser, len(types))
	for i, typ := range types {
		desc, err := ParseTypeDesc(typ)
		if err != nil {
			return nil, fmt.Errorf("newTextRows: failed to parse a description of the type '%s': %w", typ, err)
		}

		parsers[i], err = NewDataParser(desc, &DataParserOptions{
			Location:      location,
			UseDBLocation: useDBLocation,
		})
		if err != nil {
			return nil, fmt.Errorf("newTextRows: failed to create a data parser for the type '%s': %w", typ, err)
		}
	}

	return &textRows{
		c:        c,
		respBody: body,
		tsv:      tsvReader,
		columns:  columns,
		types:    types,
		parsers:  parsers,
	}, nil
}

type textRows struct {
	c        *conn
	respBody io.ReadCloser
	tsv      dataReader
	columns  []string
	types    []string
	parsers  []DataParser
}

func (r *textRows) Columns() []string {
	return r.columns
}

func (r *textRows) Close() error {
	r.c.cancel = nil
	return r.respBody.Close()
}

func (r *textRows) Next(dest []driver.Value) error {
	row, err := r.tsv.Read()
	if err != nil {
		return err
	}

	// skip row before WITH TOTALS,
	// not but do not skip an empty line if it is part of the result
	if len(row) == 1 && row[0] == "" {
		row, err = r.tsv.Read()
		if err != nil {
			return err
		}
	}

	for i, s := range row {
		reader := strings.NewReader(s)
		v, err := r.parsers[i].Parse(reader)
		if err != nil {
			return err
		}
		if _, _, err := reader.ReadRune(); err != io.EOF {
			return fmt.Errorf("trailing data after parsing the value")
		}
		dest[i] = v
	}

	return nil
}

// ColumnTypeScanType implements the driver.RowsColumnTypeScanType
func (r *textRows) ColumnTypeScanType(index int) reflect.Type {
	return r.parsers[index].Type()
}

// ColumnTypeDatabaseTypeName implements the driver.RowsColumnTypeDatabaseTypeName
func (r *textRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.types[index]
}

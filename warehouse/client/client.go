package client

import (
	"context"
	"database/sql"
	"fmt"

	"cloud.google.com/go/bigquery"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"google.golang.org/api/iterator"
)

const (
	SQLClient = "SQLClient"
	BQClient  = "BigQueryClient"
)

type Client struct {
	SQL  *sql.DB
	BQ   *bigquery.Client
	Type string
}

func (cl *Client) sqlQuery(statement string) (result warehouseutils.QueryResult, err error) {
	rows, err := cl.SQL.Query(statement)
	if err != nil && err != sql.ErrNoRows {
		return result, err
	}
	if err == sql.ErrNoRows {
		return result, nil
	}
	defer rows.Close()

	result.Columns, err = rows.Columns()
	if err != nil {
		return result, err
	}

	colCount := len(result.Columns)
	values := make([]interface{}, colCount)
	valuePtrs := make([]interface{}, colCount)

	for rows.Next() {
		for i := 0; i < colCount; i++ {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			return result, err
		}
		var stringRow []string
		for i := 0; i < colCount; i++ {
			stringRow = append(stringRow, fmt.Sprintf("%+v", values[i]))
		}
		result.Values = append(result.Values, stringRow)
	}
	return result, err
}

func (cl *Client) bqQuery(statement string) (result warehouseutils.QueryResult, err error) {
	query := cl.BQ.Query(statement)
	context := context.Background()
	it, err := query.Read(context)
	if err != nil {
		return
	}

	for index := 0; index < len(it.Schema); index++ {
		result.Columns = append(result.Columns, (*it.Schema[index]).Name)
	}

	for {
		var row []bigquery.Value
		err = it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return
		}
		var stringRow []string
		for index := 0; index < len(row); index++ {
			stringRow = append(stringRow, fmt.Sprintf("%+v", row[index]))
		}
		result.Values = append(result.Values, stringRow)
	}
	return result, nil
}

func (cl *Client) Query(statement string) (result warehouseutils.QueryResult, err error) {
	switch cl.Type {
	case BQClient:
		return cl.bqQuery(statement)
	default:
		return cl.sqlQuery(statement)
	}
}

func (cl *Client) Close() {
	switch cl.Type {
	case BQClient:
		cl.BQ.Close()
	default:
		cl.SQL.Close()
	}
}

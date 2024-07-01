package db

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/flusher/report"
)

func TestGetStart(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	rows := sqlmock.NewRows([]string{"reported_at"}).AddRow(time.Now().UTC())

	mock.ExpectQuery("SELECT MIN").WillReturnRows(rows)

	p := &PostgresDB{
		db: db,
	}

	_, err = p.GetStart(context.Background(), "test_table")
	assert.NoError(t, err)
}

func TestFetchBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	rows := sqlmock.NewRows([]string{"col1", "col2"}).
		AddRow("val1", "val2")

	mock.ExpectQuery("SELECT \\* FROM test_table WHERE reported_at").WillReturnRows(rows)

	p := &PostgresDB{
		db: db,
	}

	result, err := p.FetchBatch(context.Background(), "test_table", time.Now().UTC(), time.Now().UTC(), 10, 0)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())

	// Check the result.
	expected := []report.RawReport{
		{
			"col1": "val1",
			"col2": "val2",
		},
	}
	assert.Equal(t, expected, result)
}

func TestDelete(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(1, 1))

	p := &PostgresDB{
		db: db,
	}

	err = p.Delete(context.Background(), "test_table", time.Now().UTC(), time.Now().UTC())
	assert.NoError(t, err)
}

func TestClose(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectClose()

	p := &PostgresDB{
		db: db,
	}

	err = p.Close()
	assert.NoError(t, err)
}

func TestFetchInternal(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	rows := sqlmock.NewRows([]string{"column1", "column2"}).
		AddRow("value1", "value2").
		AddRow("value3", "value4")

	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	p := &PostgresDB{
		db: db,
	}

	result, err := p.fetch(context.Background(), "SELECT * FROM table")
	assert.NoError(t, err)

	expected := []report.RawReport{
		{
			"column1": "value1",
			"column2": "value2",
		},
		{
			"column1": "value3",
			"column2": "value4",
		},
	}

	assert.Equal(t, expected, result)
}

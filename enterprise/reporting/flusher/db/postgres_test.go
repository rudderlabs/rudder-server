package db

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestGetStart(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	rows := sqlmock.NewRows([]string{"reported_at"}).AddRow(time.Now().UTC())

	mock.ExpectQuery("SELECT MIN").WillReturnRows(rows)

	p := &PostgresDB{
		DB: db,
	}

	_, err = p.GetStart(context.Background(), "test_table")
	assert.NoError(t, err)
}

func TestDelete(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectExec("DELETE FROM").WillReturnResult(sqlmock.NewResult(1, 1))

	p := &PostgresDB{
		DB: db,
	}

	err = p.Delete(context.Background(), "test_table", time.Now().UTC(), time.Now().UTC())
	assert.NoError(t, err)
}

func TestClose(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectClose()

	p := &PostgresDB{
		DB: db,
	}

	err = p.Close()
	assert.NoError(t, err)
}

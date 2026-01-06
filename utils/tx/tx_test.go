package tx

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestTx_SuccessListener(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	sqlTx, err := db.Begin()
	require.NoError(t, err)

	tx := &Tx{Tx: sqlTx}

	successCalled := false
	failureCalled := false

	tx.AddSuccessListener(func() {
		successCalled = true
	})
	tx.AddFailureListener(func() {
		failureCalled = true
	})

	err = tx.Commit()
	require.NoError(t, err)
	require.True(t, successCalled, "success listener should be called on successful commit")
	require.False(t, failureCalled, "failure listener should not be called on successful commit")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTx_FailureListenerOnCommitError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(sql.ErrConnDone)

	sqlTx, err := db.Begin()
	require.NoError(t, err)

	tx := &Tx{Tx: sqlTx}

	successCalled := false
	failureCalled := false

	tx.AddSuccessListener(func() {
		successCalled = true
	})
	tx.AddFailureListener(func() {
		failureCalled = true
	})

	err = tx.Commit()
	require.Error(t, err)
	require.False(t, successCalled, "success listener should not be called on commit error")
	require.True(t, failureCalled, "failure listener should be called on commit error")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTx_FailureListenerOnRollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	sqlTx, err := db.Begin()
	require.NoError(t, err)

	tx := &Tx{Tx: sqlTx}

	successCalled := false
	failureCalled := false

	tx.AddSuccessListener(func() {
		successCalled = true
	})
	tx.AddFailureListener(func() {
		failureCalled = true
	})

	err = tx.Rollback()
	require.NoError(t, err)
	require.False(t, successCalled, "success listener should not be called on rollback")
	require.True(t, failureCalled, "failure listener should be called on rollback")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTx_MultipleListeners(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	sqlTx, err := db.Begin()
	require.NoError(t, err)

	tx := &Tx{Tx: sqlTx}

	successCount := 0
	failureCount := 0

	tx.AddSuccessListener(func() { successCount++ })
	tx.AddSuccessListener(func() { successCount++ })
	tx.AddSuccessListener(func() { successCount++ })

	tx.AddFailureListener(func() { failureCount++ })
	tx.AddFailureListener(func() { failureCount++ })

	err = tx.Commit()
	require.NoError(t, err)
	require.Equal(t, 3, successCount, "all success listeners should be called")
	require.Equal(t, 0, failureCount, "no failure listeners should be called")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTx_NoListeners(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	sqlTx, err := db.Begin()
	require.NoError(t, err)

	tx := &Tx{Tx: sqlTx}

	// Should not panic with no listeners
	err = tx.Commit()
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

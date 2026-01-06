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

func TestTx_FinallyListenerOnCommit(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	sqlTx, err := db.Begin()
	require.NoError(t, err)

	tx := &Tx{Tx: sqlTx}

	finallyCalled := false
	tx.AddFinallyListener(func() {
		finallyCalled = true
	})

	err = tx.Commit()
	require.NoError(t, err)
	require.True(t, finallyCalled, "finally listener should be called after successful commit")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTx_FinallyListenerOnRollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	sqlTx, err := db.Begin()
	require.NoError(t, err)

	tx := &Tx{Tx: sqlTx}

	finallyCalled := false
	tx.AddFinallyListener(func() {
		finallyCalled = true
	})

	err = tx.Rollback()
	require.NoError(t, err)
	require.True(t, finallyCalled, "finally listener should be called after rollback")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTx_FinallyListenerOnCommitFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit().WillReturnError(sql.ErrConnDone)

	sqlTx, err := db.Begin()
	require.NoError(t, err)

	tx := &Tx{Tx: sqlTx}

	finallyCalled := false
	tx.AddFinallyListener(func() {
		finallyCalled = true
	})

	err = tx.Commit()
	require.Error(t, err)
	require.True(t, finallyCalled, "finally listener should be called even when commit fails")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTx_FinallyListenerOnlyFiresOnce_CommitThenRollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	sqlTx, err := db.Begin()
	require.NoError(t, err)

	tx := &Tx{Tx: sqlTx}

	finallyCount := 0
	failureCount := 0

	tx.AddFinallyListener(func() {
		finallyCount++
	})
	tx.AddFailureListener(func() {
		failureCount++
	})

	// Commit successfully
	err = tx.Commit()
	require.NoError(t, err)
	require.Equal(t, 1, finallyCount, "finally listener should fire once")
	require.Equal(t, 0, failureCount, "failure listener should not fire on successful commit")

	// Rollback after commit (common pattern: if err := tx.Commit(); err != nil { tx.Rollback() })
	// Listeners should NOT fire again because arrays are cleared
	_ = tx.Rollback()
	require.Equal(t, 1, finallyCount, "finally listener should NOT fire again")
	require.Equal(t, 0, failureCount, "failure listener should NOT fire on rollback after commit")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTx_FinallyListenerOnlyFiresOnce_RollbackThenCommit(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectRollback()

	sqlTx, err := db.Begin()
	require.NoError(t, err)

	tx := &Tx{Tx: sqlTx}

	finallyCount := 0
	failureCount := 0
	successCount := 0

	tx.AddFinallyListener(func() {
		finallyCount++
	})
	tx.AddFailureListener(func() {
		failureCount++
	})
	tx.AddSuccessListener(func() {
		successCount++
	})

	// Rollback first
	err = tx.Rollback()
	require.NoError(t, err)
	require.Equal(t, 1, finallyCount, "finally listener should fire once")
	require.Equal(t, 1, failureCount, "failure listener should fire on rollback")
	require.Equal(t, 0, successCount, "success listener should not fire on rollback")

	// Try to commit after rollback
	// Listeners should NOT fire again because arrays are cleared
	_ = tx.Commit()
	require.Equal(t, 1, finallyCount, "finally listener should NOT fire again")
	require.Equal(t, 1, failureCount, "failure listener should NOT fire again")
	require.Equal(t, 0, successCount, "success listener should NOT fire after rollback")

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTx_AllListenerTypes(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectCommit()

	sqlTx, err := db.Begin()
	require.NoError(t, err)

	tx := &Tx{Tx: sqlTx}

	order := []string{}
	tx.AddSuccessListener(func() {
		order = append(order, "success")
	})
	tx.AddFailureListener(func() {
		order = append(order, "failure")
	})
	tx.AddFinallyListener(func() {
		order = append(order, "finally")
	})

	err = tx.Commit()
	require.NoError(t, err)
	require.Equal(t, []string{"success", "finally"}, order, "success listener should fire before finally")

	require.NoError(t, mock.ExpectationsWereMet())
}

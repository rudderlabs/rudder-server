package tx

import "database/sql"

// Tx is a wrapper around sql.Tx that supports registering and executing
// post-commit actions, a.k.a. success listeners, and post-rollback actions, a.k.a. failure listeners.
type Tx struct {
	*sql.Tx
	successListeners []func()
	failureListeners []func()
}

// AddSuccessListener registers a listener to be executed after the transaction has been committed successfully.
func (tx *Tx) AddSuccessListener(listener func()) {
	tx.successListeners = append(tx.successListeners, listener)
}

// AddFailureListener registers a listener to be executed after the transaction has been rolled back.
func (tx *Tx) AddFailureListener(listener func()) {
	tx.failureListeners = append(tx.failureListeners, listener)
}

// Commit commits the transaction and executes all success listeners on success,
// or failure listeners if the commit fails.
func (tx *Tx) Commit() error {
	err := tx.Tx.Commit()
	if err == nil {
		for _, successListener := range tx.successListeners {
			successListener()
		}
	} else {
		for _, failureListener := range tx.failureListeners {
			failureListener()
		}
	}
	return err
}

// Rollback rolls back the transaction and executes all failure listeners.
func (tx *Tx) Rollback() error {
	err := tx.Tx.Rollback()
	for _, failureListener := range tx.failureListeners {
		failureListener()
	}
	return err
}

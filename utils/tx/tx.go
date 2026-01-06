package tx

import "database/sql"

// Tx is a wrapper around sql.Tx that supports registering and executing
// post-commit actions (success listeners), post-rollback actions (failure listeners),
// and post-completion actions (finally listeners that run regardless of outcome).
type Tx struct {
	*sql.Tx
	successListeners []func()
	failureListeners []func()
	finallyListeners []func()
}

// AddSuccessListener registers a listener to be executed after the transaction has been committed successfully.
func (tx *Tx) AddSuccessListener(listener func()) {
	tx.successListeners = append(tx.successListeners, listener)
}

// AddFailureListener registers a listener to be executed after the transaction has been rolled back.
func (tx *Tx) AddFailureListener(listener func()) {
	tx.failureListeners = append(tx.failureListeners, listener)
}

// AddFinallyListener registers a listener to be executed after the transaction completes,
// regardless of whether it was committed or rolled back. Finally listeners are executed
// after success/failure listeners. Useful for cleanup that should always happen.
func (tx *Tx) AddFinallyListener(listener func()) {
	tx.finallyListeners = append(tx.finallyListeners, listener)
}

// Commit commits the transaction and executes all success listeners on success,
// or failure listeners if the commit fails. Finally listeners are always executed.
// Failure and finally listeners are cleared to prevent double-firing if Rollback is called afterward.
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
	for _, finallyListener := range tx.finallyListeners {
		finallyListener()
	}

	// Clear failure and finally listeners to prevent double-firing if Rollback() is called after Commit()
	// Success listeners don't need clearing as they only fire in Commit() on success
	tx.failureListeners = nil
	tx.finallyListeners = nil

	return err
}

// Rollback rolls back the transaction and executes all failure listeners,
// followed by finally listeners.
// Failure and finally listeners are cleared to prevent double-firing if Commit is called afterward.
func (tx *Tx) Rollback() error {
	err := tx.Tx.Rollback()

	for _, failureListener := range tx.failureListeners {
		failureListener()
	}
	for _, finallyListener := range tx.finallyListeners {
		finallyListener()
	}

	// Clear failure and finally listeners to prevent double-firing if Commit() is called after Rollback()
	// Success listeners don't need clearing as they won't fire in Commit() after a Rollback()
	tx.failureListeners = nil
	tx.finallyListeners = nil

	return err
}

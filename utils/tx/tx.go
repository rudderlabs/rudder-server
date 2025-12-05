package tx

import "database/sql"

// Tx is a wrapper around sql.Tx that supports registering and executing
// post-commit actions, a.k.a. success listeners.
type Tx struct {
	*sql.Tx
	done                bool
	successListeners    []func()
	completionListeners []func()
	context             map[string]any
}

// AddSuccessListener registers a listener to be executed after the transaction has been committed successfully.
func (tx *Tx) AddSuccessListener(listener func()) {
	tx.successListeners = append(tx.successListeners, listener)
}

// AddCompletionListener registers a listener to be executed after the transaction has been completed, regardless of success or failure.
// Completion listeners are executed after success listeners.
func (tx *Tx) AddCompletionListener(listener func()) {
	tx.completionListeners = append(tx.completionListeners, listener)
}

// Commit commits the transaction and executes all listeners.
func (tx *Tx) Commit() error {
	err := tx.Tx.Commit()
	if err == nil {
		for _, successListener := range tx.successListeners {
			successListener()
		}
	}
	if !tx.done {
		for _, completionListener := range tx.completionListeners {
			completionListener()
		}
		tx.done = true
	}
	return err
}

func (tx *Tx) Rollback() error {
	err := tx.Tx.Rollback()
	for _, completionListener := range tx.completionListeners {
		completionListener()
	}
	tx.done = true
	return err
}

// SetValue sets a value to the tx context
func (tx *Tx) SetValue(key string, value any) {
	if tx.context == nil {
		tx.context = make(map[string]any)
	}
	tx.context[key] = value
}

// GetValue gets a value from the tx context
func (tx *Tx) GetValue(key string) (any, bool) {
	value, ok := tx.context[key]
	return value, ok
}

// GetTxValue gets a typed value from the tx context
func GetTxValue[T any](tx *Tx, key string) (T, bool) {
	var zero T
	value, ok := tx.GetValue(key)
	if !ok {
		return zero, false
	}
	typedValue, ok := value.(T)
	if !ok {
		return zero, false
	}
	return typedValue, true
}

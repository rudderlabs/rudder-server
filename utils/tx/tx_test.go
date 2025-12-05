package tx

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/postgres"
)

func TestTx(t *testing.T) {
	t.Run("success listeners are called on commit", func(t *testing.T) {
		db := setupTestDB(t)

		sqlTx, err := db.Begin()
		require.NoError(t, err)

		tx := &Tx{Tx: sqlTx}
		var called bool
		tx.AddSuccessListener(func() {
			called = true
		})
		err = tx.Commit()
		require.NoError(t, err)
		require.True(t, called, "success listener should be called")
	})

	t.Run("success listeners are not called on rollback", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		sqlTx, err := db.Begin()
		require.NoError(t, err)

		tx := &Tx{Tx: sqlTx}
		var called bool
		tx.AddSuccessListener(func() {
			called = true
		})

		err = tx.Rollback()
		require.NoError(t, err)
		require.False(t, called, "success listener should not be called on rollback")
	})

	t.Run("completion listeners are called on commit", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		sqlTx, err := db.Begin()
		require.NoError(t, err)

		tx := &Tx{Tx: sqlTx}
		var called bool
		tx.AddCompletionListener(func() {
			called = true
		})

		err = tx.Commit()
		require.NoError(t, err)
		require.True(t, called, "completion listener should be called")
	})

	t.Run("completion listeners are called on rollback", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		sqlTx, err := db.Begin()
		require.NoError(t, err)

		tx := &Tx{Tx: sqlTx}
		var called int
		tx.AddCompletionListener(func() {
			called++
		})

		err = tx.Rollback()
		require.NoError(t, err)
		require.Equal(t, 1, called, "completion listener should be called on rollback")
		err = tx.Commit()
		require.Error(t, err)
		require.Equal(t, 1, called, "completion listener shouldn't be called twice")
	})

	t.Run("multiple listeners are called in order", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		sqlTx, err := db.Begin()
		require.NoError(t, err)

		tx := &Tx{Tx: sqlTx}
		var order []int

		tx.AddSuccessListener(func() {
			order = append(order, 1)
		})
		tx.AddSuccessListener(func() {
			order = append(order, 2)
		})
		tx.AddCompletionListener(func() {
			order = append(order, 3)
		})
		tx.AddCompletionListener(func() {
			order = append(order, 4)
		})

		err = tx.Commit()
		require.NoError(t, err)
		require.Equal(t, []int{1, 2, 3, 4}, order, "listeners should be called in order")
	})

	t.Run("completion listeners are called after success listeners", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		sqlTx, err := db.Begin()
		require.NoError(t, err)

		tx := &Tx{Tx: sqlTx}
		var order []string

		tx.AddCompletionListener(func() {
			order = append(order, "completion")
		})
		tx.AddSuccessListener(func() {
			order = append(order, "success")
		})

		err = tx.Commit()
		require.NoError(t, err)
		require.Equal(t, []string{"success", "completion"}, order, "success should run before completion")
	})

	t.Run("SetValue and GetValue", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		sqlTx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = sqlTx.Rollback() }()

		tx := &Tx{Tx: sqlTx}

		// Set and get a string value
		tx.SetValue("key1", "value1")
		val, ok := tx.GetValue("key1")
		require.True(t, ok, "value should exist")
		require.Equal(t, "value1", val)

		// Set and get an int value
		tx.SetValue("key2", 42)
		val, ok = tx.GetValue("key2")
		require.True(t, ok, "value should exist")
		require.Equal(t, 42, val)

		// Get non-existent key
		_, ok = tx.GetValue("nonexistent")
		require.False(t, ok, "non-existent key should return false")
	})

	t.Run("GetTxValue with type safety", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		sqlTx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = sqlTx.Rollback() }()

		tx := &Tx{Tx: sqlTx}

		// Set and get typed values
		tx.SetValue("stringKey", "hello")
		tx.SetValue("intKey", 123)
		tx.SetValue("boolKey", true)

		// Get string value with correct type
		strVal, ok := GetTxValue[string](tx, "stringKey")
		require.True(t, ok, "string value should exist")
		require.Equal(t, "hello", strVal)

		// Get int value with correct type
		intVal, ok := GetTxValue[int](tx, "intKey")
		require.True(t, ok, "int value should exist")
		require.Equal(t, 123, intVal)

		// Get bool value with correct type
		boolVal, ok := GetTxValue[bool](tx, "boolKey")
		require.True(t, ok, "bool value should exist")
		require.Equal(t, true, boolVal)

		// Try to get with wrong type
		_, ok = GetTxValue[int](tx, "stringKey")
		require.False(t, ok, "wrong type should return false")

		// Get non-existent key
		_, ok = GetTxValue[string](tx, "nonexistent")
		require.False(t, ok, "non-existent key should return false")
	})

	t.Run("context persists across operations", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		sqlTx, err := db.Begin()
		require.NoError(t, err)

		tx := &Tx{Tx: sqlTx}

		// Set values before adding listeners
		tx.SetValue("counter", 0)

		// Success listener can access and modify context
		tx.AddSuccessListener(func() {
			val, ok := GetTxValue[int](tx, "counter")
			require.True(t, ok)
			tx.SetValue("counter", val+1)
		})

		// Completion listener can see modifications
		var finalCounter int
		tx.AddCompletionListener(func() {
			val, ok := GetTxValue[int](tx, "counter")
			require.True(t, ok)
			finalCounter = val
		})

		err = tx.Commit()
		require.NoError(t, err)
		require.Equal(t, 1, finalCounter, "context should be modified by listeners")
	})

	t.Run("overwriting context values", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		sqlTx, err := db.Begin()
		require.NoError(t, err)
		defer func() { _ = sqlTx.Rollback() }()

		tx := &Tx{Tx: sqlTx}

		// Set initial value
		tx.SetValue("key", "initial")
		val, ok := GetTxValue[string](tx, "key")
		require.True(t, ok)
		require.Equal(t, "initial", val)

		// Overwrite with new value
		tx.SetValue("key", "updated")
		val, ok = GetTxValue[string](tx, "key")
		require.True(t, ok)
		require.Equal(t, "updated", val)

		// Overwrite with different type
		tx.SetValue("key", 999)
		intVal, ok := GetTxValue[int](tx, "key")
		require.True(t, ok)
		require.Equal(t, 999, intVal)
	})
}

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pgResource, err := postgres.Setup(pool, t)
	require.NoError(t, err)
	return pgResource.DB
}

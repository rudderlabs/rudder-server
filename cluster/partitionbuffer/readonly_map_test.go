package partitionbuffer

import "testing"

func TestReadonlyMap(t *testing.T) {
	t.Run("Get", func(t *testing.T) {
		t.Run("should return value and true when key exists", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{"a": 1, "b": 2})
			val, ok := m.Get("a")
			if !ok {
				t.Error("expected ok to be true")
			}
			if val != 1 {
				t.Errorf("expected value 1, got %d", val)
			}
		})

		t.Run("should return zero value and false when key does not exist", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{"a": 1})
			val, ok := m.Get("b")
			if ok {
				t.Error("expected ok to be false")
			}
			if val != 0 {
				t.Errorf("expected zero value, got %d", val)
			}
		})

		t.Run("should work with empty map", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{})
			_, ok := m.Get("a")
			if ok {
				t.Error("expected ok to be false for empty map")
			}
		})
	})

	t.Run("Has", func(t *testing.T) {
		t.Run("should return true when key exists", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{"a": 1, "b": 2})
			if !m.Has("a") {
				t.Error("expected Has to return true")
			}
		})

		t.Run("should return false when key does not exist", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{"a": 1})
			if m.Has("b") {
				t.Error("expected Has to return false")
			}
		})

		t.Run("should work with empty map", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{})
			if m.Has("a") {
				t.Error("expected Has to return false for empty map")
			}
		})
	})

	t.Run("Len", func(t *testing.T) {
		t.Run("should return correct length", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{"a": 1, "b": 2, "c": 3})
			if m.Len() != 3 {
				t.Errorf("expected length 3, got %d", m.Len())
			}
		})

		t.Run("should return 0 for empty map", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{})
			if m.Len() != 0 {
				t.Errorf("expected length 0, got %d", m.Len())
			}
		})
	})

	t.Run("Keys", func(t *testing.T) {
		t.Run("should return all keys", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{"a": 1, "b": 2, "c": 3})
			keys := make(map[string]bool)
			for k := range m.Keys() {
				keys[k] = true
			}
			if len(keys) != 3 {
				t.Errorf("expected 3 keys, got %d", len(keys))
			}
			if !keys["a"] || !keys["b"] || !keys["c"] {
				t.Error("missing expected keys")
			}
		})

		t.Run("should return empty iterator for empty map", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{})
			count := 0
			for range m.Keys() {
				count++
			}
			if count != 0 {
				t.Errorf("expected 0 keys, got %d", count)
			}
		})
	})

	t.Run("Values", func(t *testing.T) {
		t.Run("should return all values", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{"a": 1, "b": 2, "c": 3})
			values := make(map[int]bool)
			for v := range m.Values() {
				values[v] = true
			}
			if len(values) != 3 {
				t.Errorf("expected 3 values, got %d", len(values))
			}
			if !values[1] || !values[2] || !values[3] {
				t.Error("missing expected values")
			}
		})

		t.Run("should return empty iterator for empty map", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{})
			count := 0
			for range m.Values() {
				count++
			}
			if count != 0 {
				t.Errorf("expected 0 values, got %d", count)
			}
		})
	})

	t.Run("ForEach", func(t *testing.T) {
		t.Run("should iterate over all key-value pairs", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{"a": 1, "b": 2, "c": 3})
			pairs := make(map[string]int)
			m.ForEach(func(k string, v int) {
				pairs[k] = v
			})
			if len(pairs) != 3 {
				t.Errorf("expected 3 pairs, got %d", len(pairs))
			}
			if pairs["a"] != 1 || pairs["b"] != 2 || pairs["c"] != 3 {
				t.Error("incorrect key-value pairs")
			}
		})

		t.Run("should not call function for empty map", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{})
			called := false
			m.ForEach(func(k string, v int) {
				called = true
			})
			if called {
				t.Error("expected ForEach not to be called for empty map")
			}
		})
	})

	t.Run("Append", func(t *testing.T) {
		t.Run("should return new map with added entries", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{"a": 1, "b": 2})
			newM := m.Append(map[string]int{"c": 3, "d": 4})

			if newM.Len() != 4 {
				t.Errorf("expected new map length 4, got %d", newM.Len())
			}
			if val, ok := newM.Get("c"); !ok || val != 3 {
				t.Error("expected new map to contain 'c': 3")
			}
			if val, ok := newM.Get("d"); !ok || val != 4 {
				t.Error("expected new map to contain 'd': 4")
			}
		})

		t.Run("should not modify original map", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{"a": 1, "b": 2})
			_ = m.Append(map[string]int{"c": 3})

			if m.Len() != 2 {
				t.Errorf("expected original map length 2, got %d", m.Len())
			}
			if m.Has("c") {
				t.Error("expected original map not to contain 'c'")
			}
		})

		t.Run("should overwrite existing keys", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{"a": 1, "b": 2})
			newM := m.Append(map[string]int{"a": 10, "c": 3})

			if val, ok := newM.Get("a"); !ok || val != 10 {
				t.Errorf("expected 'a' to be overwritten to 10, got %d", val)
			}
			if newM.Len() != 3 {
				t.Errorf("expected new map length 3, got %d", newM.Len())
			}
		})

		t.Run("should work with empty entries", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{"a": 1})
			newM := m.Append(map[string]int{})

			if newM.Len() != 1 {
				t.Errorf("expected new map length 1, got %d", newM.Len())
			}
		})

		t.Run("should work when appending to empty map", func(t *testing.T) {
			m := newReadOnlyMap(map[string]int{})
			newM := m.Append(map[string]int{"a": 1, "b": 2})

			if newM.Len() != 2 {
				t.Errorf("expected new map length 2, got %d", newM.Len())
			}
		})
	})

	t.Run("newReadOnlyMap", func(t *testing.T) {
		t.Run("should create map with provided data", func(t *testing.T) {
			data := map[string]int{"a": 1, "b": 2}
			m := newReadOnlyMap(data)

			if m.Len() != 2 {
				t.Errorf("expected length 2, got %d", m.Len())
			}
			if val, ok := m.Get("a"); !ok || val != 1 {
				t.Error("expected map to contain 'a': 1")
			}
		})

		t.Run("should work with nil map", func(t *testing.T) {
			m := newReadOnlyMap[string, int](nil)

			if m.Len() != 0 {
				t.Errorf("expected length 0, got %d", m.Len())
			}
		})
	})
}

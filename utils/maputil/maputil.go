package maputil

import "reflect"

func Compare[K comparable, V any](a, b map[K]V) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if w, ok := b[k]; !ok || !reflect.DeepEqual(v, w) {
			return false
		}
	}

	return true
}

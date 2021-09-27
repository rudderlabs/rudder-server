package clickhouse

import (
	"database/sql/driver"
)

func placeholders(query string) []int {
	n := 0
	quote := false
	first := -1
	for i := 0; i < len(query); i++ {
		switch query[i] {
		case '\\':
			i++
		case '\'':
			quote = !quote
		case '?':
			if !quote {
				n++
				if first == -1 {
					first = i
				}
			}
		}
	}
	if n == 0 {
		return nil
	}
	quote = false
	index := make([]int, n)
	n = 0
	for i, ch := range query[first:] {
		switch ch {
		case '\'':
			quote = !quote
		case '?':
			if !quote {
				index[n] = first + i
				n++
			}
		}
	}
	return index
}

func interpolateParams(query string, params []driver.Value) (string, error) {
	return interpolateParams2(query, params, placeholders(query))
}

func interpolateParams2(query string, params []driver.Value, index []int) (string, error) {
	if len(index) != len(params) {
		return "", ErrPlaceholderCount
	}
	if len(params) == 0 {
		return query, nil
	}

	var (
		queryRaw      = []byte(query)
		paramsEncoded = make([][]byte, len(params))
		n             = len(queryRaw) - len(index) // do not count number of placeholders
	)
	for i, v := range params {
		paramsEncoded[i], _ = textEncode.Encode(v)
		n += len(paramsEncoded[i])
	}
	buf := make([]byte, n)
	i := 0
	k := 0
	for j, idx := range index {
		copy(buf[k:], queryRaw[i:idx])
		k += idx - i
		copy(buf[k:], paramsEncoded[j])
		i = idx + 1
		k += len(paramsEncoded[j])
	}
	copy(buf[k:], query[i:])
	return string(buf), nil
}

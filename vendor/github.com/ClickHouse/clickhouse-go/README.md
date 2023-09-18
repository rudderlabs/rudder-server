# ClickHouse [![Build Status](https://travis-ci.org/ClickHouse/clickhouse-go.svg?branch=master)](https://travis-ci.org/ClickHouse/clickhouse-go) [![Go Report Card](https://goreportcard.com/badge/github.com/ClickHouse/clickhouse-go)](https://goreportcard.com/report/github.com/ClickHouse/clickhouse-go) [![codecov](https://codecov.io/gh/ClickHouse/clickhouse-go/branch/master/graph/badge.svg)](https://codecov.io/gh/ClickHouse/clickhouse-go)

Golang SQL database driver for [Yandex ClickHouse](https://clickhouse.yandex/)

## Key features

* Uses native ClickHouse TCP client-server protocol
* Compatibility with `database/sql`
* Round Robin load-balancing
* Bulk write support :  `begin->prepare->(in loop exec)->commit`
* LZ4 compression support (default is pure go lz4 or switch to use cgo lz4 by turning clz4 build tags on)
* External Tables support

## DSN

* username/password - auth credentials
* database - select the current default database
* read_timeout/write_timeout - timeout in second
* no_delay   - disable/enable the Nagle Algorithm for tcp socket (default is 'true' - disable)
* alt_hosts  - comma-separated list of single address hosts for load-balancing
* connection_open_strategy - random/in_order (default random).
    * random      - choose a random server from the set  
    * in_order    - first live server is chosen in specified order
    * time_random - choose random (based on the current time) server from the set. This option differs from `random` because randomness is based on the current time rather than on the number of previous connections.
* block_size - maximum rows in block (default is 1000000). If the rows are larger, the data will be split into several blocks to send to the server. If one block was sent to the server, the data would be persisted on the server disk, and we can't roll back the transaction. So always keep in mind that the batch size is no larger than the block_size if you want an atomic batch insert.
* pool_size - the maximum amount of preallocated byte chunks used in queries (default is 100). Decrease this if you experience memory problems at the expense of more GC pressure and vice versa.
* debug - enable debug output (boolean value)
* compress - enable lz4 compression (integer value, default is '0')
* check_connection_liveness - on supported platforms non-secure connections retrieved from the connection pool are checked in beginTx() for liveness before using them. If the check fails, the respective connection is marked as bad and the query retried with another connection. (boolean value, default is 'true')

SSL/TLS parameters:

* secure - establish secure connection (default is false)
* skip_verify - skip certificate verification (default is false)
* tls_config - name of a TLS config with client certificates, registered using `clickhouse.RegisterTLSConfig()`; implies secure to be true, unless explicitly specified

Example:

```sh
tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
```

## Supported data types

* UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
* Float32, Float64
* String
* FixedString(N)
* Date
* DateTime
* IPv4
* IPv6
* Enum
* UUID
* Nullable(T)
* [Array(T)](https://clickhouse.yandex/reference_en.html#Array(T)) [godoc](https://godoc.org/github.com/ClickHouse/clickhouse-go#Array)
* Array(Nullable(T))
* Tuple(...T)

## TODO

* Support other compression methods(zstd ...)

## Install

```sh
go get -u github.com/ClickHouse/clickhouse-go
```

## Examples

```go
package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go"
)

func main() {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS example (
			country_code FixedString(2),
			os_id        UInt8,
			browser_id   UInt8,
			categories   Array(Int16),
			action_day   Date,
			action_time  DateTime
		) engine=Memory
	`)

	if err != nil {
		log.Fatal(err)
	}
	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare("INSERT INTO example (country_code, os_id, browser_id, categories, action_day, action_time) VALUES (?, ?, ?, ?, ?, ?)")
	)
	defer stmt.Close()

	for i := 0; i < 100; i++ {
		if _, err := stmt.Exec(
			"RU",
			10+i,
			100+i,
			clickhouse.Array([]int16{1, 2, 3}),
			time.Now(),
			time.Now(),
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	rows, err := connect.Query("SELECT country_code, os_id, browser_id, categories, action_day, action_time FROM example")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			country               string
			os, browser           uint8
			categories            []int16
			actionDay, actionTime time.Time
		)
		if err := rows.Scan(&country, &os, &browser, &categories, &actionDay, &actionTime); err != nil {
			log.Fatal(err)
		}
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s", country, os, browser, categories, actionDay, actionTime)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	if _, err := connect.Exec("DROP TABLE example"); err != nil {
		log.Fatal(err)
	}
}
```

### Use [sqlx](https://github.com/jmoiron/sqlx)

```go
package main

import (
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/ClickHouse/clickhouse-go"
)

func main() {
	connect, err := sqlx.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	if err != nil {
		log.Fatal(err)
	}
	var items []struct {
		CountryCode string    `db:"country_code"`
		OsID        uint8     `db:"os_id"`
		BrowserID   uint8     `db:"browser_id"`
		Categories  []int16   `db:"categories"`
		ActionTime  time.Time `db:"action_time"`
	}

	if err := connect.Select(&items, "SELECT country_code, os_id, browser_id, categories, action_time FROM example"); err != nil {
		log.Fatal(err)
	}

	for _, item := range items {
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_time: %s", item.CountryCode, item.OsID, item.BrowserID, item.Categories, item.ActionTime)
	}
}
```

### External tables support

```go
package main

import (
	"database/sql"
    "database/sql/driver"
	"fmt"
    "github.com/ClickHouse/clickhouse-go/lib/column"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go"
)

func main() {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS example (
			country_code FixedString(2),
			os_id        UInt8,
			browser_id   UInt8,
			categories   Array(Int16),
			action_day   Date,
			action_time  DateTime
		) engine=Memory
	`)

	if err != nil {
		log.Fatal(err)
	}
	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare("INSERT INTO example (country_code, os_id, browser_id, categories, action_day, action_time) VALUES (?, ?, ?, ?, ?, ?)")
	)
	defer stmt.Close()

	for i := 0; i < 100; i++ {
		if _, err := stmt.Exec(
			"RU",
			10+i,
			100+i,
			clickhouse.Array([]int16{1, 2, 3}),
			time.Now(),
			time.Now(),
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	col, err := column.Factory("country_code", "String", nil)
	if err != nil {
		log.Fatal(err)
	}
	countriesExternalTable := clickhouse.ExternalTable{
		Name: "countries",
		Values: [][]driver.Value{
			{"RU"},
		},
		Columns: []column.Column{col},
	}
	
    rows, err := connect.Query("SELECT country_code, os_id, browser_id, categories, action_day, action_time "+
            "FROM example WHERE country_code IN ?", countriesExternalTable)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			country               string
			os, browser           uint8
			categories            []int16
			actionDay, actionTime time.Time
		)
		if err := rows.Scan(&country, &os, &browser, &categories, &actionDay, &actionTime); err != nil {
			log.Fatal(err)
		}
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s", country, os, browser, categories, actionDay, actionTime)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	if _, err := connect.Exec("DROP TABLE example"); err != nil {
		log.Fatal(err)
	}
}
```

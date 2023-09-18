# Databricks SQL Driver for Go


![http://www.apache.org/licenses/LICENSE-2.0.txt](http://img.shields.io/:license-Apache%202-brightgreen.svg)

## Description

This repo contains a Databricks SQL Driver for Go's [database/sql](https://golang.org/pkg/database/sql) package. It can be used to connect and query Databricks clusters and SQL Warehouses.

## Documentation

See `doc.go` for full documentation or the Databrick's documentation for [SQL Driver for Go](https://docs.databricks.com/dev-tools/go-sql-driver.html).

## Usage

```go
import (
  "context"
  "database/sql"
  _ "github.com/databricks/databricks-sql-go"
)

db, err := sql.Open("databricks", "token:********@********.databricks.com:443/sql/1.0/endpoints/********")
if err != nil {
  panic(err)
}
defer db.Close()


rows, err := db.QueryContext(context.Background(), "SELECT 1")
defer rows.Close()
```

Additional usage examples are available [here](https://github.com/databricks/databricks-sql-go/tree/main/examples).

### Connecting with DSN (Data Source Name)

The DSN format is:

```
token:[your token]@[Workspace hostname]:[Port number][Endpoint HTTP Path]?param=value
```

You can set query timeout value by appending a `timeout` query parameter (in seconds) and you can set max rows to retrieve per network request by setting the `maxRows` query parameter:

```
token:[your token]@[Workspace hostname]:[Port number][Endpoint HTTP Path]?timeout=1000&maxRows=1000
```
You can turn on Cloud Fetch to increase the performance of extracting large query results by fetching data in parallel via cloud storage (more info [here](https://www.databricks.com/blog/2021/08/11/how-we-achieved-high-bandwidth-connectivity-with-bi-tools.html)). To turn on Cloud Fetch, append `useCloudFetch=true`. You can also set the number of concurrently fetching goroutines by setting the `maxDownloadThreads` query parameter (default is 10):
```
token:[your token]@[Workspace hostname]:[Port number][Endpoint HTTP Path]?useCloudFetch=true&maxDownloadThreads=3
```

### Connecting with a new Connector

You can also connect with a new connector object. For example:

```go
import (
"database/sql"
  _ "github.com/databricks/databricks-sql-go"
)

connector, err := dbsql.NewConnector(
  dbsql.WithServerHostname(<Workspace hostname>),
  dbsql.WithPort(<Port number>),
  dbsql.WithHTTPPath(<Endpoint HTTP Path>),
  dbsql.WithAccessToken(<your token>)
)
if err != nil {
  log.Fatal(err)
}
db := sql.OpenDB(connector)
defer db.Close()
```

View `doc.go` or `connector.go` to understand all the functional options available when creating a new connector object.

## Develop

### Lint
We use `golangci-lint` as the lint tool. If you use vs code, just add the following settings:
``` json
{
    "go.lintTool": "golangci-lint",
    "go.lintFlags": [
        "--fast"
    ]
}
```
### Unit Tests

```bash
go test
```

## Issues

If you find any issues, feel free to create an issue or send a pull request directly.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

## License

[Apache 2.0](https://github.com/databricks/databricks-sql-go/blob/main/LICENSE)

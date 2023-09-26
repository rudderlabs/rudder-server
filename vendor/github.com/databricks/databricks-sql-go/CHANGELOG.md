# Release History

## v1.3.1 (2023-06-23)

- bug fix for panic when executing non record producing statements using DB.Query()/DB.QueryExec()

## v1.3.0 (2023-06-07)

- allow client provided authenticator
- more robust retry behaviour
- bug fix for null values in complex types

## v1.2.0 (2023-04-20)

- Improved error types and info

## v1.1.0 (2023-03-06)

- Feat: Support ability to retry on specific failures
- Fetch results in arrow format 
- Improve error message and retry behaviour

## v1.0.1 (2023-01-05)

Fixing cancel race condition 

## v1.0.0 (2022-12-20)

- Package doc (doc.go)
- Handle FLOAT values as float32
- Fix for result.AffectedRows
- Use new ctx when closing operation after cancel 
- Set default port to 443 

## v1.0.0-rc.1 (2022-12-19)

- Package doc (doc.go)
- Handle FLOAT values as float32
- Fix for result.AffectedRows
- Add or edit documentation above methods
- Tweaks to readme 
- Use new ctx when closing operation after cancel

## 0.2.2 (2022-12-12)

- Handle parsing negative years in dates
- fix thread safety issue 

## 0.2.1 (2022-12-05)

- Don't ignore error in InitThriftClient 
- Close optimization for Rows 
- Close operation after executing statement
- Minor change to examples
- P&R improvements 

## 0.1.x (Unreleased)

- Fix thread safety issue in connector

## 0.2.0 (2022-11-18)

- Support for DirectResults
- Support for context cancellation and timeout
- Session parameters (e.g.: timezone)
- Thrift Protocol update
- Several logging improvements
- Added better examples. See [workflow](https://github.com/databricks/databricks-sql-go/blob/main/examples/workflow/main.go)
- Added dbsql.NewConnector() function to help initialize DB
- Many other small improvements and bug fixes
- Removed support for client-side query parameterization
- Removed need to start DSN with "databricks://"

## 0.1.4 (2022-07-30)

- Fix: Could not fetch rowsets greater than the value of `maxRows` (#18)
- Updated default user agent
- Updated README and CONTRIBUTING

## 0.1.3 (2022-06-16)

- Add escaping of string parameters.

## 0.1.2 (2022-06-10)

- Fix timeout units to be milliseconds instead of nanos.

## 0.1.1 (2022-05-19)

- Fix module name

## 0.1.0 (2022-05-19)

- Initial release

# Go Snowflake Driver

<a href="https://codecov.io/github/snowflakedb/gosnowflake?branch=master">
    <img alt="Coverage" src="https://codecov.io/github/snowflakedb/gosnowflake/coverage.svg?branch=master">
</a>
<a href="https://github.com/snowflakedb/gosnowflake/actions?query=workflow%3A%22Build+and+Test%22">
    <img src="https://github.com/snowflakedb/gosnowflake/workflows/Build%20and%20Test/badge.svg?branch=master">
</a>
<a href="http://www.apache.org/licenses/LICENSE-2.0.txt">
    <img src="http://img.shields.io/:license-Apache%202-brightgreen.svg">
</a>
<a href="https://goreportcard.com/report/github.com/snowflakedb/gosnowflake">
    <img src="https://goreportcard.com/badge/github.com/snowflakedb/gosnowflake">
</a>

This topic provides instructions for installing, running, and modifying the Go Snowflake Driver. The driver supports Go's [database/sql](https://golang.org/pkg/database/sql/) package.

# Prerequisites

The following software packages are required to use the Go Snowflake Driver.

## Go

The latest driver requires the [Go language](https://golang.org/) 1.19 or higher. The supported operating systems are Linux, Mac OS, and Windows, but you may run the driver on other platforms if the Go language works correctly on those platforms.


# Installation

Get Gosnowflake source code, if not installed.

```sh
go get -u github.com/snowflakedb/gosnowflake
```

# Docs

For detailed documentation and basic usage examples, please see the documentation at
[godoc.org](https://godoc.org/github.com/snowflakedb/gosnowflake/).

# Sample Programs

Snowflake provides a set of sample programs to test with. Set the environment variable ``$GOPATH`` to the top directory of your workspace, e.g., ``~/go`` and make certain to 
include ``$GOPATH/bin`` in the environment variable ``$PATH``. Run the ``make`` command to build all sample programs.

```
make install
```

In the following example, the program ``select1.go`` is built and installed in ``$GOPATH/bin`` and can be run from the command line:

```
SNOWFLAKE_TEST_ACCOUNT=<your_account> \
SNOWFLAKE_TEST_USER=<your_user> \
SNOWFLAKE_TEST_PASSWORD=<your_password> \
select1
Congrats! You have successfully run SELECT 1 with Snowflake DB!
```

# Development

The developer notes are hosted with the source code on [GitHub](https://github.com/snowflakedb/gosnowflake).

## Testing Code


Set the Snowflake connection info in ``parameters.json``:

```
{
    "testconnection": {
        "SNOWFLAKE_TEST_USER":      "<your_user>",
        "SNOWFLAKE_TEST_PASSWORD":  "<your_password>",
        "SNOWFLAKE_TEST_ACCOUNT":   "<your_account>",
        "SNOWFLAKE_TEST_WAREHOUSE": "<your_warehouse>",
        "SNOWFLAKE_TEST_DATABASE":  "<your_database>",
        "SNOWFLAKE_TEST_SCHEMA":    "<your_schema>",
        "SNOWFLAKE_TEST_ROLE":      "<your_role>"
    }
}
```

Install [jq](https://stedolan.github.io/jq) so that the parameters can get parsed correctly, and run ``make test`` in your Go development environment:

```
make test
```

## Capturing Code Coverage

Configure your testing environment as described above and run ``make cov``. The coverage percentage will be printed on the console when the testing completes. 

```
make cov
```

For more detailed analysis, results are printed to ``coverage.txt`` in the project directory.

To read the coverage report, run:

```
go tool cover -html=coverage.txt
```

## Submitting Pull Requests

You may use your preferred editor to edit the driver code. Make certain to run ``make fmt lint`` before submitting any pull request to Snowflake. This command formats your source code according to the standard Go style and detects any coding style issues.

## Support

For official support, contact Snowflake support at:
[https://support.snowflake.net/](https://support.snowflake.net/).


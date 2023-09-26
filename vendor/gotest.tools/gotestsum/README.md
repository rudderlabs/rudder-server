# gotestsum

`gotestsum` runs tests using `go test -json`, prints formatted test output, and a summary of the test run.
It is designed to work well for both local development, and for automation like CI.

## Install

Download a binary from [releases](https://github.com/gotestyourself/gotestsum/releases), or build from
source with `go install gotest.tools/gotestsum@latest`. With `go` version before 1.17, use `go get gotest.tools/gotestsum`.

## Documentation

**Core features**
- [Output Format](#output-format) from compact to verbose, with color highlighting.
- [Summary](#summary) of the test run.
- [Add `go test` flags](#custom-go-test-command), or 
  [run a compiled test binary](#executing-a-compiled-test-binary).

**CI and Automation**
- [`--junitfile`](#junit-xml-output) - write a JUnit XML file for integration with CI systems.
- [`--jsonfile`](#json-file-output) - write the `test2json` output in a file.
- [`--rerun-fails`](#re-running-failed-tests) - run failed tests again to save time when dealing with flaky test suites.

**Local Development**
- [`--watch`](#run-tests-when-a-file-is-saved) - when a file is saved, run the tests for the package that includes the file.
- [`--post-run-command`](#post-run-command) - run a command after the tests, can be used for desktop notification.
- [`gotestsum tool slowest`](#finding-and-skipping-slow-tests) - find the slowest tests, also update slow tests to be skipepd with `-short`.


### Output Format

The `--format` flag or `GOTESTSUM_FORMAT` environment variable set the format that
is used to print the test names, and possibly test output, as the tests run. Most
outputs use color to highlight pass, fail, or skip.

Commonly used formats (see `--help` for a full list):

 * `dots` - print a character for each test.
 * `pkgname` (default) - print a line for each package.
 * `testname` - print a line for each test and package.
 * `standard-quiet` - the standard `go test` format.
 * `standard-verbose` - the standard `go test -v` format.

Have an idea for a new format?
Please [share it on github](https://github.com/gotestyourself/gotestsum/issues/new)!

#### Demo

A demonstration of three `--format` options.

![Demo](https://user-images.githubusercontent.com/442180/182284939-e08a0aa5-4504-4e30-9e88-207ef47f4537.gif)
<br /><sup>[Source](https://github.com/gotestyourself/gotestsum/tree/readme-demo/scripts)</sup>

### Summary

Following the formatted output is a summary of the test run. The summary includes:

 * The test output, and elapsed time, for any test that fails or is skipped.
 * The build errors for any package that fails to build.
 * A `DONE` line with a count of tests run, tests skipped, tests failed, package build errors,
   and the elapsed time including time to build.

   ```
   DONE 101 tests[, 3 skipped][, 2 failures][, 1 error] in 0.103s
   ```

To hide parts of the summary use `--hide-summary section`.


**Example: hide skipped tests in the summary**
```
gotestsum --hide-summary=skipped
```

**Example: hide everything except the DONE line**
```
gotestsum --hide-summary=skipped,failed,errors,output
# or
gotestsum --hide-summary=all
```

**Example: hide test output in the summary, only print names of failed and skipped tests
and errors**
```
gotestsum --hide-summary=output
```

### JUnit XML output

When the `--junitfile` flag or `GOTESTSUM_JUNITFILE` environment variable are set
to a file path, `gotestsum` will write a test report, in JUnit XML format, to the file.
This file can be used to integrate with CI systems.

```
gotestsum --junitfile unit-tests.xml
```

If the package names in the `testsuite.name` or `testcase.classname` fields do not
work with your CI system these values can be customized using the
`--junitfile-testsuite-name`, or `--junitfile-testcase-classname` flags. These flags
accept the following values:

* `short` - the base name of the package (the single term specified by the
  package statement).
* `relative` - a package path relative to the root of the repository
* `full` - the full package path (default)


Note: If Go is not installed, or the `go` binary is not in `PATH`, the `GOVERSION`
environment variable can be set to remove the "failed to lookup go version for junit xml"
warning.

### JSON file output

When the `--jsonfile` flag or `GOTESTSUM_JSONFILE` environment variable are set
to a file path, `gotestsum` will write a line-delimited JSON file with all the
[test2json](https://golang.org/cmd/test2json/#hdr-Output_Format)
output that was written by `go test -json`. This file can be used to compare test
runs, or find flaky tests.

```
gotestsum --jsonfile test-output.log
```

### Post Run Command

The `--post-run-command` flag may be used to execute a command after the
test run has completed. The binary will be run with the following environment
variables set:

```
GOTESTSUM_FORMAT        # gotestsum format (ex: short)
GOTESTSUM_JSONFILE      # path to the jsonfile, empty if no file path was given
GOTESTSUM_JUNITFILE     # path to the junit.xml file, empty if no file path was given
TESTS_ERRORS            # number of errors
TESTS_FAILED            # number of failed tests
TESTS_SKIPPED           # number of skipped tests
TESTS_TOTAL             # number of tests run
```

To get more details about the test run, such as failure messages or the full list of failed
tests, run `gotestsum` with either a `--jsonfile` or `--junitfile` and parse the
file from the post-run-command. The
[gotestsum/testjson](https://pkg.go.dev/gotest.tools/gotestsum/testjson?tab=doc)
package may be used to parse the JSON file output.

**Example: desktop notifications**

First install the example notification command with `go get gotest.tools/gotestsum/contrib/notify`.
The command will be downloaded to `$GOPATH/bin` as `notify`. Note that this
example `notify` command only works on macOS with
[terminal-notifer](https://github.com/julienXX/terminal-notifier) installed.

```
gotestsum --post-run-command notify
```

**Example: command with flags**

Possitional arguments or command line flags can be passed to the `--post-run-command` by
quoting the whole command.

```
gotestsum --post-run-command "notify me --date"
```

### Re-running failed tests

When the `--rerun-fails` flag is set, `gotestsum` will re-run any failed tests.
The tests will be re-run until each passes once, or the number of attempts
exceeds the maximum attempts. Maximum attempts defaults to 2, and can be changed
with `--rerun-fails=n`.

To avoid re-running tests when there are real failures, the re-run will be
skipped when there are too many test failures. By default this value is 10, and
can be changed with `--rerun-fails-max-failures=n`.

Note that using `--rerun-fails` may require the use of other flags, depending on
how you specify args to `go test`:

* when used with `--raw-command` the re-run will pass additional arguments to
  the command. The first arg is a `-test.run` flag with a regex that matches the test to re-run,
  and second is the name of a go package. These additional args can be passed to `go test`,
  or a test binary.
* when used with any `go test` args (anything after `--` on the command line), the list of
  packages to test must be specified as a space separated list using the `--packages` arg.

  **Example**

  ```
  gotestsum --rerun-fails --packages="./..." -- -count=2
  ```

* if any of the `go test` args should be passed to the test binary, instead of
  `go test` itself, the `-args` flag must be used to separate the two groups of
  arguments. `-args` is a special flag that is understood by `go test` to indicate
  that any following args should be passed directly to the test binary.

  **Example**

  ```
  gotestsum --rerun-fails --packages="./..." -- -count=2 -args -update-golden
  ```


### Custom `go test` command

By default `gotestsum` runs tests using the command `go test -json ./...`. You
can change the command with positional arguments after a `--`. You can change just the
test directory value (which defaults to `./...`) by setting the `TEST_DIRECTORY`
environment variable.

You can use `--debug` to echo the command before it is run.

**Example: set build tags**
```
gotestsum -- -tags=integration ./...
```

**Example: run tests in a single package**
```
gotestsum -- ./io/http
```

**Example: enable coverage**
```
gotestsum -- -coverprofile=cover.out ./...
```

**Example: run a script instead of `go test`**
```
gotestsum --raw-command -- ./scripts/run_tests.sh
```

Note: when using `--raw-command`, the script must follow a few rules about
stdout and stderr output:

* The stdout produced by the script must only contain the `test2json` output, or
  `gotestsum` will fail. If it isn't possible to change the script to avoid
  non-JSON output, you can use `--ignore-non-json-output-lines` (added in version 1.7.0)
  to ignore non-JSON lines and write them to `gotestsum`'s stderr instead.
* Any stderr produced by the script will be considered an error (this behaviour
  is necessary because package build errors are only reported by writting to
  stderr, not the `test2json` stdout). Any stderr produced by tests is not
  considered an error (it will be in the `test2json` stdout).

**Example: accept intput from stdin**
```
cat out.json | gotestsum --raw-command -- cat
```

**Example: run tests with profiling enabled**

Using a `profile.sh` script like this:

```sh
#!/usr/bin/env bash
set -eu

for pkg in $(go list "$@"); do
    dir="$(go list -f '{{ .Dir }}' $pkg)"
    go test -json -cpuprofile="$dir/cpu.profile" "$pkg"
done
```

You can run:
```
gotestsum --raw-command ./profile.sh ./...
```

**Example: using `TEST_DIRECTORY`**
```
TEST_DIRECTORY=./io/http gotestsum
```

### Executing a compiled test binary

`gotestsum` supports executing a compiled test binary (created with `go test -c`) by running
it as a custom command.

The `-json` flag is handled by `go test` itself, it is not available when using a
compiled test binary, so `go tool test2json` must be used to get the output
that `gotestsum` expects.

**Example: running `./binary.test`**

```
gotestsum --raw-command -- go tool test2json -t -p pkgname ./binary.test -test.v
```

`pkgname` is the name of the package being tested, it will show up in the test
output. `./binary.test` is the path to the compiled test binary. The `-test.v`
must be included so that `go tool test2json` receives all the output.

To execute a test binary without installing Go, see
[running without go](./.project/docs/running-without-go.md).


### Finding and skipping slow tests

`gotestsum tool slowest` reads [test2json output][testjson],
from a file or stdin, and prints the names and elapsed time of slow tests.
The tests are sorted from slowest to fastest.

`gotestsum tool slowest` can also rewrite the source of tests slower than the
threshold, making it possible to optionally skip them.

The [test2json output][testjson] can be created with `gotestsum --jsonfile` or `go test -json`.

See `gotestsum tool slowest --help`.

**Example: printing a list of tests slower than 500 milliseconds**

```
$ gotestsum --format dots --jsonfile json.log
[.]····↷··↷·
$ gotestsum tool slowest --jsonfile json.log --threshold 500ms
gotest.tools/example TestSomething 1.34s
gotest.tools/example TestSomethingElse 810ms
```

**Example: skipping slow tests with `go test --short`**

Any test slower than 200 milliseconds will be modified to add:

```go
if testing.Short() {
    t.Skip("too slow for testing.Short")
}
```

```sh
go test -json -short ./... | gotestsum tool slowest --skip-stmt "testing.Short" --threshold 200ms
```

Use `git diff` to see the file changes.
The next time tests are run using `--short` all the slow tests will be skipped.

[testjson]: https://golang.org/cmd/test2json/


### Run tests when a file is saved 

When the `--watch` flag is set, `gotestsum` will watch directories using
[file system notifications](https://pkg.go.dev/github.com/fsnotify/fsnotify).
When a Go file in one of those directories is modified, `gotestsum` will run the
tests for the package which contains the changed file. By default all
directories with at least one file with a `.go` extension, under the current
directory will be watched. Use the `--packages` flag to specify a different list.

If `--watch` is used with a command line that includes the name of one or more
packages as command line arguments (ex: `gotestsum --watch -- ./...` or
`gotestsum --watch -- ./extrapkg`), the
tests in those packages will also be run when any file changes.

While in watch mode, pressing some keys will perform an action:

* `r` will run tests for the previous event.
  Added in version 1.6.1.
* `u` will run tests for the previous event, with the `-update` flag added.
  Many [golden](https://gotest.tools/v3/golden) packages use this flag to automatically
  update expected values of tests.
  Added in version 1.8.1.
* `d` will run tests for the previous event using `dlv test`, allowing you to 
  debug a test failure using [delve]. A breakpoint will automatically be added at
  the first line of any tests which failed in the previous run. Additional
  breakpoints can be added with [`runtime.Breakpoint`](https://golang.org/pkg/runtime/#Breakpoint)
  or by using the delve command prompt.
  Added in version 1.6.1.
* `a` will run tests for all packages, by using `./...` as the package selector.
  Added in version 1.7.0.
* `l` will scan the directory list again, and if there are any new directories
  which contain a file with a `.go` extension, they will be added to the watch
  list.
  Added in version 1.7.0.

Note that [delve] must be installed in order to use debug (`d`).

[delve]: https://github.com/go-delve/delve

**Example: run tests for a package when any file in that package is saved**
```
gotestsum --watch --format testname
```

## Development

[![Godoc](https://godoc.org/gotest.tools/gotestsum?status.svg)](https://pkg.go.dev/gotest.tools/gotestsum?tab=subdirectories)
[![CircleCI](https://circleci.com/gh/gotestyourself/gotestsum/tree/main.svg?style=shield)](https://circleci.com/gh/gotestyourself/gotestsum/tree/main)
[![Go Reportcard](https://goreportcard.com/badge/gotest.tools/gotestsum)](https://goreportcard.com/report/gotest.tools/gotestsum)

Pull requests and bug reports are welcome! Please open an issue first for any
big changes.

## Thanks

This package is heavily influenced by the [pytest](https://docs.pytest.org) test runner for `python`.

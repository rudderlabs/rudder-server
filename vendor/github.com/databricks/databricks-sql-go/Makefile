BINARY         = databricks-sql-go
PACKAGE        = github.com/databricks/$(BINARY)
VER_PREFIX     = $(PACKAGE)/version
PRODUCTION     ?= false
DATE           = $(shell date "+%Y-%m-%d")
GIT_BRANCH     = $(shell git rev-parse --abbrev-ref 2>/dev/null)
GIT_COMMIT     = $(shell git rev-parse HEAD 2>/dev/null)
GIT_TAG        = $(shell if [ -z "`git status --porcelain`" ]; then git describe --exact-match --tags HEAD 2>/dev/null; fi)
VERSIONREL     = $(shell if [ -z "`git status --porcelain`" ]; then git rev-parse --short HEAD 2>/dev/null ; else echo "dirty"; fi)
PKGS           = $(shell go list ./... | grep -v /vendor)
LDFLAGS        = -X $(VER_PREFIX).Version=$(VERSIONREL) -X $(VER_PREFIX).Revision=$(GIT_COMMIT) -X $(VER_PREFIX).Branch=$(GIT_BRANCH) -X $(VER_PREFIX).BuildUser=$(shell whoami)  -X $(VER_PREFIX).BuildDate=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GOBUILD_ARGS   =
GO             = CGO_ENABLED=0 go
PLATFORMS      = windows linux darwin
os             = $(word 1, $@)

TEST_RESULTS_DIR ?= ./test-results


ifneq (${GIT_TAG},)
override DOCKER_TAG = ${GIT_TAG}
override VERSIONREL = ${GIT_TAG}
endif

ifeq (${PRODUCTION}, true)
LDFLAGS += -w -s -extldflags "-static"
GOBUILD_ARGS += -v
endif

.PHONY: help
help:  ## Show this help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {sub("\\\\n",sprintf("\n%22c"," "), $$2);printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: all
all: gen fmt lint test coverage  ## format and test everything

bin/golangci-lint: go.mod go.sum
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./bin v1.48.0

bin/gotestsum: go.mod go.sum
	@mkdir -p bin/
	$(GO) build -o bin/gotestsum gotest.tools/gotestsum

.PHONY: tools
tools: bin/golangci-lint bin/gotestsum  ## Build the development tools

.PHONY: fmt
fmt:  ## Format the go code.
	gofmt -w -s .

.PHONY: lint
lint: bin/golangci-lint  ## Lint the go code to ensure code sanity.
	./bin/golangci-lint run

.PHONY: test
test: bin/gotestsum  ## Run the go unit tests.
	@echo "INFO: Running all go unit tests."
	CGO_ENABLED=0 ./bin/gotestsum --format pkgname-and-test-fails --junitfile $(TEST_RESULTS_DIR)/unit-tests.xml ./...

.PHONY: test-race
test-race:
	@echo "INFO: Running all go unit tests checking for race conditions."
	go test -race


.PHONY: coverage
coverage: bin/gotestsum  ## Report the unit test code coverage.
	@echo "INFO: Generating unit test coverage report."
	CGO_ENABLED=0 ./bin/gotestsum --format pkgname-and-test-fails -- -coverprofile=$(TEST_RESULTS_DIR)/coverage.txt -covermode=atomic ./...
	go tool cover -html=$(TEST_RESULTS_DIR)/coverage.txt -o $(TEST_RESULTS_DIR)/coverage.html

.PHONY: sec
sec: lint  ## Run the snyk vulnerability scans.
	@echo "INFO: Running go vulnerability scans."
	snyk test .

.PHONY: build
build: linux darwin  ## Build the multi-arch binaries

.PHONY: $(PLATFORMS)
$(PLATFORMS):
	mkdir -p bin
	GOOS=$(os) GOARCH=amd64 $(GO) build $(GOBUILD_ARGS) -ldflags '$(LDFLAGS)' -o bin/$(BINARY)-$(os)-amd64 .

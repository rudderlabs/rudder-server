.PHONY: help default build run run-mt test test-run test-teardown mocks

GO=go
LDFLAGS?=-s -w
TESTFILE=_testok
MOUNT_PATH= /local

default: build

mocks: install-tools ## Generate all mocks
	$(GO) generate ./...

test: install-tools test-run test-teardown

test-run: ## Run all unit tests
ifeq ($(filter 1,$(debug) $(RUNNER_DEBUG)),)
	$(eval TEST_CMD = SLOW=0 gotestsum --format pkgname-and-test-fails --)
	$(eval TEST_OPTIONS = -p=1 -v -failfast -shuffle=on -coverprofile=profile.out -covermode=atomic -coverpkg=./... -vet=all --timeout=15m)
else
	$(eval TEST_CMD = SLOW=0 go test)
	$(eval TEST_OPTIONS = -p=1 -v -failfast -shuffle=on -coverprofile=profile.out -covermode=atomic -coverpkg=./... -vet=all --timeout=15m)
endif
ifdef package
ifdef exclude
	$(eval FILES = `go list ./$(package)/... | egrep -iv '$(exclude)'`)
	$(TEST_CMD) -count=1 $(TEST_OPTIONS) $(FILES) && touch $(TESTFILE) || true
else
	$(TEST_CMD) $(TEST_OPTIONS) ./$(package)/... && touch $(TESTFILE) || true
endif
else ifdef exclude
	$(eval FILES = `go list ./... | egrep -iv '$(exclude)'`)
	$(TEST_CMD) -count=1 $(TEST_OPTIONS) $(FILES) && touch $(TESTFILE) || true
else
	$(TEST_CMD) -count=1 $(TEST_OPTIONS) ./... && touch $(TESTFILE) || true
endif

test-warehouse-integration:
	$(eval TEST_PATTERN = 'TestIntegration')
	$(eval TEST_CMD = SLOW=1 go test)
	$(eval TEST_OPTIONS = -v -p 8 -timeout 30m -count 1 -run $(TEST_PATTERN) -coverprofile=profile.out -covermode=atomic -coverpkg=./...)
	$(TEST_CMD) $(TEST_OPTIONS) ./$(package)/... && touch $(TESTFILE) || true

test-warehouse: test-warehouse-integration test-teardown

test-teardown:
	@if [ -f "$(TESTFILE)" ]; then \
    	echo "Tests passed, tearing down..." ;\
		rm -f $(TESTFILE) ;\
		echo "mode: atomic" > coverage.txt ;\
		find . -name "profile.out" | while read file; do grep -v 'mode: atomic' $${file} >> coverage.txt; rm -f $${file}; done ;\
	else \
    	rm -f coverage.txt coverage.html ; find . -name "profile.out" | xargs rm -f ;\
		echo "Tests failed :-(" ;\
		exit 1 ;\
	fi

coverage:
	go tool cover -html=coverage.txt -o coverage.html

test-with-coverage: test coverage

build: ## Build rudder-server binary
	$(eval BUILD_OPTIONS = )
ifeq ($(RACE_ENABLED), TRUE)
	$(eval BUILD_OPTIONS = $(BUILD_OPTIONS) -race -o rudder-server-with-race)
endif
	$(GO) build $(BUILD_OPTIONS) -a -installsuffix cgo -ldflags="$(LDFLAGS)"
	$(GO) build -o build/wait-for-go/wait-for-go build/wait-for-go/wait-for.go
	$(GO) build -o build/regulation-worker ./regulation-worker/cmd/

run: ## Run rudder-server using go run
	$(GO) run main.go

run-mt: ## Run rudder-server in multi-tenant deployment type
	$(GO) run ./cmd/devtool etcd mode --no-wait normal
	$(GO) run ./cmd/devtool etcd workspaces --no-wait none
	DEPLOYMENT_TYPE=MULTITENANT $(GO) run main.go

help: ## Show the available commands
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' ./Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


install-tools:
	go install github.com/golang/mock/mockgen@v1.6.0
	go install mvdan.cc/gofumpt@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2.0
	go install gotest.tools/gotestsum@v1.10.0
	go install golang.org/x/tools/cmd/goimports@latest
	bash ./scripts/install-golangci-lint.sh v1.55.2

.PHONY: lint
lint: fmt ## Run linters on all go files
	golangci-lint run -v --timeout 5m

.PHONY: fmt
fmt: install-tools ## Formats all go files
	gofumpt -l -w -extra  .
	find . -type f -name '*.go' -exec grep -L -E 'Code generated by .*\. DO NOT EDIT.' {} + | xargs goimports -format-only -w -local=github.com/rudderlabs
	go run .github/tools/matrixchecker/main.go

.PHONY: proto
proto: install-tools ## Generate protobuf files
	protoc --go_out=paths=source_relative:. proto/**/*.proto
	protoc --go-grpc_out=paths=source_relative:. proto/**/*.proto

.PHONY: bench-kafka
bench-kafka:
	go test -count 1 -run BenchmarkCompression -bench=. -benchmem ./services/streammanager/kafka/client

.PHONY: generate-openapi-spec
generate-openapi-spec: install-tools
	docker run --rm \
	  -v ${PWD}:${MOUNT_PATH} openapitools/openapi-generator-cli generate \
	  -i ${MOUNT_PATH}/gateway/openapi.yaml \
	  -g html2 \
	  -o ${MOUNT_PATH}/gateway


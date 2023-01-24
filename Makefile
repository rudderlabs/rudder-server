.PHONY: help default build run run-mt test test-run test-teardown mocks

GO=go
LDFLAGS?=-s -w
TESTFILE=_testok

default: build

mocks: install-tools ## Generate all mocks
	$(GO) generate ./...

test: install-tools test-run test-teardown

test-run: ## Run all unit tests
ifeq ($(filter 1,$(debug) $(RUNNER_DEBUG)),)
	$(eval TEST_CMD = SLOW=0 gotestsum --format pkgname-and-test-fails --)
	$(eval TEST_OPTIONS = -p=1 -v -failfast -shuffle=on -coverprofile=profile.out -covermode=count -coverpkg=./... -vet=all --timeout=15m)
else
	$(eval TEST_CMD = SLOW=0 go test)
	$(eval TEST_OPTIONS = -p=1 -v -failfast -shuffle=on -coverprofile=profile.out -covermode=count -coverpkg=./... -vet=all --timeout=15m)
endif
ifdef package
	$(TEST_CMD) $(TEST_OPTIONS) $(package) && touch $(TESTFILE) || true
else
	$(TEST_CMD) -count=1 $(TEST_OPTIONS) ./... && touch $(TESTFILE) || true
endif

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
	go install gotest.tools/gotestsum@v1.8.2

.PHONY: lint
lint: fmt ## Run linters on all go files
	docker run --rm -v $(shell pwd):/app:ro -w /app golangci/golangci-lint:v1.49.0 bash -e -c \
		'golangci-lint run -v --timeout 5m'

.PHONY: fmt
fmt: install-tools ## Formats all go files
	gofumpt -l -w -extra  .

.PHONY: proto
proto: install-tools ## Generate protobuf files
	protoc --go_out=paths=source_relative:. proto/**/*.proto
	protoc --go-grpc_out=paths=source_relative:. proto/**/*.proto

cleanup-warehouse-integration:
	docker-compose -f warehouse/integrations/docker-compose.test.yml down --remove-orphans --volumes

define generate_namespace
$(shell echo wh-$(shell shuf -i 1-1000000 -n 1)-$(shell date +%s))
endef

setup-warehouse-integration: cleanup-warehouse-integration
	$(eval TEST_ENV = BIGQUERY_INTEGRATION_TEST_SCHEMA=$(call generate_namespace) REDSHIFT_INTEGRATION_TEST_SCHEMA=$(call generate_namespace) SNOWFLAKE_INTEGRATION_TEST_SCHEMA=$(call generate_namespace) DATABRICKS_INTEGRATION_TEST_SCHEMA=$(call generate_namespace))\
	$(eval TEST_CMD = $(TEST_ENV) docker-compose -f warehouse/integrations/docker-compose.test.yml up --build start_warehouse_integration)\
 	if $(TEST_CMD); then\
		echo "Warehouse integration setup successful"; \
	else \
	  	echo "Warehouse integration setup failed" ;\
      	docker logs wh-backend; \
        make cleanup-warehouse-integration; \
      	exit 1 ;\
    fi

run-warehouse-integration: setup-warehouse-integration
	$(eval TEST_PATTERN = '^TestIntegration' '^TestConfigurationValidation')
	$(eval TEST_CMD = go test -v ./warehouse/... -p 8 -timeout 30m -count 1 -run $(TEST_PATTERN))
	if docker-compose -f warehouse/integrations/docker-compose.test.yml exec -T -e SLOW=1 wh-backend $(TEST_CMD); then \
      	echo "Successfully ran Warehouse Integration Test. Getting backend container logs only."; \
      	docker logs wh-backend; \
      	make cleanup-warehouse-integration; \
    else \
      	echo "Failed running Warehouse Integration Test. Getting all logs from all containers"; \
		docker logs wh-backend; \
      	make cleanup-warehouse-integration; \
      	exit 1; \
 	fi

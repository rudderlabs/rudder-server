.PHONY: help default build run run-dev test mocks prepare-build

GO=go
GINKGO=ginkgo
LDFLAGS?=-s -w

default: build

mocks: install-tools ## Generate all mocks
	$(GO) generate ./...

test: ## Run all unit tests
ifdef package
	SLOW=0 $(GINKGO) -p --randomize-all --randomize-suites --fail-on-pending --cover -tags=integration \
		-coverprofile=profile.out -covermode=atomic --trace -keep-separate-coverprofiles $(package)
else
	SLOW=0 $(GINKGO) -p --randomize-all --randomize-suites --fail-on-pending --cover -tags=integration \
		-coverprofile=profile.out -covermode=atomic --trace -keep-separate-coverprofiles ./...
endif
	echo "mode: atomic" > coverage.txt
	find . -name "profile.out" | while read file;do grep -v 'mode: atomic' $${file} >> coverage.txt; rm $${file};done

coverage:
	go tool cover -html=coverage.txt -o coverage.html

test-with-coverage: test coverage

build-sql-migrations: ./services/sql-migrator/migrations_vfsdata.go ## Prepare sql migrations embedded scripts

prepare-build: build-sql-migrations

./services/sql-migrator/migrations_vfsdata.go: $(shell find sql/migrations)
	$(GO) run -tags=dev cmd/generate-migrations/generate-sql-migrations.go

build: prepare-build ## Build rudder-server binary
	$(eval BUILD_OPTIONS = )
ifeq ($(RACE_ENABLED), TRUE)
	$(eval BUILD_OPTIONS = $(BUILD_OPTIONS) -race -o rudder-server-with-race)
endif
	$(GO) build $(BUILD_OPTIONS) -a -installsuffix cgo -ldflags="$(LDFLAGS)"
	$(GO) build -o build/wait-for-go/wait-for-go build/wait-for-go/wait-for.go
	$(GO) build -o build/regulation-worker ./regulation-worker/cmd/

run: prepare-build ## Run rudder-server using go run
	$(GO) run main.go

run-dev: prepare-build ## Run rudder-server using go run with 'dev' build tag
	$(GO) run -tags=dev main.go

help: ## Show the available commands
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' ./Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


install-tools:
	go install github.com/golang/mock/mockgen@v1.6.0 || \
	GO111MODULE=on go install github.com/golang/mock/mockgen@v1.6.0

	go install mvdan.cc/gofumpt@latest

.PHONY: lint
lint: fmt
	docker run --rm -v $(shell pwd):/app:ro -w /app golangci/golangci-lint:v1.46.2 bash -e -c \
		'golangci-lint run -v --timeout 5m'

.PHONY: fmt
fmt: install-tools
	gofumpt -l -w -extra  .

cleanup-warehouse-integration:
	docker-compose -f warehouse/docker-compose.test.yml down --remove-orphans --volumes

setup-warehouse-integration: cleanup-warehouse-integration
	BIGQUERY_INTEGRATION_TEST_SCHEMA=bwh-$(shell uuidgen)	\
	REDSHIFT_INTEGRATION_TEST_SCHEMA=rwh-$(shell uuidgen)	\
	SNOWFLAKE_INTEGRATION_TEST_SCHEMA=swh-$(shell uuidgen) 	\
	DATABRICKS_INTEGRATION_TEST_SCHEMA=dwh-$(shell uuidgen) 	\
	docker-compose -f warehouse/docker-compose.test.yml up --build start_warehouse_integration

logs-warehouse-integration:
	docker logs wh-backend

run-warehouse-integration: setup-warehouse-integration
	if docker-compose -f warehouse/docker-compose.test.yml exec -T wh-backend go test -v ./warehouse/... -tags=warehouse_integration -p 8 -timeout 30m -count 1; then \
      	echo "Successfully ran Warehouse Integration Test. Getting backend container logs only."; \
      	make logs-warehouse-integration; \
      	make cleanup-warehouse-integration; \
    else \
      	echo "Failed running Warehouse Integration Test. Getting all logs from all containers"; \
      	make logs-warehouse-integration; \
      	make cleanup-warehouse-integration; \
      	exit 1; \
 	fi

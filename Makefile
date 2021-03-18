.PHONY: help default build run run-dev test mocks prepare-build enterprise-init enterprise-cleanup enterprise-update-commit enterprise-prepare-build

GO=go
GINKGO=ginkgo
LDFLAGS?=-s -w

include .enterprise/env

default: build

mocks: ## Generate all mocks
	$(GO) generate ./...

test: enterprise-prepare-build mocks ## Run all unit tests
# removed -p(parallel) inorder to pass builds
ifdef package
	$(GINKGO) -mod vendor --randomizeAllSpecs --randomizeSuites --failOnPending --cover -coverprofile=profile.out -covermode=atomic --trace --skipPackage=tests $(package)
else
	$(GINKGO) -mod vendor --randomizeAllSpecs --randomizeSuites --failOnPending --cover -coverprofile=profile.out -covermode=atomic --trace --skipPackage=tests ./...
endif

build-sql-migrations: ./services/sql-migrator/migrations_vfsdata.go ## Prepare sql migrations embedded scripts

prepare-build: build-sql-migrations enterprise-prepare-build

./services/sql-migrator/migrations_vfsdata.go: $(shell find sql/migrations)
	$(GO) run -tags=dev generate-sql-migrations.go

build: prepare-build ## Build rudder-server binary
	$(eval BUILD_OPTIONS = )
ifeq ($(RACE_ENABLED), TRUE)
	$(eval BUILD_OPTIONS = $(BUILD_OPTIONS) -race -o rudder-server-with-race)
endif
	$(GO) build $(BUILD_OPTIONS) -mod vendor -a -installsuffix cgo -ldflags="$(LDFLAGS)"
	$(GO) build -o build/wait-for-go/wait-for-go build/wait-for-go/wait-for.go 

run: prepare-build ## Run rudder-server using go run
	$(GO) run -mod=vendor main.go

run-dev: prepare-build ## Run rudder-server using go run with 'dev' build tag
	$(GO) run -mod=vendor -tags=dev main.go

help: ## Show the available commands
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' ./Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


# Enterprise version

enterprise-init: ## Initialise enterprise version
	@.enterprise/scripts/init.sh

enterprise-cleanup: ## Cleanup enterprise dependencies, revert to oss version
	rm -rf ${ENTERPRISE_DIR}
	rm -f ./imports/enterprise.go

enterprise-update-commit: ## Updates linked enterprise commit to current commit in ENTERPRISE_DIR
	@.enterprise/scripts/update-commit.sh


enterprise-prepare-build: ## Create ./imports/enterprise.go, to link enterprise packages in binary
	@if [ -d "./$(ENTERPRISE_DIR)" ]; then \
		$(ENTERPRISE_DIR)/import.sh ./$(ENTERPRISE_DIR) | tee ./imports/enterprise.go; \
	else \
		rm -f ./imports/enterprise.go; \
	fi

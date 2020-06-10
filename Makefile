.PHONY: help run test enterprise-init enterprise-update-commit enterprise-prepare-build

GO=go
GINKGO=ginkgo
LDFLAGS?=-s -w

include .enterprise/env

echo:
	echo $(ENTERPRISE_COMMIT_FILE)
	echo $(ENTERPRISE_COMMIT)

all: build

mocks: ## Generate all mocks
	$(GO) generate ./...

test: enterprise-prepare-build mocks ## Run all unit tests
ifdef package
	$(GINKGO) --randomizeAllSpecs -p --skipPackage=tests $(package)
else
	$(GINKGO) --randomizeAllSpecs -p --skipPackage=tests ./...
endif

build-sql-migrations: ./services/sql-migrator/migrations_vfsdata.go ## Prepare sql migrations embedded scripts	

./services/sql-migrator/migrations_vfsdata.go: sql/migrations
	$(GO) run -tags=dev generate-sql-migrations.go
	
build: build-sql-migrations ## Build rudder-server binary
	$(GO) build -mod vendor -a -installsuffix cgo -ldflags="$(LDFLAGS)"

run: enterprise-prepare-build ## Run rudder-server using go run
	$(GO) run -mod=vendor main.go

run-dev: enterprise-prepare-build ## Run rudder-server using go run with 'dev' build tag
	$(GO) run -mod=vendor -tags=dev main.go

help: ## Show the available commands
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' ./Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


# Enterprise version

enterprise-init: ## Initialise enterprise version
	@.enterprise/scripts/init.sh

enterprise-update-commit: ## Updates linked enterprise commit to current commit in ENTERPRISE_DIR
	@.enterprise/scripts/update-commit.sh
	

enterprise-prepare-build: ## Create ./imports/enterprise.go, to link enterprise packages in binary
	@if [ -d "./$(ENTERPRISE_DIR)" ]; then \
		$(ENTERPRISE_DIR)/import.sh ./$(ENTERPRISE_DIR) > ./imports/enterprise.go; \
	else \
		rm -f ./imports/enterprise.go; \
	fi

enterprise-build: enterprise-prepare-build build ## Build rudder-server enterprise version

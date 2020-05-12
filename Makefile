.PHONY: run test prepare-enterprise

GO=go
GINKGO=ginkgo

all: build

mocks:
	$(GO) generate ./...

checkout-enterprise:
	@if [ ! -d "./enterprise" ]; then \
	  git clone git@github.com:rudderlabs/rudder-server-enterprise.git enterprise; \
	fi
	@if [ -f ".enterprise-commit" ]; then \
	  cd ./enterprise; git checkout `cat ../.enterprise-commit`; \
	fi

prepare-enterprise:
	@if [ -d "./enterprise" ]; then \
		enterprise/import.sh ./enterprise > ./imports/enterprise.go; \
	else \
		rm -f ./imports/enterprise.go; \
	fi

test: prepare-enterprise mocks
	$(GINKGO) --randomizeAllSpecs -p --skipPackage=tests ./...

build: prepare-enterprise
	$(GO) build -o bin/rudder-server main.go

run: prepare-enterprise
	$(GO) run -mod=vendor main.go

.PHONY: run test prepare-enterprise

GO=go
GINKGO=ginkgo

all: build

mocks:
	$(GO) generate ./...

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

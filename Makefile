.PHONY: run test prepare-enterprise

GO=go
GINKGO=ginkgo

all: run

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

run: prepare-enterprise
	$(GO) run -mod=vendor main.go

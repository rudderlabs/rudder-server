default: help

.PHONY: help
help: ## Show the available commands
	@grep -E '^[0-9a-zA-Z_-]+:.*?## .*$$' ./Makefile | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: install-tools
install-tools: ## install necessary tools
	go install mvdan.cc/gofumpt@latest

.PHONY: lint 
lint: fmt ## Run linters on all go files
	docker run --rm -v $(shell pwd):/app:ro -w /app golangci/golangci-lint:v1.49.0 bash -e -c \
		'golangci-lint run -v --timeout 5m'

.PHONY: fmt 
fmt: install-tools ## Formats all go files
	gofumpt -l -w -extra  .

.PHONY: test
test:
	go test ./...
lint:
	if [ ! -f ./bin/golangci-lint ] ; \
	then \
		curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v1.32.2; \
	fi;
	./bin/golangci-lint run -e gosec

test:
	go test -covermode=count -coverprofile=count.out -v ./...

mock:
	@rm -rf mocks
	mockery --name Locker

.PHONY: lint test mock

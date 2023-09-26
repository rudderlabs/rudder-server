#
# Makefile to build ZMS Service Token client utility
# Prerequisite: Go development environment
#
# Copyright 2016 Yahoo Inc.
# Licensed under the Apache License, Version 2.0 - http://www.apache.org/licenses/LICENSE-2.0
#

GOPKGNAME = github.com/AthenZ/athenz/libs/go/zmssvctoken

# check to see if go utility is installed
GO := $(shell command -v go 2> /dev/null)
export GOPATH=$(PWD)

ifdef GO

# we need to make sure we have go 1.11+
# the output for the go version command is:
# go version go1.11.1 darwin/amd64

GO_VER_GTEQ11 := $(shell expr `go version | cut -f 3 -d' ' | cut -f2 -d.` \>= 11)
ifneq "$(GO_VER_GTEQ11)" "1"
all:
	@echo "Please install 1.11.x or newer version of golang"
else

.PHONY: vet fmt build test
all: vet fmt build test

endif

else

all:
	@echo "go is not available please install golang"

endif

vet:
	go vet .

fmt:
	gofmt -l .

build:
	@echo "Building zmssvctoken library..."
	go install -v $(GOPKGNAME)

test:
	go test -v $(GOPKGNAME)

clean:
	rm -rf target

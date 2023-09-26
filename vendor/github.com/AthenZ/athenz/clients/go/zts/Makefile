#
# Makefile to generate ZTS Go client and model code
# based on the ZTS RDL files. 
# Prerequisite: Go development environment
#
# Copyright 2016 Yahoo Inc.
# Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
#

RDL_FILE=../../../core/zts/src/main/rdl/ZTS.rdl
RDL_LIB=github.com/ardielle/ardielle-go/rdl

# check to see if go utility is installed
GO := $(shell command -v go 2> /dev/null)
export GOPATH=$(PWD)

# we're not going to generate our code during our automated builds since
# builds must be done based on files already checked-in into git

CICD := $(or ${SCREWDRIVER},${TRAVIS_PULL_REQUEST},${TRAVIS_TAG})

ifdef CICD

all:
	@echo "ci/cd pipeline - code generation skipped"

clean:

else

ifdef GO

all: build model.go client.go

clean:
	rm -rf model.go client.go zts_schema.go *~ ./src

else

all:
	@echo "go is not available please install golang"

clean:

endif

endif

build: src/$(RDL_LIB)

src/$(RDL_LIB):
	go get $(RDL_LIB)
	go get github.com/ardielle/ardielle-tools/...

model.go: $(RDL_FILE)
	$(GOPATH)/bin/rdl -ps generate -t -o $@ go-model $(RDL_FILE)

client.go: $(RDL_FILE)
	$(GOPATH)/bin/rdl -ps generate -t -o $@ athenz-go-client $(RDL_FILE)


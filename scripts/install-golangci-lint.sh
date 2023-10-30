#!/bin/bash
VERSION=$1
[ -z "${VERSION}" ] && VERSION="v1.55.0"
GOPATH=$(go env GOPATH)
[ -f "${GOPATH}/bin/golangci-lint-${VERSION}" ] && echo "golangci-lint ${VERSION} is already installed" || \
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/${VERSION}/install.sh | sh -s -- -b ${GOPATH}/bin ${VERSION} && \
cp ${GOPATH}/bin/golangci-lint ${GOPATH}/bin/golangci-lint-${VERSION}

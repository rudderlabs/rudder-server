
# syntax=docker/dockerfile:1

# GO_VERSION is updated automatically to match go.mod, see Makefile
ARG GO_VERSION=1.22.5
ARG ALPINE_VERSION=3.20
FROM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS builder
ARG VERSION
ARG REVISION
ARG COMMIT_HASH
ARG ENTERPRISE_TOKEN
ARG RACE_ENABLED=false
ARG CGO_ENABLED=0
ARG PKG_NAME=github.com/rudderlabs/release-demo

RUN apk add --update make tzdata ca-certificates

WORKDIR /rudder-server

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN BUILD_DATE=$(date "+%F,%T") \
    LDFLAGS="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT_HASH} -X main.buildDate=$BUILD_DATE -X main.builtBy=${REVISION} -X main.enterpriseToken=${ENTERPRISE_TOKEN} " \
    make build

RUN go build -o devtool ./cmd/devtool/
RUN go build -o rudder-cli ./cmd/rudder-cli/

FROM alpine:${ALPINE_VERSION}

RUN apk --no-cache upgrade && \
    apk --no-cache add tzdata ca-certificates postgresql-client curl bash

COPY --from=builder rudder-server/rudder-server .
COPY --from=builder rudder-server/build/wait-for-go/wait-for-go .
COPY --from=builder rudder-server/build/regulation-worker .
COPY --from=builder rudder-server/devtool .
COPY --from=builder rudder-server/rudder-cli /usr/bin/rudder-cli

RUN echo "-----BEGIN RSA PRIVATE KEY-----\nMIIEogIBAAKCAQEAjrEXeumwBtpNlca/oOYc6BSvThkOm7GpQJPX3Fqzlkyg/zfl\nKwNoCnd56pc/+PW6oHEyd5vyvhRhmCoOwQy/6LIz9c2PLSTVRAWlLvDkgh27EacG\n/dgvhnJNFe3saBj2BuL4DcyM3WqRP+pEdVagEuWNjDvYCv5OhLClJfgw2K3XXgPJ\nW5Pgo6HQaJ7UCmhKNOWN-SECRET-I9SfThkPp78HgxLeA/3Wb\nTY4FnVCObfhG+4PC5EG6XGjA2KQfnLslPlqk34KJMCcVW7elkVLrjFUoZo9aq2h7\nlCe7C4WKJYO7J5OFDZFBIwOLGaQBFm3HedeoYwIDAQABAoIBADtYihRzxuNCvgvy\nClcn/ZPErpiDsWCtxl/2XUvnaVO0cS1UmPqHwyi/QjSATXoE8WazTbMTaCUIIwL/\nUv2ViOeF3jh4EvlKnWxaELZyOAepx9jyp+TBmhqHxWm+d5lf8Esy8MoEbf3Uya5U\nh75plc/mKzXM2fDQjeV/9l/RVrd3C0dH+AZHX6dA+txfPUnJgsyEXw+31LqUz0Kf\nCs5OsgEEFoh+k7coipsOThjM5PcVo0fjNDsZAgQZWm8fy9WmNHDoTV1XPUwCn2gu\nKloPAy8VGNX2S/+BB81SzBMOXHHxQkmdDXbFfLRZuR4vBZLk8Nw0NBOyPhz4+14R\nEdWB02kCgYEA7HoxJtXsnCjO8g8hclLK7UEhifgt7OWjQ2wydggKfaQlXVRzy5BB\ngz4t0N6uHh85wy0S5jc/d1GnVw1FyTqmeLMIgVvs+FMopoR/8QLHktI+zKumGzkJ\nWLv3ZF9aGsXRECxhftuDPMJ4Zh6NjzcyeUzPrwsy7Vziug6XcY5yaE0CgYEAmnjM\nNTInbrgvhTckWROHCZvxEKpCraWFgMSh3lUOYwdwAO+SediuzwDbi9ZDmK7NbgzH\n7yMDGUm5Bw90HAF7SFdKw7eHkHn8tqvGh9Rcia2JsHxcEQttAOUmPjXCGE3JLJye\nWWnTHHMWFkTHa41mhhfF73OGjU79I7AQas1Vq28CgYAhOL1k/lLCUX1ZRkTlobn+\ngNLsZiBVkqFQHIguSszmf8P8C5xE8dwySu721AHUG4Dq6Y8pJoPVCHSg5y1xqrqu\nZECCo95zuIMSebTbsA9Hkh/ecxLf6jBk9es4f6jR8A6B3ipIPyB9zbhKaGpsv01o\n+yCGL+WUsSLtDpW6D1AFVQKBgFkmRDyjQGy+8fYBGAIsqe+axyqmCKts7rqdQQou\nronqnfJ8UV3u9xxS02JF+2cf40GTMMRLwZsTmCyB97G+DSd38Zc40Y1JSdcnFgII\nRovS0rc/Xmcb4AH3PfYDUoxyQBt2HFFgKM2vgdzCPxFMXxIeoEaMtufQ4Xl6QE9T\nSBETAoGAGpZ9Kz3UnDIpusUeWJLs8yZ7N+axb6POs/YXf0T1zyddN/Bzgk6m3E5b\npif66aozmBkaaqRXrNGu5Z6bKILW0bnZ9jNPcD2torMz2F1uOMwMZ1yXi11hNgez\nbAhupaZVm4P9enkOLOIvjijAFvkiIwj58cfTj4CCDX0LGMYzc5o=\n-----END RSA PRIVATE KEY-----\n","RawV2":"","Redacted":"-----BEGIN RSA PRIVATE KEY-----\nMIIEogIBAAKCAQEAjrEXeumwBtpNlca/" > key

COPY build/docker-entrypoint.sh /
COPY build/wait-for /
COPY scripts/generate-event /scripts/generate-event
COPY scripts/batch.json /scripts/batch.json

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["/rudder-server"]

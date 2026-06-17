# Makefile for luxlibertas

BINARY_NAME=luxlibertas
GIT_COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
GO_VERSION = $(shell go version | awk '{print $$3}')
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
PLATFORM = $(GOOS)/$(GOARCH)

LDFLAGS=-ldflags "-s -w \
  -X main.gitCommit=${GIT_COMMIT} \
  -X main.buildTime=${BUILD_TIME} \
  -X main.goVersion=${GO_VERSION} \
  -X main.platform=${PLATFORM}"

.PHONY: all build test clean

all: build

build:
	@echo "Building for ${PLATFORM} with Go:${GO_VERSION}..."
	CGO_ENABLED=0 GOOS=${GOOS} GOARCH=${GOARCH} go build ${LDFLAGS} -o ${BINARY_NAME}

test:
	go test -v ./...

clean:
	rm -f ${BINARY_NAME}

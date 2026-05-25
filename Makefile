.PHONY: all build daemon cli deploy-tool run install test clean

all: build

build: daemon cli deploy-tool

daemon:
	go build -o drings-daemon ./cmd/drings-daemon

cli:
	go build -o drings ./cmd/drings

deploy-tool:
	go build -o drings-deploy ./cmd/drings-deploy

run: build
	./drings-daemon --mount

install:
	go install ./cmd/drings ./cmd/drings-daemon ./cmd/drings-deploy
	cp $(shell go env GOPATH)/bin/drings $(shell go env GOPATH)/bin/drings-daemon $(shell go env GOPATH)/bin/drings-deploy . 2>/dev/null || true

test:
	go test -v $(if $(pkg),./$(pkg)/...,./...)

clean:
	rm -f drings drings-daemon drings-deploy

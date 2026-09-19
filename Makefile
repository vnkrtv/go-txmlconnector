VERSION ?= development
GOLANGCI ?= golangci-lint
GOLANGCI_VERSION := v2.5.0

.PHONY: test fmt lint lint-windows lint-install build native-test-build generate tools

test:
	go test -race ./...

fmt:
	$(GOLANGCI) fmt -c .golangci.yaml

lint:
	$(GOLANGCI) run -v -c .golangci.yaml --timeout=5m

# Include the native DLL adapter, which is excluded from Linux builds.
lint-windows:
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 $(GOLANGCI) run -v -c .golangci.yaml --timeout=5m

lint-install:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_VERSION)

build:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -trimpath -ldflags "-X main.version=$(VERSION)" -o bin/txmlconnector.exe ./cmd/txmlconnector

native-test-build:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go test -c -o bin/native.test.exe ./internal/native

generate:
	protoc --go_out=. --go_opt=module=go-txmlconnector --go-grpc_out=. --go-grpc_opt=module=go-txmlconnector api/connect.proto

tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

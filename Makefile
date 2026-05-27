.PHONY: build test test-race integration vet lint clean

build:
	go build -o bin/tgpipe ./cmd/tgpipe

test:
	go test ./...

test-race:
	go test -race -coverprofile=coverage.out ./...

integration:
	go test -race -timeout 60s ./tests/integration/...

vet:
	go vet ./...

lint: vet
	@command -v staticcheck >/dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed, skipping"

clean:
	rm -rf bin/ coverage.out tgpipe tgpipe.exe

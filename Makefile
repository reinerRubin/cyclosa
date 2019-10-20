build:
	@mkdir -p bin
	@go build -o bin/cyclosa cmd/cyclosa/cyclosa.go
.PHONY: build

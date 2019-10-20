PKGS = $(shell go list ./... | grep -v /test)

build:
	@mkdir -p bin
	@go build -o bin/cyclosa cmd/cyclosa/cyclosa.go
.PHONY: build

integrationtest:
	@docker build -t cyclosa-test -f integrationtest/Dockerfile .
	@docker run cyclosa-test
	@echo $?
.PHONY: integrationtest

lint:
	golint $(PKGS)
.PHONY: lint

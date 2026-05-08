# Murmur development targets.
#
# `make help` for the full list. Key entry points:
#   make test-unit         — fast Go unit tests, no infra
#   make test-integration  — Go tests against docker-compose stack
#   make web-build         — build the web UI into pkg/admin/dist (embedded)
#   make ui                — web-build + run cmd/murmur-ui --demo
#   make compose-up        — bring up the docker-compose stack
#   make lint              — go vet + golangci-lint + npm typecheck
#   make ci                — fmt-check, lint, build, test-unit (what CI runs)

.DEFAULT_GOAL := help

GO        ?= go
NPM       ?= npm
DOCKER    ?= docker compose
PROTOC    ?= protoc

UI_SRC    := $(shell find web/src web/index.html -type f 2>/dev/null)
DIST_DIR  := pkg/admin/dist
WEB_DIST  := web/dist

.PHONY: help
help:
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ----------------------------------------------------------------------------
# Go
# ----------------------------------------------------------------------------

# Submodules: pkg/exec/batch/sparkconnect carries its own go.mod so non-Spark
# consumers don't need to mirror the spark-connect-go fork-replace.
# `go vet ./...` / `go build ./...` only see the current module — we run them
# in each submodule too.
SUBMODULES := pkg/exec/batch/sparkconnect

.PHONY: build
build: $(DIST_DIR)/index.html ## Build all Go packages and binaries
	$(GO) build ./...
	@for d in $(SUBMODULES); do (cd $$d && $(GO) build ./...) || exit 1; done

.PHONY: vet
vet: ## go vet ./...
	$(GO) vet ./...
	@for d in $(SUBMODULES); do (cd $$d && $(GO) vet ./...) || exit 1; done

.PHONY: fmt
fmt: ## gofmt -w on the Go tree
	$(GO) fmt ./...

.PHONY: fmt-check
fmt-check: ## fail if any Go file isn't gofmt'd (CI gate, non-mutating)
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then echo "gofmt diff in:"; echo "$$out"; exit 1; fi

.PHONY: lint
lint: vet fmt-check ## go vet + gofmt check (golangci-lint via tool)
	@which golangci-lint >/dev/null 2>&1 && golangci-lint run ./... || \
		echo "golangci-lint not installed; run: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"

# go list ./... walks node_modules under web/ if it has any Go files (some npm
# packages ship Go example code). Filter explicitly to keep CI output clean.
GO_PACKAGES := $(shell $(GO) list ./... 2>/dev/null | grep -v '/web/node_modules/')

.PHONY: test-unit
test-unit: ## Fast unit tests — no docker, no AWS. Skips integration/e2e.
	$(GO) test -short -timeout 60s $(GO_PACKAGES)
	@for d in $(SUBMODULES); do (cd $$d && $(GO) test -short -timeout 60s ./...) || exit 1; done

.PHONY: test-integration
test-integration: compose-up ## Full test suite including E2E. Requires docker-compose stack.
	DDB_LOCAL_ENDPOINT=http://localhost:8000 \
	KAFKA_BROKERS=localhost:9092 \
	VALKEY_ADDRESS=localhost:6379 \
	S3_ENDPOINT=http://localhost:9000 \
	SPARK_CONNECT_REMOTE=sc://localhost:15002 \
	MONGO_URI="mongodb://localhost:27017/?replicaSet=rs0&directConnection=true" \
	$(GO) test -timeout 240s $(GO_PACKAGES)

# ----------------------------------------------------------------------------
# Web UI
# ----------------------------------------------------------------------------

.PHONY: web-deps
web-deps: web/node_modules ## Install npm deps for the UI

web/node_modules: web/package.json web/package-lock.json
	cd web && $(NPM) ci

.PHONY: web-build
web-build: $(DIST_DIR)/index.html ## Build the UI into pkg/admin/dist for embed.FS

# The dist gets copied into pkg/admin/dist where embed.FS reads it. Triggered
# whenever any UI source file changes.
$(DIST_DIR)/index.html: web/node_modules $(UI_SRC) web/vite.config.ts
	cd web && $(NPM) run build
	@mkdir -p $(DIST_DIR)
	@rm -rf $(DIST_DIR)/assets $(DIST_DIR)/index.html $(DIST_DIR)/favicon.svg $(DIST_DIR)/icons.svg
	cp -r $(WEB_DIST)/* $(DIST_DIR)/

.PHONY: web-typecheck
web-typecheck: web-deps ## tsc --noEmit
	cd web && $(NPM) run -s typecheck 2>/dev/null || cd web && npx tsc -b --noEmit

.PHONY: web-lint
web-lint: web-deps ## eslint
	cd web && $(NPM) run -s lint

.PHONY: ui
ui: web-build build ## Build everything and run cmd/murmur-ui in --demo mode
	$(GO) run ./cmd/murmur-ui --demo --addr :8080

# ----------------------------------------------------------------------------
# Proto
# ----------------------------------------------------------------------------

.PHONY: proto-tools
proto-tools: ## Install Go protobuf / gRPC / Connect plugins required by `make proto`
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	$(GO) install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest

.PHONY: proto
proto: ## Regenerate Go bindings (proto/gen/*) via `buf generate`
	@which buf >/dev/null 2>&1 || { echo "install buf: brew install bufbuild/buf/buf"; exit 1; }
	PATH="$$PATH:$$($(GO) env GOPATH)/bin" buf generate

.PHONY: web-proto
web-proto: web-deps ## Regenerate TypeScript bindings (web/src/gen/*) via npx buf
	cd web && npx buf generate

# ----------------------------------------------------------------------------
# Docker compose
# ----------------------------------------------------------------------------

.PHONY: compose-up
compose-up: ## Bring up the local docker-compose stack
	$(DOCKER) up -d kafka dynamodb-local valkey mongo minio spark-connect
	@./scripts/init-mongo-replset.sh 2>/dev/null || true

.PHONY: compose-down
compose-down: ## Stop the local docker-compose stack
	$(DOCKER) down

.PHONY: seed-ddb
seed-ddb: ## Create the page_views DDB table on dynamodb-local for the example
	aws --endpoint-url=http://localhost:8000 dynamodb create-table \
		--table-name page_views \
		--attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=N \
		--key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
		--billing-mode PAY_PER_REQUEST 2>/dev/null || echo "table page_views already exists"

# ----------------------------------------------------------------------------
# CI gate
# ----------------------------------------------------------------------------

.PHONY: ci
ci: fmt-check vet test-unit ## CI gate: format, vet, unit tests. Web typecheck via web-typecheck.

.PHONY: clean
clean: ## Remove built artifacts
	rm -rf $(WEB_DIST)
	rm -rf $(DIST_DIR)/assets $(DIST_DIR)/index.html $(DIST_DIR)/favicon.svg $(DIST_DIR)/icons.svg

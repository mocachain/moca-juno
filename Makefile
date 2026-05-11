VERSION := $(shell git describe --tags --always 2>/dev/null | sed 's/^v//')
COMMIT  := $(shell git log -1 --format='%H')
GO_TOOLCHAIN ?= go1.23.12
GO_BINARY ?= $(shell command -v go 2>/dev/null || echo go)
GO_LOCAL_ENV ?= env -u GOROOT GOTOOLCHAIN=$(GO_TOOLCHAIN)
GO := $(GO_LOCAL_ENV) $(GO_BINARY)
GO_GOPATH ?= $(shell $(GO) env GOPATH 2>/dev/null)
GO_BIN ?= $(or $(GOBIN),$(if $(GO_GOPATH),$(GO_GOPATH)/bin,$(HOME)/go/bin))
LEFTHOOK ?= $(GO_BIN)/lefthook
LEFTHOOK_VERSION ?= v1.11.3
GOLANGCI_LINT ?= $(GO_BIN)/golangci-lint
GOLANGCI_LINT_VERSION ?= v1.64.8
LINT_TIMEOUT ?= 10m

export GO111MODULE = on

###############################################################################
###                                   All                                   ###
###############################################################################

all: lint test-unit install

###############################################################################
###                                Build flags                              ###
###############################################################################

LD_FLAGS = -X github.com/forbole/juno/v4/cmd.Version=$(VERSION) \
	-X github.com/forbole/juno/v4/cmd.Commit=$(COMMIT)

BUILD_FLAGS := -ldflags '$(LD_FLAGS)'

###############################################################################
###                                  Build                                  ###
###############################################################################

build: go.sum
ifeq ($(OS),Windows_NT)
	@echo "building juno binary..."
	@$(GO) build -mod=readonly $(BUILD_FLAGS) -o build/juno.exe ./cmd/juno
else
	@echo "building juno binary..."
	@$(GO) build -mod=readonly $(BUILD_FLAGS) -o build/juno ./cmd/juno
endif
.PHONY: build

###############################################################################
###                                 Install                                 ###
###############################################################################

install: go.sum
	@echo "installing juno binary..."
	@$(GO) install -mod=readonly $(BUILD_FLAGS) ./cmd/juno
.PHONY: install

###############################################################################
###                          Tools & Dependencies                           ###
###############################################################################

go-mod-cache: go.sum
	@echo "--> Download go modules to local cache"
	@$(GO) mod download

go.sum: go.mod
	@echo "--> Ensure dependencies have not been modified"
	@$(GO) mod verify
	@$(GO) mod tidy

clean:
	rm -rf $(BUILDDIR)/

.PHONY: go-mod-cache go.sum clean

###############################################################################
###                           Tests & Simulation                            ###
###############################################################################

stop-docker-test:
	@echo "Stopping Docker container..."
	@docker stop bdjuno-test-db || true && docker rm bdjuno-test-db || true
.PHONY: stop-docker-test

start-docker-test: stop-docker-test
	@echo "Starting Docker container..."
	@docker run --name bdjuno-test-db -e POSTGRES_USER=bdjuno -e POSTGRES_PASSWORD=password -e POSTGRES_DB=bdjuno -d -p 6433:5432 postgres
.PHONY: start-docker-test

coverage:
	@echo "viewing test coverage..."
	@$(GO) tool cover --html=coverage.out
.PHONY: coverage

test-unit: start-docker-test
	@echo "Executing unit tests..."
	@$(GO) test -mod=readonly -v -coverprofile coverage.txt ./...
.PHONY: test-unit

###############################################################################
###                                Linting                                  ###
###############################################################################
check-go-env:
	@echo "--> Using Go binary: $(GO_BINARY)"
	@$(GO) version
	@echo "--> Repository toolchain: $(GO_TOOLCHAIN)"
	@echo "--> Ignoring external GOROOT for repository commands"
.PHONY: check-go-env

install-lint:
	@$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
.PHONY: install-lint

check-lint:
	@if [ ! -x "$(GOLANGCI_LINT)" ]; then \
		echo "golangci-lint not found at $(GOLANGCI_LINT)"; \
		echo "Run 'make install-lint' first."; \
		exit 1; \
	fi
	@echo "--> Using golangci-lint binary: $(GOLANGCI_LINT)"
	@$(GOLANGCI_LINT) version
.PHONY: check-lint

hooks:
	@if [ ! -x "$(LEFTHOOK)" ]; then \
		echo "--> Installing lefthook $(LEFTHOOK_VERSION) into $(GO_BIN)"; \
		$(GO) install github.com/evilmartians/lefthook@$(LEFTHOOK_VERSION); \
	else \
		echo "--> Using lefthook binary: $(LEFTHOOK)"; \
	fi
	@$(LEFTHOOK) install
.PHONY: hooks

lint: check-go-env check-lint
	@echo "--> Running linter"
	@$(GOLANGCI_LINT) run --timeout $(LINT_TIMEOUT)
.PHONY: lint

lint-fix: check-go-env check-lint
	@echo "--> Running linter"
	@$(GOLANGCI_LINT) run --fix --out-format=tab --issues-exit-code=0 --timeout $(LINT_TIMEOUT)
.PHONY: lint-fix

lint-changed: check-go-env check-lint
	@changed_go_files="$$( { git diff --name-only --diff-filter=ACMR HEAD; git ls-files --others --exclude-standard; } | grep '\.go$$' | sort -u || true )"; \
	if { git diff --name-only --diff-filter=ACMR HEAD; git ls-files --others --exclude-standard; } | grep -Eq '(^|/)(go\.mod|go\.sum)$$'; then \
		echo "--> go.mod/go.sum changed; running full golangci-lint..."; \
		$(GOLANGCI_LINT) run --timeout $(LINT_TIMEOUT); \
	elif [ -z "$$changed_go_files" ]; then \
		echo "--> No local changed Go files to lint"; \
	else \
		changed_dirs="$$(printf '%s\n' "$$changed_go_files" | xargs -n1 dirname | sed 's#^\.$$#./.#' | sed 's#^[^./]#./&#' | sort -u)"; \
		echo "--> Running golangci-lint on local changed Go packages..."; \
		$(GOLANGCI_LINT) run --timeout $(LINT_TIMEOUT) $$changed_dirs; \
	fi
.PHONY: lint-changed

lint-staged: check-go-env check-lint
	@staged_go_files="$$(git diff --cached --name-only --diff-filter=ACMR | grep '\.go$$' | sort -u || true)"; \
	if git diff --cached --name-only --diff-filter=ACMR | grep -Eq '(^|/)(go\.mod|go\.sum)$$'; then \
		echo "--> go.mod/go.sum changed; running full golangci-lint..."; \
		$(GOLANGCI_LINT) run --timeout $(LINT_TIMEOUT); \
	elif [ -z "$$staged_go_files" ]; then \
		echo "--> No staged Go files to lint"; \
	else \
		staged_dirs="$$(printf '%s\n' "$$staged_go_files" | xargs -n1 dirname | sed 's#^\.$$#./.#' | sed 's#^[^./]#./&#' | sort -u)"; \
		echo "--> Running golangci-lint on staged Go packages..."; \
		$(GOLANGCI_LINT) run --timeout $(LINT_TIMEOUT) $$staged_dirs; \
	fi
.PHONY: lint-staged

pre-commit: lint-changed
.PHONY: pre-commit

pre-commit-staged: lint-staged
.PHONY: pre-commit-staged

format:
	find . -name '*.go' -type f -not -path "./vendor*" -not -path "*.git*" -not -name '*.pb.go' -not -path "./venv" | xargs gofmt -w -s
	find . -name '*.go' -type f -not -path "./vendor*" -not -path "*.git*" -not -name '*.pb.go' -not -path "./venv" | xargs misspell -w
	find . -name '*.go' -type f -not -path "./vendor*" -not -path "*.git*" -not -name '*.pb.go' -not -path "./venv" | xargs goimports -w -local github.com/forbole/juno
.PHONY: format

.PHONY: format

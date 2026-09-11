SHELL := /bin/bash

PROJECT_NAME := $(shell if [ -f PROJECT ]; then sed -n '/^[[:space:]]*[^#\[[:space:]]/p' PROJECT | head -1 | tr -d '[:space:]'; else sed -n 's/^[[:space:]]*name[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' Cargo.toml | head -1; fi)
PROJECT_VERSION := $(shell if [ -f PROJECT ]; then sed -n '/^[[:space:]]*[^#\[[:space:]]/p' PROJECT | sed -n '2p' | tr -d '[:space:]'; else sed -n 's/^[[:space:]]*version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' Cargo.toml | head -1; fi)
ifeq ($(PROJECT_NAME),)
    $(error Error: PROJECT file not found or invalid)
endif

TOP_DIR := $(CURDIR)
CARGO := cargo
EXAMPLE ?= 01_single_machine
ARGS ?=
CAMERA_DEVICE ?= /dev/video0
SERVER ?=
PREFIX ?= $(HOME)/.local
AUDIT_DB ?= $(TOP_DIR)/target/advisory-db
AUDIT_IGNORES ?= --ignore RUSTSEC-2023-0071

HAS_REL := $(shell command -v git-rel 2>/dev/null)

$(info ------------------------------------------)
$(info Project: $(PROJECT_NAME) v$(PROJECT_VERSION))
$(info ------------------------------------------)

.PHONY: build b compile c run r camera-publisher camera-subscriber test t integration agent-test directory-test remote-test examples-smoke audit check check-all test-all clippy rustdoc fmt fmt-check lock clean verify release help h

build:
	@$(CARGO) build --lib

b: build

compile:
	@$(CARGO) clean
	@$(MAKE) build

c: compile

run:
	@$(CARGO) run --example $(EXAMPLE) -- $(ARGS)

r: run

camera-publisher:
	@$(CARGO) run --example 10_camera_publisher -- --device "$(CAMERA_DEVICE)" $(ARGS)

camera-subscriber:
	@if [ -z "$(SERVER)" ]; then \
		echo "SERVER is required (Endpoint ID or did:key)"; \
		exit 1; \
	fi
	@$(CARGO) run --example 11_camera_subscriber -- "$(SERVER)" $(ARGS)

test:
	@$(CARGO) test --all-targets -- --test-threads=1

t: test

integration:
	@$(CARGO) test --tests -- --test-threads=1

agent-test:
	@$(CARGO) test --test agent_local -- --test-threads=1

directory-test:
	@$(CARGO) test --test directory_reconciliation -- --test-threads=1

remote-test:
	@$(CARGO) test --test referral_remote -- --test-threads=1

examples-smoke:
	@$(CARGO) run --example 05_all_exchanges

audit:
	@$(CARGO) audit --db $(AUDIT_DB) $(AUDIT_IGNORES)

check:
	@$(CARGO) check --all-targets

check-all:
	@$(CARGO) check --all-targets --all-features

fmt:
	@$(CARGO) fmt --package $(PROJECT_NAME)

fmt-check:
	@$(CARGO) fmt --package $(PROJECT_NAME) -- --check

lock:
	@$(CARGO) generate-lockfile

clippy:
	@$(CARGO) clippy --all-targets --all-features -- -D warnings

rustdoc:
	@RUSTDOCFLAGS="-Dwarnings" $(CARGO) doc --all-features --no-deps

test-all:
	@$(CARGO) test --all-targets --all-features -- --test-threads=1

clean:
	@$(CARGO) clean

verify: fmt-check check test check-all test-all clippy rustdoc

release:
	@if [ -z "$(HAS_REL)" ]; then \
		echo "git-rel is not installed. Please install it first."; \
		exit 1; \
	fi
	@if [ -z "$(TYPE)" ]; then \
		echo "Release type not specified. Use 'make release TYPE=[patch|minor|major|M.m.p]'"; \
		exit 1; \
	fi
	@git rel $(TYPE)

help:
	@echo
	@echo "Usage: make [target]"
	@echo
	@echo "Available targets:"
	@echo "  build        Build the library"
	@echo "  compile      Clean and rebuild"
	@echo "  run          Run an example (EXAMPLE=name ARGS='...')"
	@echo "  camera-publisher Publish RGB frames from CAMERA_DEVICE"
	@echo "  camera-subscriber Subscribe using SERVER=<did:key>"
	@echo "  test         Run all tests"
	@echo "  integration  Run integration tests"
	@echo "  agent-test   Run local and forced-QUIC Agent tests"
	@echo "  directory-test Run reconciliation and lease tests"
	@echo "  remote-test  Run the forced-QUIC referral test"
	@echo "  examples-smoke Run the bounded exchange example"
	@echo "  audit        Scan dependencies for security advisories"
	@echo "  check        Run cargo check on all targets"
	@echo "  check-all    Run cargo check on all targets/all features"
	@echo "  test-all     Run cargo test on all targets/all features"
	@echo "  clippy       Run clippy with warnings denied"
	@echo "  rustdoc      Build docs with warnings denied"
	@echo "  fmt          Format the workspace"
	@echo "  fmt-check    Check formatting"
	@echo "  lock         Regenerate Cargo.lock"
	@echo "  clean        Remove Cargo build artifacts"
	@echo "  verify       Run the full local gate"
	@echo "  release      Release a new version"
	@echo

h: help

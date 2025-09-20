SHELL := /bin/bash

# Project settings
BINARY := mincache-cli
PKG := ./src
BIN_DIR := bin

.PHONY: all build run clean install install-user uninstall uninstall-user fmt test help

all: build

# Build the CLI binary into bin/
build:
	@mkdir -p $(BIN_DIR)
	@echo "Building $(BINARY) -> $(BIN_DIR)/$(BINARY)"
	@go build -o $(BIN_DIR)/$(BINARY) $(PKG)
	@echo "Done. Run with ./$(BIN_DIR)/$(BINARY) or use 'make install-user'"

# Run directly from sources (no binary)
run:
	@echo "Starting $(BINARY) (go run)"
	@go run $(PKG)

# Install to user-local bin (recommended): ~/.local/bin/mincache-cli
# Ensure ~/.local/bin is on your PATH to run 'mincache-cli' everywhere
install-user: build
	@mkdir -p $$HOME/.local/bin
	@install -m 0755 $(BIN_DIR)/$(BINARY) $$HOME/.local/bin/$(BINARY)
	@echo "Installed to $$HOME/.local/bin/$(BINARY). Ensure $$HOME/.local/bin is on PATH."

# Install system-wide (requires sudo): /usr/local/bin/mincache-cli
install: build
	@echo "Installing to /usr/local/bin (may require sudo)"
	@install -m 0755 $(BIN_DIR)/$(BINARY) /usr/local/bin/$(BINARY)
	@echo "Installed /usr/local/bin/$(BINARY)"

uninstall:
	@rm -f /usr/local/bin/$(BINARY)
	@echo "Removed /usr/local/bin/$(BINARY)"

uninstall-user:
	@rm -f $$HOME/.local/bin/$(BINARY)
	@echo "Removed $$HOME/.local/bin/$(BINARY)"

clean:
	@rm -rf $(BIN_DIR)
	@echo "Cleaned build artifacts"

fmt:
	@go fmt ./...

test:
	@go test ./...

help:
	@echo "Targets:"
	@echo "  build           Build binary to $(BIN_DIR)/$(BINARY)"
	@echo "  run             Run with 'go run' from $(PKG)"
	@echo "  install-user    Install to $$HOME/.local/bin (no sudo)"
	@echo "  install         Install to /usr/local/bin (sudo)"
	@echo "  uninstall       Remove from /usr/local/bin"
	@echo "  uninstall-user  Remove from $$HOME/.local/bin"
	@echo "  fmt             go fmt ./..."
	@echo "  test            go test ./..."
	@echo "  clean           Remove build artifacts"
	@echo "Usage: make install-user  # then run '$(BINARY)'"

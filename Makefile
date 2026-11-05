.PHONY: help install lint format format-check typecheck test check build clean

help:  ## Show this help
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

install:  ## Install dependencies (including dev group)
	uv sync

lint:  ## Run ruff linter
	uv run ruff check .

format:  ## Format code with ruff
	uv run ruff format .

format-check:  ## Check formatting without changing files
	uv run ruff format --check .

typecheck:  ## Run mypy
	uv run mypy

test:  ## Run the test suite
	uv run pytest

check: lint format-check typecheck test  ## Run every check (what CI runs)

build:  ## Build sdist and wheel into dist/
	uv build

clean:  ## Remove build and cache artifacts
	rm -rf dist .pytest_cache .mypy_cache .ruff_cache

.PHONY: help install lint format format-check typecheck test check build clean docs docs-serve

help:  ## Show this help
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

install:  ## Install dependencies (including dev group)
	uv sync --locked

lint:  ## Run ruff linter
	uv run ruff check .

format:  ## Format code with ruff
	uv run ruff format .

format-check:  ## Check formatting without changing files
	uv run ruff format --check .

typecheck:  ## Run mypy
	uv run mypy

test:  ## Run the test suite and report coverage
	uv run pytest

check: lint format-check typecheck test  ## Run every check (what CI runs)

build:  ## Build sdist and wheel into dist/
	uv build

docs:  ## Build the documentation site into site/ (strict)
	uv run --locked --group docs zensical build --clean --strict

docs-serve:  ## Serve the documentation site locally
	uv run --locked --group docs zensical serve

clean:  ## Remove build and cache artifacts
	rm -rf dist site .coverage .pytest_cache .mypy_cache .ruff_cache

# =============================================================================
# agora-etl — Makefile
# =============================================================================

# ─── Configuration ────────────────────────────────────────────────────────────
VENV   := .venv
PYTHON := $(VENV)/bin/python
PIP    := $(VENV)/bin/pip
RUFF   := $(VENV)/bin/ruff
PYTEST := $(VENV)/bin/pytest
MYPY   := $(VENV)/bin/mypy
MKDOCS := $(VENV)/bin/mkdocs

.PHONY: help setup install lint format fix check docs-check test test-core test-all contracts-runtime contracts-plugin contracts-baseline contracts preservation ci clean

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*##"}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ─── Setup ────────────────────────────────────────────────────────────────────
setup:  ## Create .venv and install all dependencies (first-time setup)
	python3.11 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) cache purge
	$(PIP) install -e ".[file,dev,benchmark]"

install:  ## Install/sync dependencies into existing .venv
	$(PIP) install -e ".[file,dev]"

# ─── Code Quality ─────────────────────────────────────────────────────────────
lint:  ## Lint code (ruff check)
	$(RUFF) check .

format:  ## Format code (ruff format)
	$(RUFF) format .

fix:  ## Auto-fix lint + format issues
	$(RUFF) check --fix .
	$(RUFF) format .

check:  ## Lint + format check (no auto-fix, for CI)
	$(RUFF) check .
	$(RUFF) format --check .

docs-check:  ## Build docs in strict mode
	$(MKDOCS) build --strict

# ─── Testing ──────────────────────────────────────────────────────────────────
test:  ## Run all tests (excluding integration)
	PYTHONPATH=src $(PYTEST) tests/ --ignore=tests/integration -v

test-core:  ## Run non-integration tests excluding preservation contract suites
	PYTHONPATH=src $(PYTEST) tests/ --ignore=tests/integration --ignore=tests/preservation -v

contracts-runtime:  ## Run public runtime + recovery contract suites
	PYTHONPATH=src $(PYTEST) tests/preservation/test_runtime_guarantees.py tests/preservation/test_recovery_edge_cases.py -v

contracts-plugin:  ## Run public plugin contract suite
	PYTHONPATH=src $(PYTEST) tests/preservation/test_plugin_contract.py -v

contracts-baseline:  ## Run baseline preservation suite
	PYTHONPATH=src $(PYTEST) tests/preservation/test_baseline_behaviors.py -v

contracts:  ## Run all documented public contract suites
	$(MAKE) contracts-runtime
	$(MAKE) contracts-plugin

preservation:  ## Run all preservation suites, including baseline compatibility checks
	$(MAKE) contracts
	$(MAKE) contracts-baseline

test-all:  ## Run all tests including integration (requires external services)
	PYTHONPATH=src $(PYTEST) tests/ -v

ci:  ## Full CI check: lint + format + type check + tests (mirrors GitHub Actions)
	$(MAKE) check
	$(MYPY) src/agora
	$(MAKE) contracts
	$(MAKE) contracts-baseline
	$(MAKE) docs-check
	$(MAKE) test-core

# ─── Cleanup ──────────────────────────────────────────────────────────────────
clean:  ## Remove build artifacts, caches, and generated files
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "dist"  -not -path "*/.venv/*" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "build" -not -path "*/.venv/*" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name ".coverage"     -delete 2>/dev/null || true
	find . -type d -name "htmlcov"       -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache"  -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache"  -exec rm -rf {} + 2>/dev/null || true

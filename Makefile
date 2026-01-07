.PHONY: help install install-dev test lint format run validate clean

PYTHON := python3
VENV := venv
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
PYTHON_VENV := $(VENV)/bin/python

# Windows compatibility
ifeq ($(OS),Windows_NT)
	VENV := venv
	PIP := $(VENV)/Scripts/pip
	PYTEST := $(VENV)/Scripts/pytest
	PYTHON_VENV := $(VENV)/Scripts/python
endif

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

venv:  ## Create virtual environment
	$(PYTHON) -m venv $(VENV)

install: venv  ## Install production dependencies
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

install-dev: install  ## Install development dependencies
	$(PIP) install -r requirements-dev.txt

test:  ## Run tests
	$(PYTEST) tests/ -v

test-cov:  ## Run tests with coverage
	$(PYTEST) tests/ -v --cov=src/etl --cov-report=term-missing

lint:  ## Run linters (ruff)
	$(VENV)/bin/ruff check src/ tests/

format:  ## Format code (ruff)
	$(VENV)/bin/ruff format src/ tests/
	$(VENV)/bin/ruff check --fix src/ tests/

run:  ## Run ETL pipeline
	$(PYTHON_VENV) -m etl run

run-verbose:  ## Run ETL pipeline with verbose output
	$(PYTHON_VENV) -m etl run --verbose

run-truncate:  ## Run ETL pipeline with table truncation
	$(PYTHON_VENV) -m etl run --truncate

validate:  ## Validate configuration
	$(PYTHON_VENV) -m etl validate

clean:  ## Clean up generated files
	rm -rf __pycache__ .pytest_cache .ruff_cache
	rm -rf src/__pycache__ src/etl/__pycache__
	rm -rf tests/__pycache__
	rm -rf .coverage htmlcov
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete

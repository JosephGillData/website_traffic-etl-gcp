.PHONY: help install install-dev test test-debug test-cov lint format run validate clean

PYTHON := python3
VENV := venv
PYTHON_VENV := $(VENV)/bin/python

# Windows compatibility - use python -m to avoid bin vs Scripts issues
ifeq ($(OS),Windows_NT)
	PYTHON_VENV := $(VENV)/Scripts/python
endif

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

venv:  ## Create virtual environment
	$(PYTHON) -m venv $(VENV)

install: venv  ## Install production dependencies
	$(PYTHON_VENV) -m pip install --upgrade pip
	$(PYTHON_VENV) -m pip install -r requirements.txt
	$(PYTHON_VENV) -m pip install -e .

install-dev: install  ## Install development dependencies
	$(PYTHON_VENV) -m pip install -r requirements-dev.txt

test:  ## Run tests
	$(PYTHON_VENV) -m pytest tests/ -v

test-debug:  ## Run tests with stdout visible (useful for debugging)
	$(PYTHON_VENV) -m pytest tests/ -v -s

test-cov:  ## Run tests with coverage
	$(PYTHON_VENV) -m pytest tests/ -v --cov=src/etl --cov-report=term-missing

lint:  ## Lint code (ruff check)
	$(PYTHON_VENV) -m ruff check src/ tests/

format:  ## Format code (ruff)
	$(PYTHON_VENV) -m ruff format src/ tests/
	$(PYTHON_VENV) -m ruff check --fix src/ tests/

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

.PHONY: build clean clean-build clean-pyc clean-test coverage docs format help lint lint/ruff lint/format lint/mypy servedocs test
.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python3 -c "$$BROWSER_PYSCRIPT"

help:
	@python3 -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

lint/ruff: ## check style with ruff
	uv run ruff check datamasque tests
lint/format: ## check formatting with ruff
	uv run ruff format --check datamasque tests
lint/mypy: ## check types with mypy
	uv run mypy datamasque

lint: lint/ruff lint/format lint/mypy ## check style, formatting, and types

format: ## autoformat with ruff
	uv run ruff format datamasque tests

test: ## run tests quickly with the default Python
	uv run pytest

build: ## build sdist and wheel into dist/
	uv build

coverage: ## check code coverage quickly with the default Python
	uv run pytest --cov=datamasque
	uv run coverage report -m
	uv run coverage html
	$(BROWSER) htmlcov/index.html

docs: ## generate Sphinx HTML documentation, including API docs
	rm -f docs/client.rst
	rm -f docs/modules.rst
	uv run sphinx-apidoc -o docs/ datamasque
	$(MAKE) -C docs clean
	uv run $(MAKE) -C docs html
	$(BROWSER) docs/_build/html/index.html

servedocs: docs ## compile the docs watching for changes
	uv run watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .

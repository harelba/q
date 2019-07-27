SHELL := /bin/bash

PROJECT_NAME=$(shell dirname "$0")
ROOT_DIR:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

.PHONY: test help
.DEFAULT_GOAL := ci

ci: lint test ## Equivelant to 'make lint test'

help: ## Show this help message.

	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

dep: ## Install the dependent libraries.

	pip install -r test-requirements.txt
	pip install -e .

lint: dep ## Run lint validations.

	flake8 q/ --count --select=E901,E999,F821,F822,F823 --show-source --statistics

test: dep ## Run the unit tests.

	py.test -rs -c pytest.ini -s -v q/tests/suite.py --rootdir .

release: ## Run release
	pip install py-ci==0.7.3
	pyci release --no-wheel-publish --wheel-universal

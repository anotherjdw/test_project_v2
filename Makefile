.PHONY: install test test-unit test-integration test-e2e lint format typecheck check fmt plan package clean

install:
	pip install -e ".[dev]"

test:
	pytest

test-unit:
	pytest -m unit

test-integration:
	pytest -m integration

test-e2e:
	pytest -m end_to_end

lint:
	ruff check src/

format:
	ruff format src/

typecheck:
	mypy src/

check: lint typecheck test

fmt:
	terraform fmt -recursive infra/terraform/

ENV ?= dev

# Validates configuration only (-backend=false, no remote state) -- this cannot
# show a true diff against real infrastructure. The authoritative plan is the
# one CI posts as a PR comment on a pull request into develop or main. If the
# two disagree, trust CI's, not this one.
plan:
	terraform -chdir=infra/terraform init -backend=false
	terraform -chdir=infra/terraform plan -var-file=$(ENV).tfvars

package:
	python -m build --wheel

clean:
	rm -rf __pycache__ .pytest_cache .mypy_cache .ruff_cache dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} +

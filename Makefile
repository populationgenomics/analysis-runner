.PHONY: compile-requirements lint install-dev

compile-requirements:
	docker run --platform linux/amd64 -v $$(pwd):/opt/deps python:3.10 /bin/bash -c '\
		cd /opt/deps; \
		pip install pip-tools; \
		pip-compile --extra dev --unsafe-package analysis-runner --unsafe-package setuptools --output-file requirements-dev.txt pyproject.toml;\
	'
# Deployment lockfiles from the pyproject.toml dependency groups. Set
# UPGRADE=--upgrade for a full pin refresh; without it uv keeps existing
# pins that still satisfy the group constraints.
	uv pip compile --group web --universal --python-version 3.10 $(UPGRADE) -o web/requirements.txt
	uv pip compile --group server --universal --python-version 3.10 $(UPGRADE) -o server/requirements.txt
	uv pip compile --group metamist --universal --python-version 3.11 $(UPGRADE) -o metamist/requirements.txt


lint:
	ruff check
	ruff format


install-dev:
	pip install --no-deps -r requirements-dev.txt

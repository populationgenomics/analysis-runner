.PHONY: compile-requirements lint install-dev

compile-requirements:
	docker run --platform linux/amd64 -v $$(pwd):/opt/deps python:3.10 /bin/bash -c '\
		cd /opt/deps; \
		pip install pip-tools; \
		pip-compile --extra dev --output-file requirements-dev.txt pyproject.toml;\
		pip-compile web/requirements.in;\
	'


lint:
	ruff check
	ruff format


install-dev:
	pip install --no-deps -r requirements-dev.txt

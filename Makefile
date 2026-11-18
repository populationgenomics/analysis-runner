.PHONY: lock install-dev lint test

# Re-resolve every package lockfile. Each package is an independent uv project,
# so lock them individually.
lock:
	uv lock
	uv lock --project packages/analysis-runner
	uv lock --project packages/server
	uv lock --project packages/web
	uv lock --project infrastructure

# Install the repo-wide dev tooling (ruff, pre-commit).
install-dev:
	uv sync

lint:
	uv run ruff check
	uv run ruff format

test:
	cd packages/analysis-runner && uv sync && uv run pytest -v

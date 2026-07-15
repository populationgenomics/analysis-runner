.PHONY: lock install-dev lint

# Re-resolve every package lockfile. Each package is an independent uv project,
# so lock them individually; the metamist Cloud Function additionally needs a
# requirements.txt exported from its lock (Cloud Functions build from pip).
lock:
	uv lock
	uv lock --project packages/analysis-runner
	uv lock --project packages/server
	uv lock --project packages/web
	uv lock --project packages/metamist-consumer
	uv export --project packages/metamist-consumer --no-dev --no-hashes --no-emit-project \
		-o packages/metamist-consumer/requirements.txt


# Install the repo-wide dev tooling (ruff, pre-commit, pylint).
install-dev:
	uv sync


lint:
	uv run ruff check
	uv run ruff format

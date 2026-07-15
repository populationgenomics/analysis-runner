FROM python:3.10-slim

ENV PYTHONUNBUFFERED True
ENV PORT 8080

WORKDIR /opt/app

# Bring in the uv binary to install dependencies.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install the fully-resolved dependencies from the lockfile into the system Python.
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-dev --no-emit-project --no-hashes -o requirements.txt && \
    uv pip install --system --no-cache -r requirements.txt

COPY main.py ./

# we use an environment varaible to set the port,
# so we can't use the JSON notation for the CMD
# hadolint ignore=DL3025
CMD exec gunicorn --bind :$PORT --workers 4 --threads 8 --timeout 0 main:app

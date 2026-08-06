# Since the server relies on Hail as well, we're reusing the driver image.
ARG DRIVER_IMAGE
FROM ${DRIVER_IMAGE}

# Allow statements and log messages to immediately appear in the Knative logs.
ENV PYTHONUNBUFFERED 1

ENV PORT 8080
EXPOSE $PORT

# Bring in the uv binary to install dependencies.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install the server's extra dependencies ON TOP OF the driver base image (which
# already provides hail, cpg-utils, aiohttp, nest_asyncio and analysis-runner).
# `uv pip install --system` layers onto the base image's Python without creating a
# venv and without removing the base image's packages.
COPY pyproject.toml ./
RUN uv pip install --system --no-cache -r pyproject.toml

COPY main.py ar.py config.py cromwell.py util.py ./

# Prepare the Hail deploy config to point to the CPG domain.
COPY deploy-config.json /deploy-config/deploy-config.json

CMD gunicorn --bind :$PORT --worker-class aiohttp.GunicornWebWorker main:init_func

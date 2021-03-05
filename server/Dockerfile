# Since the server relies on Hail as well, we're reusing the driver image.
ARG DRIVER_IMAGE
FROM ${DRIVER_IMAGE}

RUN conda install -c conda-forge\
    google-api-python-client=1.12.8\
    google-auth==1.24.0\
    google-cloud-secret-manager==2.2.0\
    google-cloud-pubsub==2.3.0\
    gunicorn &&\
    rm -r /root/miniconda3/pkgs

# Allow statements and log messages to immediately appear in the Knative logs.
ENV PYTHONUNBUFFERED 1

ENV PORT 8080
EXPOSE $PORT

COPY main.py cloud_identity.py ./

# Prepare the Hail deploy config to point to the CPG domain.
COPY deploy-config.json /deploy-config/deploy-config.json

CMD gunicorn --bind :$PORT --worker-class aiohttp.GunicornWebWorker main:init_func

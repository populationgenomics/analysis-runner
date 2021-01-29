# Since the server relies on Hail as well, we're reusing the driver image.
FROM 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:06dd37129625'

RUN conda install -c conda-forge google-auth google-cloud-secret-manager gunicorn

# Allow statements and log messages to immediately appear in the Knative logs.
ENV PYTHONUNBUFFERED 1

ENV PORT 8080
EXPOSE $PORT

COPY main.py .

CMD gunicorn --bind :$PORT --worker-class aiohttp.GunicornWebWorker main:init_func
# Since the server relies on Hail as well, we're reusing the driver image.
FROM 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:f2fc08ff8c5e'

RUN conda install -c conda-forge google-auth==1.24.0 google-cloud-secret-manager==2.2.0 google-cloud-pubsub==2.3.0 gunicorn

# Allow statements and log messages to immediately appear in the Knative logs.
ENV PYTHONUNBUFFERED 1

ENV PORT 8080
EXPOSE $PORT

COPY main.py .

CMD gunicorn --bind :$PORT --worker-class aiohttp.GunicornWebWorker main:init_func

# Since the server relies on Hail as well, we're reusing the driver image.
FROM 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:50ac35f838ae'

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

CMD gunicorn --bind :$PORT --worker-class aiohttp.GunicornWebWorker main:init_func

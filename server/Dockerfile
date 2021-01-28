# Since the server relies on Hail as well, we're reusing the driver image.
FROM 'australia-southeast1-docker.pkg.dev/analysis-runner/images/driver:latest'

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN rm requirements.txt

# Allow statements and log messages to immediately appear in the Knative logs.
ENV PYTHONUNBUFFERED 1

ENV PORT 8080
EXPOSE $PORT

COPY main.py .

CMD gunicorn --bind :$PORT --worker-class aiohttp.GunicornWebWorker main:init_func
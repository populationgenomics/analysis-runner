FROM python:3.10-slim

ENV PYTHONUNBUFFERED True
ENV PORT 8080

WORKDIR /opt/app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir --no-deps -r requirements.txt

COPY main.py ./

# we use an environment varaible to set the port,
# so we can't use the JSON notation for the CMD
# hadolint ignore=DL3025
CMD exec gunicorn --bind :$PORT --workers 4 --threads 8 --timeout 0 main:app

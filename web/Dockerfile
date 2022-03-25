FROM python:3.10-slim

ENV PYTHONUNBUFFERED True

RUN pip3 install  \
    cpg-utils==2.5.1 \
    cryptography \
    flask \
    gunicorn \
    google-api-python-client==2.10.0 \
    google-cloud-storage==1.38.0 \
    google-cloud-secret-manager==2.2.0

COPY main.py ./

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app

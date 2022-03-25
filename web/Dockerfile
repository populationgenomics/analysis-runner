FROM python:3.10-slim

ENV PYTHONUNBUFFERED True

RUN pip3 install  \
    #cpg-utils==2.5.1 \
    hail==0.2.91 \
    google-auth==2.6.2 \
    cryptography \
    flask \
    gunicorn \
    google-api-python-client==2.42.0 \
    google-cloud-storage==2.2.1 \
    google-cloud-secret-manager==2.9.2

COPY main.py ./

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app

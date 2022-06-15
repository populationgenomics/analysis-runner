FROM python:3.10-slim

ENV PYTHONUNBUFFERED True

RUN pip3 install  \
    cpg-utils==4.3.6 \
    cryptography==3.3.2 \
    flask==2.1.2 \
    gunicorn==20.1.0 \
    google-api-python-client==2.51.0 \
    google-cloud-storage==2.4.0 \
    google-cloud-secret-manager==2.11.1

COPY main.py ./

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app

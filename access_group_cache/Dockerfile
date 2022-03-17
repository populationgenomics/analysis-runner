FROM python:3.10-slim

ENV PYTHONUNBUFFERED True

RUN pip3 install  \
    cpg-utils==2.5.1  \
    aiohttp \
    flask \
    gunicorn

COPY main.py ./

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app

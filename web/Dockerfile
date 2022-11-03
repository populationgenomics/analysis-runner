FROM python:3.10-slim

ENV PYTHONUNBUFFERED True

COPY requirements.txt ./
RUN pip3 install -r requirements.txt

COPY main.py ./

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app

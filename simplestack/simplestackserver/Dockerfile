FROM python:3.8

ENV PORT 8000
ENV PYTHONUNBUFFERED 1
ENV GCP_PATH "gs://cpg-na12878/file.txt"
ENV AZ_PATH "az://<container>/cpg-na12878/file.txt"

EXPOSE $PORT

WORKDIR /app/
RUN pip install \
    fastapi==0.70.0 \
    google-auth==1.35.0 \
    uvicorn==0.13.4 \
    cryptography==36.0.1

COPY main.py main.py

CMD uvicorn --port ${PORT} --host 0.0.0.0 main:app

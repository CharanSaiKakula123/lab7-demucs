FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    flask \
    gunicorn \
    redis \
    minio

COPY rest-server.py .

EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "--timeout", "120", "rest-server:app"]

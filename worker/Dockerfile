FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    numpy \
    demucs \
    redis \
    minio \
    requests

COPY worker-server.py .

CMD ["python", "-u", "worker-server.py"]

import os
import json
import time
import tempfile
import logging
import subprocess
from pathlib import Path

import redis
import requests
from minio import Minio

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

MINIO_HOST = os.getenv("MINIO_HOST", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "rootuser")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "rootpass123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

QUEUE_BUCKET = os.getenv("QUEUE_BUCKET", "queue")
OUTPUT_BUCKET = os.getenv("OUTPUT_BUCKET", "output")
WORK_QUEUE = "toWorker"
LOG_QUEUE = "logging"

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    decode_responses=True
)

minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)


def log_info(message):
    logging.info(message)
    try:
        redis_client.lpush(LOG_QUEUE, f"worker.info: {message}")
    except redis.RedisError:
        logging.warning("Could not send message to Redis logging queue")


def ensure_buckets():
    for bucket in (QUEUE_BUCKET, OUTPUT_BUCKET):
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)


def process_job(job):
    songhash = job["hash"]
    model = job.get("model", "htdemucs")
    callback = job.get("callback")

    log_info(f"Starting separation for {songhash} using {model}")

    with tempfile.TemporaryDirectory() as temp_dir:
        work_dir = Path(temp_dir)
        input_dir = work_dir / "input"
        output_dir = work_dir / "output"

        input_dir.mkdir()
        output_dir.mkdir()

        input_file = input_dir / f"{songhash}.mp3"

        # Download the original audio from MinIO.
        response = minio_client.get_object(
            QUEUE_BUCKET,
            f"{songhash}.mp3"
        )

        try:
            with open(input_file, "wb") as file:
                for chunk in response.stream(1024 * 1024):
                    file.write(chunk)
        finally:
            response.close()
            response.release_conn()

        # Run Demucs in CPU mode.
        command = [
            "python", "-m", "demucs.separate",
            "--mp3",
            "-d", "cpu",
            "-n", model,
            "--out", str(output_dir),
            str(input_file)
        ]

        log_info(f"Running Demucs for {songhash}")

        result = subprocess.run(
            command,
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"Demucs failed: {result.stderr[-3000:]}"
            )

        # Demucs writes tracks under output/<model>/<song-name>/.
        separated_dir = output_dir / model / songhash

        tracks = ("bass", "drums", "vocals", "other")

        for track in tracks:
            track_file = separated_dir / f"{track}.mp3"

            if not track_file.exists():
                raise FileNotFoundError(
                    f"Demucs output not found: {track_file}"
                )

            # Store each track using the course's object naming convention.
            object_name = f"{songhash}-{track}.mp3"

            minio_client.fput_object(
                OUTPUT_BUCKET,
                object_name,
                str(track_file),
                content_type="audio/mpeg"
            )

            log_info(f"Uploaded {object_name}")

    log_info(f"Completed separation for {songhash}")

    # Notify the optional callback. Callback failures do not undo the job.
    if callback and callback.get("url"):
        try:
            callback_data = callback.get("data", {})
            if not isinstance(callback_data, dict):
                callback_data = {"data": callback_data}

            callback_data.update({
                "hash": songhash,
                "reason": "Song separation completed"
            })

            requests.post(
                callback["url"],
                json=callback_data,
                timeout=15
            )

            log_info(f"Callback sent for {songhash}")

        except Exception as error:
            log_info(f"Callback failed for {songhash}: {error}")


def main():
    log_info("Worker starting")

    while True:
        try:
            # Wait until a job is available.
            item = redis_client.blpop(WORK_QUEUE, timeout=30)

            if item is None:
                continue

            _, raw_job = item
            job = json.loads(raw_job)

            process_job(job)

        except Exception as error:
            logging.exception("Worker encountered an error")
            log_info(f"Job failed: {error}")
            time.sleep(2)


if __name__ == "__main__":
    ensure_buckets()
    main()

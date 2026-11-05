import os
import json
import base64
import hashlib
import logging
from io import BytesIO

import redis
from flask import Flask, request, jsonify, send_file
from minio import Minio
from minio.error import S3Error

app = Flask(__name__)

# Configuration
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

MAX_UPLOAD_SIZE = 16 * 1024 * 1024
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_SIZE

# Connections
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
        redis_client.lpush(LOG_QUEUE, f"rest.info: {message}")
    except redis.RedisError:
        logging.warning("Could not send message to Redis logging queue")


def ensure_buckets():
    for bucket in (QUEUE_BUCKET, OUTPUT_BUCKET):
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
            log_info(f"Created MinIO bucket: {bucket}")


def get_track_object(songhash, track):
    """Translate public API track names into MinIO object names."""
    track = track.lower().removesuffix(".mp3")

    if track in ("base", "bass"):
        track = "bass"
    elif track not in ("vocals", "drums", "other"):
        return None, None

    return OUTPUT_BUCKET, f"{songhash}-{track}.mp3"


@app.route("/", methods=["GET"])
def health():
    return "<h1>Music Separation Server</h1><p>Use a valid endpoint</p>", 200


@app.route("/apiv1/separate", methods=["POST"])
def separate():
    try:
        data = request.get_json(force=True)

        if not isinstance(data, dict):
            return jsonify({"error": "Request must contain a JSON object"}), 400

        encoded_mp3 = data.get("mp3")
        if not encoded_mp3:
            return jsonify({"error": "Missing mp3 field"}), 400

        try:
            mp3_bytes = base64.b64decode(encoded_mp3, validate=True)
        except Exception:
            return jsonify({"error": "Invalid Base64 MP3 data"}), 400

        if not mp3_bytes:
            return jsonify({"error": "MP3 file is empty"}), 400

        if len(mp3_bytes) > MAX_UPLOAD_SIZE:
            return jsonify({"error": "MP3 exceeds 16 MB limit"}), 413

        songhash = hashlib.sha224(mp3_bytes).hexdigest()

        model = data.get("model") or "htdemucs"
        callback = data.get("callback")

        ensure_buckets()

        # Store the original MP3 in MinIO.
        minio_client.put_object(
            QUEUE_BUCKET,
            f"{songhash}.mp3",
            BytesIO(mp3_bytes),
            length=len(mp3_bytes),
            content_type="audio/mpeg"
        )

        # Send a small job description through Redis.
        job = {
            "hash": songhash,
            "model": model,
            "callback": callback
        }

        redis_client.lpush(WORK_QUEUE, json.dumps(job))

        log_info(f"Enqueued song {songhash} using model {model}")

        return jsonify({
            "hash": songhash,
            "reason": "Song enqueued for separation"
        }), 200

    except redis.RedisError as error:
        app.logger.exception("Redis error")
        return jsonify({"error": f"Redis error: {str(error)}"}), 503

    except Exception as error:
        app.logger.exception("Error processing separation request")
        return jsonify({"error": str(error)}), 500


@app.route("/apiv1/queue", methods=["GET"])
@app.route("/apiv1/queue/", methods=["GET"])
def queue():
    try:
        jobs = redis_client.lrange(WORK_QUEUE, 0, -1)
        hashes = []

        for job in jobs:
            try:
                hashes.append(json.loads(job).get("hash", job))
            except (ValueError, TypeError):
                hashes.append(job)

        return jsonify({"queue": hashes}), 200

    except redis.RedisError as error:
        return jsonify({"error": str(error)}), 503


@app.route("/apiv1/track/<songhash>/<track>", methods=["GET"])
def get_track(songhash, track):
    bucket, object_name = get_track_object(songhash, track)

    if bucket is None:
        return jsonify({"error": "Unsupported track"}), 400

    try:
        response = minio_client.get_object(bucket, object_name)

        try:
            audio = response.read()
        finally:
            response.close()
            response.release_conn()

        return send_file(
            BytesIO(audio),
            mimetype="audio/mpeg",
            as_attachment=True,
            download_name=track
        )

    except S3Error as error:
        if error.code in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
            return jsonify({"error": "Track not found"}), 404

        app.logger.exception("MinIO error")
        return jsonify({"error": str(error)}), 500

    except Exception as error:
        app.logger.exception("Track retrieval error")
        return jsonify({"error": str(error)}), 500


@app.route("/apiv1/remove/<songhash>/<track>", methods=["GET", "DELETE"])
def remove_track(songhash, track):
    bucket, object_name = get_track_object(songhash, track)

    if bucket is None:
        return jsonify({"error": "Unsupported track"}), 400

    try:
        minio_client.remove_object(bucket, object_name)
        log_info(f"Removed {object_name} from {bucket}")

        return jsonify({
            "hash": songhash,
            "track": track,
            "reason": "Track removed"
        }), 200

    except S3Error as error:
        app.logger.exception("MinIO removal error")
        return jsonify({"error": str(error)}), 500

    except Exception as error:
        app.logger.exception("Removal error")
        return jsonify({"error": str(error)}), 500


@app.errorhandler(413)
def request_too_large(error):
    return jsonify({"error": "Request exceeds 16 MB limit"}), 413


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)

import os
import json
import time
import hashlib
import subprocess
import sys
import requests

import redis
from minio import Minio

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
MINIO_HOST = os.getenv("MINIO_HOST", "localhost:9000")
MINIO_USER = os.getenv("MINIO_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_PASSWORD", "minioadmin123")

QUEUE_NAME = "toWorkers"
LOG_QUEUE = "logging"

r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

minio = Minio(
    MINIO_HOST,
    access_key=MINIO_USER,
    secret_key=MINIO_PASSWORD,
    secure=False
)

INPUT_BUCKET = "queue"
OUTPUT_BUCKET = "output"

os.makedirs("/tmp/worker-input", exist_ok=True)
os.makedirs("/tmp/worker-output", exist_ok=True)


def log_info(message):
    print(message, flush=True)
    r.lpush(LOG_QUEUE, json.dumps({
        "level": "INFO",
        "message": message
    }))


def process_job(job):
    songhash = job["songhash"]
    model = job.get("model", "mdx_extra_q")

    log_info("Starting job " + songhash)

    input_file = f"/tmp/worker-input/{songhash}.mp3"
    output_dir = f"/tmp/worker-output/{songhash}"

    os.makedirs(output_dir, exist_ok=True)

    # Download original MP3 from MinIO
    minio.fget_object(
        job.get("bucket", INPUT_BUCKET),
        job.get("object", songhash + ".mp3"),
        input_file
    )

    # Run Demucs
    command = [
        sys.executable, "-m", "demucs.separate",
        "-n", model,
        "--mp3",
        "-d", "cpu",
        "-o", output_dir,
        input_file
    ]

    result = subprocess.run(command)

    if result.returncode != 0:
        raise Exception("Demucs failed")

    # Upload separated tracks
    for track in ["bass", "drums", "vocals", "other"]:
        track_file = f"{output_dir}/{model}/{songhash}/{track}.mp3"
        object_name = f"{songhash}-{track}.mp3"

        minio.fput_object(
            OUTPUT_BUCKET,
            object_name,
            track_file,
            content_type="audio/mpeg"
        )

        log_info("Uploaded " + object_name)

    # Send callback if one was supplied
    callback = job.get("callback")

    if callback:
        try:
            requests.post(
                callback["url"],
                json=callback.get("data", {}),
                timeout=10
            )
        except Exception as e:
            log_info("Callback failed: " + str(e))

    log_info("Finished job " + songhash)


def main():
    log_info("Worker started")

    while True:
        item = r.blpop(QUEUE_NAME, timeout=5)

        if item is None:
            continue

        _, message = item

        try:
            job = json.loads(message)
            process_job(job)
        except Exception as e:
            log_info("Job failed: " + str(e))


if __name__ == "__main__":
    main()

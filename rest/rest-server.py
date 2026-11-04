import base64
import hashlib
import json
import io

from flask import Flask, request, jsonify, Response
import redis
from minio import Minio

app = Flask(__name__)

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

m = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin123",
    secure=False
)

QUEUE_BUCKET = "queue"
OUTPUT_BUCKET = "output"
REDIS_QUEUE = "toWorkers"


@app.route("/", methods=["GET"])
def home():
    return "<h1>Music Separation Server</h1>"


@app.route("/apiv1/separate", methods=["POST"])
def separate():
    data = request.get_json()

    if not data or "mp3" not in data:
        return jsonify({"error": "Missing mp3 data"}), 400

    try:
        mp3_data = base64.b64decode(data["mp3"], validate=True)
    except Exception:
        return jsonify({"error": "Invalid base64 MP3 data"}), 400

    songhash = hashlib.sha256(mp3_data).hexdigest()

    # Save the original MP3 in MinIO
    m.put_object(
        QUEUE_BUCKET,
        songhash + ".mp3",
        io.BytesIO(mp3_data),
        len(mp3_data),
        content_type="audio/mpeg"
    )

    # Send a small job message to Redis
    job = {
        "songhash": songhash,
        "model": data.get("model", "mdx_extra_q"),
        "callback": data.get("callback"),
        "bucket": QUEUE_BUCKET,
        "object": songhash + ".mp3"
    }

    r.rpush(REDIS_QUEUE, json.dumps(job))

    return jsonify({
        "hash": songhash,
        "reason": "Song enqueued for separation"
    })


@app.route("/apiv1/queue/", methods=["GET"])
@app.route("/apiv1/queue", methods=["GET"])
def queue():
    jobs = r.lrange(REDIS_QUEUE, 0, -1)
    hashes = []

    for job in jobs:
        try:
            hashes.append(json.loads(job)["songhash"])
        except Exception:
            hashes.append(job)

    return jsonify({"queue": hashes})


@app.route("/apiv1/track/<songhash>/<track>", methods=["GET"])
def get_track(songhash, track):
    if track == "base.mp3":
        bucket = QUEUE_BUCKET
        object_name = songhash + ".mp3"
    elif track in ["vocals.mp3", "drums.mp3", "bass.mp3", "other.mp3"]:
        bucket = OUTPUT_BUCKET
        object_name = songhash + "-" + track
    else:
        return jsonify({"error": "Invalid track"}), 400

    try:
        obj = m.get_object(bucket, object_name)
        content = obj.read()
        obj.close()
        obj.release_conn()

        return Response(content, mimetype="audio/mpeg")

    except Exception:
        return jsonify({"error": "Track not found"}), 404


@app.route("/apiv1/remove/<songhash>/<track>", methods=["GET"])
def remove_track(songhash, track):
    if track == "base.mp3":
        bucket = QUEUE_BUCKET
        object_name = songhash + ".mp3"
    elif track in ["vocals.mp3", "drums.mp3", "bass.mp3", "other.mp3"]:
        bucket = OUTPUT_BUCKET
        object_name = songhash + "-" + track
    else:
        return jsonify({"error": "Invalid track"}), 400

    try:
        m.remove_object(bucket, object_name)
        return jsonify({"reason": "Track removed"})
    except Exception:
        return jsonify({"error": "Track not found"}), 404


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

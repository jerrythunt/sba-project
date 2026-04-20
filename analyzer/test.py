import json
import yaml
import logging
import logging.config
from kafka import KafkaConsumer
from flask import Flask, jsonify

# -------------------------
# Logging Setup
# -------------------------
with open("log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

logger.info("Starting Analyzer Service")

# -------------------------
# Load App Config
# -------------------------
with open("app_conf.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

kafka_config = app_config["events"]
topic_name = kafka_config["topic"]
bootstrap_server = f"{kafka_config['hostname']}:{kafka_config['port']}"

logger.info(f"Kafka configured for topic '{topic_name}' at {bootstrap_server}")

app = Flask(__name__)

# -------------------------
# Kafka Consumer Creator
# -------------------------
def get_consumer():
    logger.debug("Creating new Kafka consumer")
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_server,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000
    )

# -------------------------
# Get Message by Type + Index
# -------------------------
def get_message_by_type_and_index(msg_type, index):
    logger.info(f"Request received for {msg_type} at index {index}")
    consumer = get_consumer()
    counter = 0

    for msg in consumer:
        data = json.loads(msg.value.decode("utf-8"))
        if data.get("type") == msg_type:
            if counter == index:
                logger.info(f"{msg_type.capitalize()} found at index {index}")
                return {"payload": data}, 200
            counter += 1

    logger.warning(f"No {msg_type} event found at index {index}")
    return {"message": f"No {msg_type} event at index {index}"}, 404

# -------------------------
# Get Stats
# -------------------------
def get_stats():
    logger.info("Stats endpoint called")
    consumer = get_consumer()
    arrivals = 0
    delays = 0

    for msg in consumer:
        data = json.loads(msg.value.decode("utf-8"))
        if data.get("type") == "arrival":
            arrivals += 1
        elif data.get("type") == "delay":
            delays += 1

    logger.info(f"Stats calculated - Arrivals: {arrivals}, Delays: {delays}")
    return {"total_arrivals": arrivals, "total_delays": delays}, 200

# -------------------------
# Endpoints
# -------------------------
@app.route("/arrival/<int:index>")
def arrival(index):
    return get_message_by_type_and_index("arrival", index)

@app.route("/delay/<int:index>")
def delay(index):
    return get_message_by_type_and_index("delay", index)

@app.route("/stats")
def stats():
    return get_stats()

# -------------------------
# Run App
# -------------------------
if __name__ == "__main__":
    logger.info("Analyzer service running on port 8081")
    app.run(port=8081, debug=False)
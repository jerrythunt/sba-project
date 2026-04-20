import json
import yaml
import logging
import logging.config
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from connexion import FlaskApp
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware
from threading import Thread
import time
import random

# ----------------------------
# Logging setup
# ----------------------------
with open("/config/analyzer_log_config.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")
logger.info("Starting Analyzer Service")

# ----------------------------
# App config
# ----------------------------
with open("/config/analyzer_config.yml", "r") as f:
    app_config = yaml.safe_load(f.read())

kafka_config = app_config["events"]
topic_name = kafka_config["topic"]
bootstrap_server = f"{kafka_config['hostname']}:{kafka_config['port']}"

# ----------------------------
# In-memory stats/cache
# ----------------------------
stats = {
    "num_arrival_events": 0,
    "num_delay_events": 0,
    "max_arrival_delay": 0,
    "max_reported_delay": 0,
    "arrival_events": [],
    "delay_events": []
}

# ----------------------------
# Kafka Consumer Wrapper
# ----------------------------
class KafkaConsumerWrapper:
    def __init__(self, brokers, topics, group_id='analyzer-group'):
        self.brokers = brokers
        self.topics = topics if isinstance(topics, list) else [topics]
        self.group_id = group_id
        self.consumer = None
        self.connect()

    def connect(self):
        while True:
            try:
                self.consumer = KafkaConsumer(
                    *self.topics,
                    bootstrap_servers=[self.brokers],
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id=self.group_id,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                )
                logger.info("Kafka consumer connected")
                break
            except KafkaError as e:
                logger.warning(f"Kafka connection failed: {e}, retrying...")
                time.sleep(random.randint(500, 1500) / 1000)

    def messages(self):
        if self.consumer is None:
            self.connect()
        while True:
            try:
                for msg in self.consumer:
                    yield msg
            except KafkaError as e:
                logger.warning(f"Kafka error: {e}, reconnecting...")
                self.consumer = None
                self.connect()

# ----------------------------
# Background Kafka processing
# ----------------------------
def process_messages():
    kafka_wrapper = KafkaConsumerWrapper(bootstrap_server, topic_name)

    # attach for health check
    app.kafka_wrapper = kafka_wrapper

    for message in kafka_wrapper.messages():
        data = message.value
        msg_type = data.get("type")
        payload = data.get("payload", {})

        logger.info(f"Received event: {msg_type}")

        if msg_type == "arrival":
            stats["num_arrival_events"] += 1
            delay = payload.get("delayMinutes", 0)
            stats["max_arrival_delay"] = max(stats["max_arrival_delay"], delay)
            stats["arrival_events"].append(data)

        elif msg_type == "delay":
            stats["num_delay_events"] += 1
            delay = payload.get("delayDurationMinutes", 0)
            stats["max_reported_delay"] = max(stats["max_reported_delay"], delay)
            stats["delay_events"].append(data)

# ----------------------------
# Start background thread
# ----------------------------
def setup_kafka_thread():
    t = Thread(target=process_messages)
    t.daemon = True
    t.start()

# ----------------------------
# Helper function
# ----------------------------
def _get_event_by_index(event_list, index):
    if index is None or index < 0:
        return {"message": "index must be non-negative"}, 400

    if index >= len(event_list):
        return {"message": "not found"}, 404

    return event_list[index], 200

# ----------------------------
# Endpoints
# ----------------------------
def get_arrival_event(index: int):
    return _get_event_by_index(stats["arrival_events"], index)

def get_delay_event(index: int):
    return _get_event_by_index(stats["delay_events"], index)

def get_latest_arrival():
    if not stats["arrival_events"]:
        return {"message": "No arrival events"}, 404
    return stats["arrival_events"][-1], 200

def get_latest_delay():
    if not stats["delay_events"]:
        return {"message": "No delay events"}, 404
    return stats["delay_events"][-1], 200

def get_stats():
    return {
        "num_arrival_events": stats["num_arrival_events"],
        "num_delay_events": stats["num_delay_events"],
        "max_arrival_delay": stats["max_arrival_delay"],
        "max_reported_delay": stats["max_reported_delay"],
    }, 200

def get_health():
    return {"status": "ok"}, 200

# ----------------------------
# Create app
# ----------------------------
app = FlaskApp(__name__)
app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_api("openapi.yml", strict_validation=True, validate_responses=True)

# ----------------------------
# Run
# ----------------------------
if __name__ == "__main__":
    setup_kafka_thread()
    logger.info("Analyzer running on port 8080")
    app.run(port=8080, host="0.0.0.0")
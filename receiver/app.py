import os
import connexion
import uuid
import yaml
import logging
import logging.config
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
import random

# ----------------------------
# Logging
# ----------------------------
with open("/config/receiver_log_config.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

# ----------------------------
# App config
# ----------------------------
with open('/config/receiver_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

kafka_config = app_config["events"]

# ----------------------------
# Health Check
# ----------------------------
def get_health():
    return {"status": "ok"}, 200

# ----------------------------
# Kafka Wrapper
# ----------------------------
class KafkaWrapper:
    """Thread-safe KafkaProducer wrapper with automatic reconnect"""
    def __init__(self, hostname, topic):
        self.hostname = hostname
        self.topic = topic
        self.producer = None
        self.connect()

    def connect(self):
        while True:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=f"{self.hostname}",
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                logger.info("Kafka producer connected!")
                break
            except KafkaError as e:
                # Retry
                logger.warning(f"Kafka not ready, retrying in 5s: {e}")
                time.sleep(5)

    def send(self, msg):
        """Send message safely, reconnect if needed"""
        if self.producer is None:
            self.connect()
        try:
            self.producer.send(self.topic, msg)
            self.producer.flush()
        except KafkaError as e:
            logger.warning(f"Kafka send failed, reconnecting: {e}")
            self.producer = None
            self.connect()
            self.producer.send(self.topic, msg)
            self.producer.flush()

# Initialize global Kafka wrapper
producer_wrapper = KafkaWrapper(
    hostname=f"{kafka_config['hostname']}:{kafka_config['port']}",
    topic=kafka_config["topic"]
)

# ----------------------------
# Receiver functions
# ----------------------------
def submit_arrival_time_batch(body):
    """Receives a batch of arrival times and produces each event to Kafka"""
    trace_id = str(uuid.uuid4())
    logger.info(f"Starting arrival batch processing: trace_id={trace_id}")
    try:
        for item in body.get("items", []):
            data = {
                "traceId": trace_id,
                "senderId": body["senderId"],
                "routeId": body["routeId"],
                "stopId": item["stopId"],
                "scheduledArrival": datetime.fromisoformat(item["scheduledArrival"].replace("Z", "+00:00")).isoformat(),
                "actualArrival": datetime.fromisoformat(item["actualArrival"].replace("Z", "+00:00")).isoformat(),
                "delayMinutes": item["delayMinutes"],
                "vehicleType": item["vehicleType"],
                "batchTimestamp": datetime.fromisoformat(body["sentAt"].replace("Z", "+00:00")).isoformat()
            }
            msg = {
                "type": "arrival",
                "datetime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "payload": data
            }
            producer_wrapper.send(msg)  # <--- use wrapper
            logger.debug(f"Produced Kafka message: stop_id={item['stopId']} trace_id={trace_id}")

        logger.info(f"Finished arrival batch: trace_id={trace_id}, total_items={len(body.get('items', []))}")
        return {"status": "success"}, 201
    except Exception as e:
        logger.exception(f"Unexpected error trace_id={trace_id}")
        return {"status": "error", "message": str(e)}, 500


def submit_delay_report_batch(body):
    """Receives a batch of delay reports and produces each event to Kafka"""
    trace_id = str(uuid.uuid4())
    logger.info(f"Starting delay batch processing: trace_id={trace_id}")
    try:
        for item in body.get("items", []):
            data = {
                "traceId": trace_id,
                "senderId": body["senderId"],
                "routeId": item["routeId"],
                "delayDurationMinutes": item["delayDurationMinutes"],
                "cause": item["cause"],
                "reportedAt": datetime.fromisoformat(item["reportedAt"].replace("Z", "+00:00")).isoformat(),
                "batchTimestamp": datetime.fromisoformat(body["sentAt"].replace("Z", "+00:00")).isoformat()
            }
            msg = {
                "type": "delay",
                "datetime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "payload": data
            }
            producer_wrapper.send(msg)  # <--- use wrapper
            logger.debug(f"Produced Kafka message for delay: trace_id={trace_id}")

        logger.info(f"Finished delay batch: trace_id={trace_id}, total_items={len(body.get('items', []))}")
        return {"status": "success"}, 201
    except Exception as e:
        logger.exception(f"Unexpected error trace_id={trace_id}")
        return {"status": "error", "message": str(e)}, 500

# ----------------------------
# Setup Connexion
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
yaml_path = os.path.join(BASE_DIR, "openapi.yml")

app = connexion.FlaskApp(__name__, specification_dir=BASE_DIR)
app.add_api(
    yaml_path,
    strict_validation=True,
    validate_responses=True
)

# ----------------------------
# Run the app
# ----------------------------
if __name__ == "__main__":
    app.run(port=8080, host="0.0.0.0")
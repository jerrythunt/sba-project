import os
import functools
import connexion
from datetime import datetime
from db import ENGINE
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from models import Base, Arrivals, Delays  
import yaml
import logging
import logging.config
import json
from threading import Thread
import time
import random
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ----------------------------
# Wait for MySQL and create tables
# ----------------------------
connected = False
while not connected:
    try:
        conn = ENGINE.connect()
        conn.close()
        connected = True
        print("Connected to MySQL, creating tables...")
    except OperationalError:
        print("MySQL not ready yet, retrying in 2s...")
        time.sleep(2)

Base.metadata.create_all(ENGINE)

# Logging 
with open("/config/storage_log_config.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)
    
logger = logging.getLogger("basicLogger")

# Database session setup
def make_session():
    return sessionmaker(bind=ENGINE)()

def use_db_session(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        session = make_session()
        try:
            return func(session, *args, **kwargs)
        finally:
            session.close()
    return wrapper

def parse_iso_datetime(dt_str):
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

# ----------------------------
# Storage functions
# ----------------------------
@use_db_session
def store_arrival_event(session, body):
    trace_id = body.get("traceId", "MISSING_TRACE_ID")
    stop_id = body.get("stopId", "UNKNOWN_STOP")
    try:
        event = Arrivals(
            sender_id=body["senderId"],
            trace_id=trace_id,
            route_id=body["routeId"],
            stop_id=body["stopId"],
            scheduled_arrival=parse_iso_datetime(body["scheduledArrival"]),
            actual_arrival=parse_iso_datetime(body["actualArrival"]),
            delay_minutes=body["delayMinutes"],
            vehicle_type=body["vehicleType"],
            batch_timestamp=parse_iso_datetime(body["batchTimestamp"])
        )
        session.add(event)
        session.commit()
        logger.debug(f"Inserted arrival into DB: trace_id={trace_id}, id={event.id}")
    except Exception:
        logger.exception(f"Failed to store arrival event: trace_id={trace_id}, stop_id={stop_id}")
        session.rollback()

@use_db_session
def store_delay_event(session, body):
    trace_id = body.get("traceId", "MISSING_TRACE_ID")
    route_id = body.get("routeId", "UNKNOWN_ROUTE")
    try:
        event = Delays(
            sender_id=body["senderId"],
            trace_id=trace_id,
            route_id=body["routeId"],
            delay_duration_minutes=body["delayDurationMinutes"],
            cause=body["cause"],
            reported_at=parse_iso_datetime(body["reportedAt"]),
            batch_timestamp=parse_iso_datetime(body["batchTimestamp"])
        )
        session.add(event)
        session.commit()
        logger.debug(f"Inserted delay into DB: trace_id={trace_id}, id={event.id}")
    except Exception:
        logger.exception(f"Failed to store delay event: trace_id={trace_id}, route_id={route_id}")
        session.rollback()

# ----------------------------
# GET endpoints
# ----------------------------
@use_db_session
def get_arrival_events(session, start_timestamp: str, end_timestamp: str):
    start_dt = parse_iso_datetime(start_timestamp)
    end_dt = parse_iso_datetime(end_timestamp)
    events = session.query(Arrivals).filter(
        Arrivals.date_created >= start_dt,
        Arrivals.date_created < end_dt
    ).all()
    return [
        {
            "id": e.id,
            "senderId": e.sender_id,
            "traceId": e.trace_id,
            "routeId": e.route_id,
            "stopId": e.stop_id,
            "scheduledArrival": e.scheduled_arrival.isoformat(),
            "actualArrival": e.actual_arrival.isoformat(),
            "delayMinutes": e.delay_minutes,
            "vehicleType": e.vehicle_type,
            "batchTimestamp": e.batch_timestamp.isoformat(),
            "dateCreated": e.date_created.isoformat()
        } for e in events
    ], 200

@use_db_session
def get_delay_events(session, start_timestamp: str, end_timestamp: str):
    start_dt = parse_iso_datetime(start_timestamp)
    end_dt = parse_iso_datetime(end_timestamp)
    events = session.query(Delays).filter(
        Delays.date_created >= start_dt,
        Delays.date_created < end_dt
    ).all()
    return [
        {
            "id": e.id,
            "senderId": e.sender_id,
            "traceId": e.trace_id,
            "routeId": e.route_id,
            "delayDurationMinutes": e.delay_duration_minutes,
            "cause": e.cause,
            "reportedAt": e.reported_at.isoformat(),
            "batchTimestamp": e.batch_timestamp.isoformat(),
            "dateCreated": e.date_created.isoformat()
        } for e in events
    ], 200

# ----------------------------
# Kafka Consumer Wrapper
# ----------------------------
class KafkaConsumerWrapper:
    def __init__(self, brokers, topics, group_id='storage-service-group'):
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
                logger.info("Kafka consumer connected successfully")
                break
            except KafkaError as e:
                logger.warning(f"Kafka consumer connection failed, retrying... ({e})")
                self.consumer = None
                time.sleep(random.randint(500, 1500) / 1000)

    def messages(self):
        if self.consumer is None:
            self.connect()
        while True:
            try:
                for msg in self.consumer:
                    yield msg
            except KafkaError as e:
                logger.warning(f"Kafka consumer error: {e}")
                self.consumer = None
                self.connect()

# ----------------------------
# Kafka message processing
# ----------------------------
def process_messages():
    with open("/config/storage_config.yml", "r") as f:
        app_config = yaml.safe_load(f.read())

    brokers = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    topics = app_config['events']['topic'].split(",")

    kafka_wrapper = KafkaConsumerWrapper(brokers, topics)
    # Attach Kafka wrapper to app for health check
    app.kafka_wrapper = kafka_wrapper

    for message in kafka_wrapper.messages():
        msg_json = message.value
        logger.info(f"Received Kafka message: {msg_json}")

        payload = msg_json.get("payload")
        if msg_json["type"] == "arrival":
            store_arrival_event(payload)
        elif msg_json["type"] == "delay":
            store_delay_event(payload)

def setup_kafka_thread():
    t1 = Thread(target=process_messages)
    t1.daemon = True
    t1.start()

# ----------------------------
# Health check endpoint
# ----------------------------
def get_health():
    return {"status": "ok"}, 200

# ----------------------------
# Setup Connexion app
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
yaml_path = os.path.join(BASE_DIR, "openapi.yml")

app = connexion.FlaskApp(__name__, specification_dir=BASE_DIR)
app.add_api(yaml_path, strict_validation=True, validate_responses=True)

# ----------------------------
# Run the app
# ----------------------------
if __name__ == "__main__":
    setup_kafka_thread()
    app.run(port=8080, host="0.0.0.0")
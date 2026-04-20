import yaml
import json
import logging
import logging.config
import httpx
from datetime import datetime
from threading import Thread
import time
from connexion import FlaskApp
from starlette.middleware.cors import CORSMiddleware
from connexion.middleware import MiddlewarePosition


# ----------------------------
# Logging
# ----------------------------
with open("/config/health_check_log_config.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

# ----------------------------
# App config
# ----------------------------
with open('/config/health_check_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

logger = logging.getLogger("basicLogger")

DATA_FILE = app_config["data_file"]
SERVICES = app_config["services"]
INTERVAL = app_config["interval_seconds"]
TIMEOUT = app_config["timeout_seconds"]

def read_data():
    try:
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    except:
        return {
            "receiver": "Down",
            "storage": "Down",
            "processing": "Down",
            "analyzer": "Down",
            "last_update": None
        }


def write_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)
        
def poll_services():
    while True:
        results = {}

        for service, url in SERVICES.items():
            try:
                response = httpx.get(url, timeout=TIMEOUT)
                if response.status_code == 200:
                    results[service] = "Up"
                else:
                    results[service] = "Down"
            except Exception:
                results[service] = "Down"

            logger.info(f"Health check: {service} -> {results[service]}")

        results["last_update"] = datetime.utcnow().isoformat()

        write_data(results)

        time.sleep(INTERVAL)
        
def start_background_thread():
    t = Thread(target=poll_services)
    t.daemon = True
    t.start()
    
def get_health_status():
    data = read_data()

    logger.info("Health status requested via API")

    return data, 200

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

# 🔥 START THREAD HERE (THIS IS THE FIX)
start_background_thread()

if __name__ == "__main__":
    logger.info("Health Check Service running on port 8120")
    app.run(port=8120, host="0.0.0.0")
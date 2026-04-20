import os
import connexion
import yaml
import logging
import logging.config
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import httpx
import json
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware
from connexion import FlaskApp

# Logging setup
with open("/config/processing_log_config.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

# Load app configuration
CONFIG_FILE = "/config/processing_config.yml"

with open(CONFIG_FILE, "r") as f:
    app_config = yaml.safe_load(f)

ARRIVALS_URL = app_config["storage_service"]["arrivals_url"]
DELAYS_URL = app_config["storage_service"]["delays_url"]
SCHED_INTERVAL = app_config["processing"]["interval_seconds"]

# Persisted stats file (mounted volume)
STATS_FILE = "/data/data.json"

# Default statistics written to JSON
DEFAULT_STATS = {
    "cumulative_arrivals": 0,
    "cumulative_delays": 0,
    "max_arrival_delay_minutes": 0,
    "max_reported_delay_minutes": 0,
    "last_updated": None
}

# ----------------------------
# Helper functions
# ----------------------------
 
# Convert ISO string to datetime object
def parse_iso_datetime(dt_str):
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

def read_stats_file():
    """Read stats from JSON file, return default if missing"""
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.exception(f"Error reading stats file: {str(e)}")
            return DEFAULT_STATS.copy()
    return DEFAULT_STATS.copy()

def write_stats_file(stats):
    """Write stats dictionary to JSON file"""
    try:
        with open(STATS_FILE, "w") as f:
            json.dump(stats, f, indent=2)
    except Exception as e:
        logger.exception(f"Error writing stats file: {str(e)}")

# ----------------------------
# Periodic statistics processing
# ----------------------------
def populate_stats():
    """Fetch events from storage and update statistics"""
    logger.info("Periodic processing started...")

    stats = read_stats_file()
    last_time_str = stats.get("last_updated")
    start_time = parse_iso_datetime(last_time_str) if last_time_str else datetime.utcnow() - timedelta(days=365)
    end_time = datetime.utcnow()

    params = {
        "start_timestamp": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_timestamp": end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    try:
        # Fetch arrivals
        arrivals_resp = httpx.get(ARRIVALS_URL, params=params)
        if arrivals_resp.status_code != 200:
            logger.error(f"Failed to fetch arrivals: {arrivals_resp.status_code} {arrivals_resp.text}")
            arrivals = []
        else:
            arrivals = arrivals_resp.json()
        logger.info(f"Received {len(arrivals)} arrival events")

        # Fetch delays
        delays_resp = httpx.get(DELAYS_URL, params=params)
        if delays_resp.status_code != 200:
            logger.error(f"Failed to fetch delays: {delays_resp.status_code} {delays_resp.text}")
            delays = []
        else:
            delays = delays_resp.json()
        logger.info(f"Received {len(delays)} delay events")

        # Update statistics
        stats["cumulative_arrivals"] += len(arrivals)
        stats["cumulative_delays"] += len(delays)
        stats["max_arrival_delay_minutes"] = max(
            stats.get("max_arrival_delay_minutes", 0),
            max([a.get("delayMinutes", 0) for a in arrivals], default=0)
        )
        stats["max_reported_delay_minutes"] = max(
            stats.get("max_reported_delay_minutes", 0),
            max([d.get("delayDurationMinutes", 0) for d in delays], default=0)
        )
        stats["last_updated"] = end_time.isoformat() + "Z"

        write_stats_file(stats)
        logger.debug(f"Updated statistics: {stats}")
        logger.info("Periodic processing ended.")

    except Exception as e:
        logger.exception(f"Error in populate_stats: {str(e)}")

def init_scheduler():
    """Setup periodic call to populate_stats"""
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(
        populate_stats,
        "interval",
        seconds=SCHED_INTERVAL
    )
    sched.start()
    logger.info(f"Scheduler initialized with interval {SCHED_INTERVAL} seconds")

# ----------------------------
# GET endpoint for /statistics
# ----------------------------
def get_statistics():
    """Return statistics from JSON file"""
    logger.info("Received request for /statistics")

    if not os.path.exists(STATS_FILE):
        logger.error("Statistics file not found")
        logger.info("Completed request for /statistics")
        return {"message": "Statistics do not exist"}, 404

    try:
        with open(STATS_FILE, "r") as f:
            stats = json.load(f)
        logger.debug(f"Statistics content: {stats}")
        logger.info("Completed request for /statistics")
        return stats, 200
    except Exception as e:
        logger.exception(f"Error reading statistics file: {str(e)}")
        return {"message": "Error reading statistics"}, 500
    
def get_health():
    return {"status": "ok"}, 200

# ----------------------------
# Setup Connexion app
# ----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

yaml_path = os.path.join(BASE_DIR, "openapi.yml")
app = FlaskApp(__name__)

app.add_middleware(
CORSMiddleware,
position=MiddlewarePosition.BEFORE_EXCEPTION,
allow_origins=["*"],
allow_credentials=True,
allow_methods=["*"],
allow_headers=["*"],
)

app.add_api(yaml_path)

# ----------------------------
# Run the app
# ----------------------------
if __name__ == "__main__":
    populate_stats()  # populate immediately on start
    init_scheduler()  # start periodic updates
    app.run(port=8080, host="0.0.0.0")

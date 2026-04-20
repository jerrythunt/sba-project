# db_reset.py
import logging
import yaml
import logging.config
from db import ENGINE
from models import Base, Arrivals, Delays

# ----------------------------
# Optional: Logging setup
# ----------------------------
with open("log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger("basicLogger")

# ----------------------------
# Drop and create tables
# ----------------------------
def reset_database():
    try:
        logger.info("Dropping all tables...")
        Base.metadata.drop_all(ENGINE)
        logger.info("All tables dropped.")

        logger.info("Creating all tables...")
        Base.metadata.create_all(ENGINE)
        logger.info("All tables created successfully.")
    except Exception as e:
        logger.exception(f"Error resetting database: {e}")

# ----------------------------
# Run if called directly
# ----------------------------
if __name__ == "__main__":
    reset_database()

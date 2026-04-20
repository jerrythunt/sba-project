from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml

# Load configuration
with open('/config/storage_config.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

# MySQL engine
from sqlalchemy import create_engine

# MySQL engine with robust connection handling
ENGINE = create_engine(
    f"mysql+pymysql://{app_config['datastore']['user']}:{app_config['datastore']['password']}"
    f"@{app_config['datastore']['hostname']}:{app_config['datastore']['port']}/{app_config['datastore']['db']}",
    pool_size=5,       # Keep 5 connections open in the pool
    pool_recycle=1800,  # Recycle connections every 30 minutes to avoid MySQL timeout
    pool_pre_ping=True  # Test connections before use to avoid stale connection errors
)

# Helper to create a new session
def make_session():
    return sessionmaker(bind=ENGINE)()

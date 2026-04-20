from db import ENGINE
from models import Base

# Create all tables defined in models.py
if __name__ == "__main__":
    Base.metadata.create_all(ENGINE)
    print("All tables created successfully!")

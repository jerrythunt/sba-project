from db import ENGINE
from models import Base

# Drop all tables defined in models.py
if __name__ == "__main__":
    Base.metadata.drop_all(ENGINE)
    print("All tables dropped successfully!")

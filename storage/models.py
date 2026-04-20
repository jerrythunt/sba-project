from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import Integer, String, Float, DateTime, func

# Base class for declarative models
class Base(DeclarativeBase):
    pass

# ----------------------------
# Arrival events
# ----------------------------
class Arrivals(Base):
    __tablename__ = "arrivals"

    id = mapped_column(Integer, primary_key=True)
    sender_id = mapped_column(String(250), nullable=False)
    trace_id = mapped_column(String(36), nullable=False)
    route_id = mapped_column(String(250), nullable=False)
    stop_id = mapped_column(String(250), nullable=False)
    scheduled_arrival = mapped_column(DateTime, nullable=False)
    actual_arrival = mapped_column(DateTime, nullable=False)
    delay_minutes = mapped_column(Float, nullable=False)
    vehicle_type = mapped_column(String(50), nullable=False)  # BUS or TRAIN
    batch_timestamp = mapped_column(DateTime, nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=func.now())

# ----------------------------
# Delay events
# ----------------------------
class Delays(Base):
    __tablename__ = "delays"

    id = mapped_column(Integer, primary_key=True)
    sender_id = mapped_column(String(250), nullable=False)
    trace_id = mapped_column(String(36), nullable=False)
    route_id = mapped_column(String(250), nullable=False)
    delay_duration_minutes = mapped_column(Float, nullable=False)
    cause = mapped_column(String(50), nullable=False)  # TRAFFIC, WEATHER, etc.
    reported_at = mapped_column(DateTime, nullable=False)
    batch_timestamp = mapped_column(DateTime, nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=func.now())


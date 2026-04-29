from fastapi import FastAPI, HTTPException, Depends, Request
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, Boolean, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
import pandas as pd
import os

from fastapi.responses import Response
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST
import logging
import uuid
from pythonjsonlogger import jsonlogger

app = FastAPI(title="Driver Service")

DATABASE_URL = "sqlite+aiosqlite:///./driver_service.db"

engine = create_async_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

avg_driver_rating = Gauge(
    "avg_driver_rating",
    "Average driver rating"
)
avg_driver_rating.set(4.3)

logger = logging.getLogger("driver-service")
logger.setLevel(logging.INFO)

log_handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    "%(asctime)s %(levelname)s %(name)s %(message)s %(correlation_id)s"
)
log_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(log_handler)


def get_correlation_id(request: Request):
    return request.headers.get("X-Correlation-ID", str(uuid.uuid4()))


class Driver(Base):
    __tablename__ = "drivers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    phone = Column(String)
    vehicle_type = Column(String)
    license_plate = Column(String)
    city = Column(String)
    is_active = Column(Boolean, default=True)


class DriverRequest(BaseModel):
    name: str
    phone: str
    vehicle_type: str
    license_plate: str
    city: str
    is_active: bool = True


class DriverStatusRequest(BaseModel):
    is_active: bool


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db():
    async with SessionLocal() as session:
        yield session


def to_bool(value):
    return str(value).lower() in ["true", "1", "yes", "active"]


@app.on_event("startup")
async def startup_event():
    await init_db()
    
    async with SessionLocal() as db:
        result = await db.execute(select(Driver))
        if len(result.scalars().all()) == 0:
            csv_path = os.getenv("DRIVERS_CSV_PATH", "../data/ride_drivers.csv")
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path)
                for _, row in df.iterrows():
                    driver = Driver(
                        id=int(row.get("driver_id", row.get("id", 0))),
                        name=str(row.get("name", "")),
                        phone=str(row.get("phone", "")),
                        vehicle_type=str(row.get("vehicle_type", "Car")),
                        license_plate=str(row.get("license_plate", row.get("vehicle_plate", ""))),
                        city=str(row.get("city", "Jaipur")),
                        is_active=to_bool(row.get("is_active", True))
                    )
                    db.add(driver)
                await db.commit()


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
def health():
    return {"service": "driver-service", "status": "UP"}


@app.get("/v1/drivers")
async def get_drivers(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Driver))
    return result.scalars().all()


@app.get("/v1/drivers/available")
async def get_available_driver(request: Request, city: str = "Jaipur", db: AsyncSession = Depends(get_db)):
    correlation_id = get_correlation_id(request)

    logger.info(
        f"Searching available driver in city {city}",
        extra={"correlation_id": correlation_id}
    )

    result = await db.execute(
        select(Driver).filter(Driver.city == city, Driver.is_active == True)
    )
    driver = result.scalars().first()

    if not driver:
        logger.error(
            "No active driver available",
            extra={"correlation_id": correlation_id}
        )
        raise HTTPException(status_code=404, detail="No active driver available")

    logger.info(
        f"Available driver found: {driver.id}",
        extra={"correlation_id": correlation_id}
    )

    return driver


@app.get("/v1/drivers/{driver_id}")
async def get_driver(driver_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Driver).filter(Driver.id == driver_id))
    driver = result.scalars().first()

    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    return driver


@app.post("/v1/drivers")
async def create_driver(request: DriverRequest, db: AsyncSession = Depends(get_db)):
    driver = Driver(
        name=request.name,
        phone=request.phone,
        vehicle_type=request.vehicle_type,
        license_plate=request.license_plate,
        city=request.city,
        is_active=request.is_active
    )

    db.add(driver)
    await db.commit()
    await db.refresh(driver)

    return driver


@app.put("/v1/drivers/{driver_id}")
async def update_driver(driver_id: int, request: DriverRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Driver).filter(Driver.id == driver_id))
    driver = result.scalars().first()

    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    driver.name = request.name
    driver.phone = request.phone
    driver.vehicle_type = request.vehicle_type
    driver.license_plate = request.license_plate
    driver.city = request.city
    driver.is_active = request.is_active

    await db.commit()
    await db.refresh(driver)

    return driver


@app.patch("/v1/drivers/{driver_id}/status")
async def update_driver_status(driver_id: int, request: DriverStatusRequest, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Driver).filter(Driver.id == driver_id))
    driver = result.scalars().first()

    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    driver.is_active = request.is_active

    await db.commit()
    await db.refresh(driver)

    return driver
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
import os

app = FastAPI(title="Driver Service")

DATABASE_URL = "sqlite:///./driver_service.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


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


Base.metadata.create_all(bind=engine)


def to_bool(value):
    return str(value).lower() in ["true", "1", "yes", "active"]


def seed_data():
    db = SessionLocal()

    if db.query(Driver).count() == 0:
        csv_path = "../data/ride_drivers.csv"

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

            db.commit()

    db.close()


seed_data()


@app.get("/health")
def health():
    return {"service": "driver-service", "status": "UP"}


@app.get("/v1/drivers")
def get_drivers():
    db = SessionLocal()
    drivers = db.query(Driver).all()
    db.close()
    return drivers


@app.get("/v1/drivers/available")
def get_available_driver(city: str = "Jaipur"):
    db = SessionLocal()

    driver = db.query(Driver).filter(
        Driver.city == city,
        Driver.is_active == True
    ).first()

    db.close()

    if not driver:
        raise HTTPException(status_code=404, detail="No active driver available")

    return driver


@app.get("/v1/drivers/{driver_id}")
def get_driver(driver_id: int):
    db = SessionLocal()
    driver = db.query(Driver).filter(Driver.id == driver_id).first()
    db.close()

    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    return driver


@app.post("/v1/drivers")
def create_driver(request: DriverRequest):
    db = SessionLocal()

    driver = Driver(
        name=request.name,
        phone=request.phone,
        vehicle_type=request.vehicle_type,
        license_plate=request.license_plate,
        city=request.city,
        is_active=request.is_active
    )

    db.add(driver)
    db.commit()
    db.refresh(driver)
    db.close()

    return driver


@app.put("/v1/drivers/{driver_id}")
def update_driver(driver_id: int, request: DriverRequest):
    db = SessionLocal()
    driver = db.query(Driver).filter(Driver.id == driver_id).first()

    if not driver:
        db.close()
        raise HTTPException(status_code=404, detail="Driver not found")

    driver.name = request.name
    driver.phone = request.phone
    driver.vehicle_type = request.vehicle_type
    driver.license_plate = request.license_plate
    driver.city = request.city
    driver.is_active = request.is_active

    db.commit()
    db.refresh(driver)
    db.close()

    return driver


@app.patch("/v1/drivers/{driver_id}/status")
def update_driver_status(driver_id: int, request: DriverStatusRequest):
    db = SessionLocal()
    driver = db.query(Driver).filter(Driver.id == driver_id).first()

    if not driver:
        db.close()
        raise HTTPException(status_code=404, detail="Driver not found")

    driver.is_active = request.is_active

    db.commit()
    db.refresh(driver)
    db.close()

    return driver
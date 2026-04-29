# Driver Service

The Driver Service manages driver profiles, vehicle information, and availability status for the ride-hailing platform.

## Features
- Manage driver registration and profile details.
- Query available drivers based on active status and city location.
- Track and update driver availability.

## Tech Stack
- **Framework:** FastAPI
- **Database:** SQLite
- **ORM:** SQLAlchemy (Asynchronous)

## Running Locally

1. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Start the service:
   ```bash
   uvicorn main:app --host 0.0.0.0 --port 8002
   ```

## Key Endpoints
- `GET /v1/drivers/available`: Fetch an active driver for a specific city.
- `GET /v1/drivers`: List all registered drivers.
- `PATCH /v1/drivers/{driver_id}/status`: Toggle a driver's active status.

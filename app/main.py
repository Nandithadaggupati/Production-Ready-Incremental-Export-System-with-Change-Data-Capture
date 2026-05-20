import uuid
from datetime import datetime, timezone
from fastapi import FastAPI, BackgroundTasks, Header, HTTPException, Response, status, Depends
from fastapi.responses import JSONResponse
from app.config import settings
from app.database import init_db_pool, close_db_pool, get_db_pool
from app.worker import run_export_job

app = FastAPI(title="CDC Incremental Export System", version="1.0.0")

@app.on_event("startup")
async def startup_event():
    await init_db_pool()

@app.on_event("shutdown")
async def shutdown_event():
    await close_db_pool()

# Helper to validate header
def get_consumer_id(x_consumer_id: str = Header(None, alias="X-Consumer-ID")) -> str:
    if not x_consumer_id or not x_consumer_id.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing or empty X-Consumer-ID header"
        )
    return x_consumer_id.strip()

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    }

@app.post("/exports/full", status_code=status.HTTP_202_ACCEPTED)
async def trigger_full_export(
    background_tasks: BackgroundTasks,
    consumer_id: str = Depends(get_consumer_id)
):
    job_id = str(uuid.uuid4())
    timestamp = int(datetime.now(timezone.utc).timestamp())
    output_filename = f"full_{consumer_id}_{timestamp}.csv"

    # Enqueue background task
    background_tasks.add_task(
        run_export_job,
        job_id,
        consumer_id,
        "full",
        output_filename
    )

    return {
        "jobId": job_id,
        "status": "started",
        "exportType": "full",
        "outputFilename": output_filename
    }

@app.post("/exports/incremental", status_code=status.HTTP_202_ACCEPTED)
async def trigger_incremental_export(
    background_tasks: BackgroundTasks,
    consumer_id: str = Depends(get_consumer_id)
):
    job_id = str(uuid.uuid4())
    timestamp = int(datetime.now(timezone.utc).timestamp())
    output_filename = f"incremental_{consumer_id}_{timestamp}.csv"

    # Enqueue background task
    background_tasks.add_task(
        run_export_job,
        job_id,
        consumer_id,
        "incremental",
        output_filename
    )

    return {
        "jobId": job_id,
        "status": "started",
        "exportType": "incremental",
        "outputFilename": output_filename
    }

@app.post("/exports/delta", status_code=status.HTTP_202_ACCEPTED)
async def trigger_delta_export(
    background_tasks: BackgroundTasks,
    consumer_id: str = Depends(get_consumer_id)
):
    job_id = str(uuid.uuid4())
    timestamp = int(datetime.now(timezone.utc).timestamp())
    output_filename = f"delta_{consumer_id}_{timestamp}.csv"

    # Enqueue background task
    background_tasks.add_task(
        run_export_job,
        job_id,
        consumer_id,
        "delta",
        output_filename
    )

    return {
        "jobId": job_id,
        "status": "started",
        "exportType": "delta",
        "outputFilename": output_filename
    }

@app.get("/exports/watermark")
async def get_watermark(consumer_id: str = Depends(get_consumer_id)):
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT last_exported_at FROM watermarks WHERE consumer_id = $1",
            consumer_id
        )
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No watermark found for consumer: {consumer_id}"
            )
        
        last_exported = row["last_exported_at"].astimezone(timezone.utc)
        last_exported_at = last_exported.isoformat().replace("+00:00", "Z")
        return {
            "consumerId": consumer_id,
            "lastExportedAt": last_exported_at
        }



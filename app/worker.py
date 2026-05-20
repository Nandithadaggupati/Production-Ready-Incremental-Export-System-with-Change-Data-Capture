import os
import csv
import time
from datetime import datetime, timezone
import asyncpg
from app.config import settings
from app.logging_config import logger

async def run_export_job(job_id: str, consumer_id: str, export_type: str, output_filename: str):
    # Log: Export job started
    started_log = {
        "custom_attrs": {
            "jobId": job_id,
            "consumerId": consumer_id,
            "exportType": export_type
        }
    }
    logger.info(f"Export job {job_id} started.", extra=started_log)
    
    start_time = time.time()
    output_path = os.path.join(settings.OUTPUT_DIR, output_filename)
    
    pool = None
    try:
        from app.database import get_db_pool
        pool = await get_db_pool()
    except Exception as e:
        error_msg = f"Failed to acquire database pool: {e}"
        failed_log = {
            "custom_attrs": {
                "jobId": job_id,
                "errorMessage": error_msg
            }
        }
        logger.error(f"Export job {job_id} failed.", extra=failed_log)
        return

    # Ensure output dir exists
    os.makedirs(settings.OUTPUT_DIR, exist_ok=True)

    rows_exported = 0
    max_updated_at = None

    # We will run the database queries in a connection acquired from pool
    try:
        async with pool.acquire() as conn:
            # For incremental or delta, find the watermark first
            watermark = None
            if export_type in ("incremental", "delta"):
                row = await conn.fetchrow(
                    "SELECT last_exported_at FROM watermarks WHERE consumer_id = $1",
                    consumer_id
                )
                if row:
                    watermark = row["last_exported_at"]
                else:
                    # No watermark exists. Trigger failure.
                    raise ValueError(f"No watermark exists for consumer {consumer_id}. Run a full export first.")

            # Prepare appropriate SQL query
            # We want to select the required fields: id, name, email, created_at, updated_at, is_deleted
            # To handle large datasets efficiently, we open a transaction and use a cursor.
            async with conn.transaction():
                if export_type == "full":
                    query = "SELECT id, name, email, created_at, updated_at, is_deleted FROM users WHERE is_deleted = FALSE"
                    args = []
                elif export_type == "incremental":
                    query = "SELECT id, name, email, created_at, updated_at, is_deleted FROM users WHERE is_deleted = FALSE AND updated_at > $1"
                    args = [watermark]
                elif export_type == "delta":
                    # Delta export includes soft-deleted users
                    query = "SELECT id, name, email, created_at, updated_at, is_deleted FROM users WHERE updated_at > $1"
                    args = [watermark]
                else:
                    raise ValueError(f"Invalid export type: {export_type}")

                # Open CSV file for writing
                with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.writer(csvfile)
                    
                    # Write headers based on export type
                    if export_type == "delta":
                        headers = ["operation", "id", "name", "email", "created_at", "updated_at", "is_deleted"]
                    else:
                        headers = ["id", "name", "email", "created_at", "updated_at", "is_deleted"]
                    writer.writerow(headers)

                    # Create a cursor to stream results and avoid loading 100,000+ rows into memory
                    # asyncpg supports cursor on connection
                    cursor = conn.cursor(query, *args)
                    async for record in cursor:
                        # Process records
                        r_id = record["id"]
                        r_name = record["name"]
                        r_email = record["email"]
                        # ISO 8601 timestamps
                        r_created = record["created_at"].isoformat()
                        r_updated = record["updated_at"].isoformat()
                        r_deleted = record["is_deleted"]

                        # Track maximum updated_at
                        if max_updated_at is None or record["updated_at"] > max_updated_at:
                            max_updated_at = record["updated_at"]

                        if export_type == "delta":
                            # operation is:
                            # 'DELETE' if is_deleted is true.
                            # 'INSERT' if created_at equals updated_at.
                            # Otherwise, 'UPDATE'.
                            if r_deleted:
                                op = "DELETE"
                            elif record["created_at"] == record["updated_at"]:
                                op = "INSERT"
                            else:
                                op = "UPDATE"
                            
                            row_data = [op, r_id, r_name, r_email, r_created, r_updated, str(r_deleted).upper()]
                        else:
                            row_data = [r_id, r_name, r_email, r_created, r_updated, str(r_deleted).upper()]

                        writer.writerow(row_data)
                        rows_exported += 1

            # Watermark transactional update: only if we successfully exported records
            # and only after writing of file is fully complete.
            if rows_exported > 0 and max_updated_at is not None:
                await conn.execute(
                    """
                    INSERT INTO watermarks (consumer_id, last_exported_at, updated_at)
                    VALUES ($1, $2, NOW())
                    ON CONFLICT (consumer_id)
                    DO UPDATE SET last_exported_at = EXCLUDED.last_exported_at, updated_at = NOW()
                    """,
                    consumer_id, max_updated_at
                )

        duration = time.time() - start_time
        # Log: Export job completed
        completed_log = {
            "custom_attrs": {
                "jobId": job_id,
                "rowsExported": rows_exported,
                "duration": round(duration, 4)
            }
        }
        logger.info(f"Export job {job_id} completed successfully.", extra=completed_log)

    except Exception as e:
        # Clean up partial file on failure to prevent corrupted outputs
        if os.path.exists(output_path):
            try:
                os.remove(output_path)
            except Exception as cleanup_err:
                logger.error(f"Failed to clean up partial file {output_path}: {cleanup_err}")

        duration = time.time() - start_time
        error_msg = str(e)
        # Log: Export job failed
        failed_log = {
            "custom_attrs": {
                "jobId": job_id,
                "errorMessage": error_msg
            }
        }
        logger.error(f"Export job {job_id} failed: {error_msg}", extra=failed_log)

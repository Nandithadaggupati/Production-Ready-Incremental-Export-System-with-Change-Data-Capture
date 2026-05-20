import os
import csv
import pytest
import asyncpg
from datetime import datetime, timedelta, timezone
from app.config import settings
from app.worker import run_export_job

# Helper to clean up files
def cleanup_file(path: str):
    if os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass

@pytest.mark.asyncio
async def test_full_export_success(db_pool: asyncpg.Pool):
    consumer_id = "worker-test-full"
    output_filename = f"full_{consumer_id}_test.csv"
    output_path = os.path.join(settings.OUTPUT_DIR, output_filename)
    
    # 1. Clean up potential leftover watermarks/files
    cleanup_file(output_path)
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM watermarks WHERE consumer_id = $1", consumer_id)
        # Clear any existing test-specific users
        await conn.execute("DELETE FROM users WHERE email LIKE 'test-full-%'")
        
        # 2. Insert test users
        now = datetime.now(timezone.utc)
        # User 1: Active
        await conn.execute(
            "INSERT INTO users (name, email, created_at, updated_at, is_deleted) VALUES ($1, $2, $3, $4, $5)",
            "Active User 1", "test-full-1@example.com", now - timedelta(days=5), now - timedelta(days=5), False
        )
        # User 2: Soft deleted
        await conn.execute(
            "INSERT INTO users (name, email, created_at, updated_at, is_deleted) VALUES ($1, $2, $3, $4, $5)",
            "Deleted User 2", "test-full-2@example.com", now - timedelta(days=4), now - timedelta(days=4), True
        )
        # User 3: Active, newer
        await conn.execute(
            "INSERT INTO users (name, email, created_at, updated_at, is_deleted) VALUES ($1, $2, $3, $4, $5)",
            "Active User 3", "test-full-3@example.com", now - timedelta(days=3), now - timedelta(days=3), False
        )

    try:
        # 3. Run the export job
        await run_export_job("job-1", consumer_id, "full", output_filename)
        
        # 4. Verify CSV is created and contains correct data
        assert os.path.exists(output_path)
        
        with open(output_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
            
        # Headers: id, name, email, created_at, updated_at, is_deleted
        assert rows[0] == ["id", "name", "email", "created_at", "updated_at", "is_deleted"]
        
        # Filter test rows to isolate from seeded users
        test_rows = [row for row in rows[1:] if row[2].startswith("test-full-")]
        assert len(test_rows) == 2
        
        emails = [row[2] for row in test_rows]
        assert "test-full-1@example.com" in emails
        assert "test-full-3@example.com" in emails
        assert "test-full-2@example.com" not in emails
        
        # 5. Verify watermark is updated to max updated_at of all exported (active) users
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT last_exported_at FROM watermarks WHERE consumer_id = $1", consumer_id)
            assert row is not None
            # Fetch the actual max updated_at of active users to verify exact match
            max_updated = await conn.fetchval("SELECT MAX(updated_at) FROM users WHERE is_deleted = FALSE")
            assert row["last_exported_at"].replace(microsecond=0) == max_updated.replace(microsecond=0)

    finally:
        cleanup_file(output_path)

@pytest.mark.asyncio
async def test_incremental_export_missing_watermark(db_pool: asyncpg.Pool):
    consumer_id = "worker-test-inc-missing"
    output_filename = f"inc_{consumer_id}_test.csv"
    output_path = os.path.join(settings.OUTPUT_DIR, output_filename)
    
    cleanup_file(output_path)
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM watermarks WHERE consumer_id = $1", consumer_id)
        
    try:
        # Running incremental export with no watermark should fail
        await run_export_job("job-2", consumer_id, "incremental", output_filename)
        
        # CSV file should not exist (cleaned up or never written)
        assert not os.path.exists(output_path)
        
        # Watermark should still not exist
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT last_exported_at FROM watermarks WHERE consumer_id = $1", consumer_id)
            assert row is None
    finally:
        cleanup_file(output_path)

@pytest.mark.asyncio
async def test_incremental_export_success(db_pool: asyncpg.Pool):
    consumer_id = "worker-test-inc-success"
    output_filename = f"inc_{consumer_id}_test.csv"
    output_path = os.path.join(settings.OUTPUT_DIR, output_filename)
    
    cleanup_file(output_path)
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM watermarks WHERE consumer_id = $1", consumer_id)
        await conn.execute("DELETE FROM users WHERE email LIKE 'test-inc-%'")
        
        now = datetime.now(timezone.utc)
        # Setup initial watermark
        watermark_time = now - timedelta(days=3)
        await conn.execute(
            "INSERT INTO watermarks (consumer_id, last_exported_at, updated_at) VALUES ($1, $2, NOW())",
            consumer_id, watermark_time
        )
        
        # User 1: Updated BEFORE watermark (should not export)
        await conn.execute(
            "INSERT INTO users (name, email, created_at, updated_at, is_deleted) VALUES ($1, $2, $3, $4, $5)",
            "Old User", "test-inc-1@example.com", now - timedelta(days=5), now - timedelta(days=4), False
        )
        # User 2: Updated AFTER watermark (should export)
        await conn.execute(
            "INSERT INTO users (name, email, created_at, updated_at, is_deleted) VALUES ($1, $2, $3, $4, $5)",
            "New Active User", "test-inc-2@example.com", now - timedelta(days=2), now - timedelta(days=2), False
        )
        # User 3: Updated AFTER watermark but deleted (should not export)
        await conn.execute(
            "INSERT INTO users (name, email, created_at, updated_at, is_deleted) VALUES ($1, $2, $3, $4, $5)",
            "New Deleted User", "test-inc-3@example.com", now - timedelta(days=1), now - timedelta(days=1), True
        )

    try:
        await run_export_job("job-3", consumer_id, "incremental", output_filename)
        
        assert os.path.exists(output_path)
        with open(output_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
            
        assert rows[0] == ["id", "name", "email", "created_at", "updated_at", "is_deleted"]
        
        # Filter test rows to isolate from seeded users
        test_rows = [row for row in rows[1:] if row[2].startswith("test-inc-")]
        assert len(test_rows) == 1
        
        emails = [row[2] for row in test_rows]
        assert "test-inc-2@example.com" in emails
        assert "test-inc-1@example.com" not in emails
        assert "test-inc-3@example.com" not in emails
        
        # Watermark should update to max updated_at of all exported (active) users after watermark_time
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT last_exported_at FROM watermarks WHERE consumer_id = $1", consumer_id)
            max_updated = await conn.fetchval(
                "SELECT MAX(updated_at) FROM users WHERE is_deleted = FALSE AND updated_at > $1",
                watermark_time
            )
            assert row["last_exported_at"].replace(microsecond=0) == max_updated.replace(microsecond=0)
    finally:
        cleanup_file(output_path)

@pytest.mark.asyncio
async def test_delta_export_success(db_pool: asyncpg.Pool):
    consumer_id = "worker-test-delta"
    output_filename = f"delta_{consumer_id}_test.csv"
    output_path = os.path.join(settings.OUTPUT_DIR, output_filename)
    
    cleanup_file(output_path)
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM watermarks WHERE consumer_id = $1", consumer_id)
        await conn.execute("DELETE FROM users WHERE email LIKE 'test-delta-%'")
        
        now = datetime.now(timezone.utc)
        watermark_time = now - timedelta(days=3)
        await conn.execute(
            "INSERT INTO watermarks (consumer_id, last_exported_at, updated_at) VALUES ($1, $2, NOW())",
            consumer_id, watermark_time
        )
        
        # User 1: INSERT operation (created_at == updated_at, updated_at > watermark)
        await conn.execute(
            "INSERT INTO users (name, email, created_at, updated_at, is_deleted) VALUES ($1, $2, $3, $4, $5)",
            "Insert User", "test-delta-1@example.com", now - timedelta(days=2), now - timedelta(days=2), False
        )
        # User 2: UPDATE operation (created_at != updated_at, updated_at > watermark)
        await conn.execute(
            "INSERT INTO users (name, email, created_at, updated_at, is_deleted) VALUES ($1, $2, $3, $4, $5)",
            "Update User", "test-delta-2@example.com", now - timedelta(days=5), now - timedelta(days=1), False
        )
        # User 3: DELETE operation (is_deleted == True, updated_at > watermark)
        await conn.execute(
            "INSERT INTO users (name, email, created_at, updated_at, is_deleted) VALUES ($1, $2, $3, $4, $5)",
            "Deleted User", "test-delta-3@example.com", now - timedelta(days=2), now - timedelta(hours=12), True
        )

    try:
        await run_export_job("job-4", consumer_id, "delta", output_filename)
        
        assert os.path.exists(output_path)
        with open(output_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
            
        # Headers: operation, id, name, email, created_at, updated_at, is_deleted
        assert rows[0] == ["operation", "id", "name", "email", "created_at", "updated_at", "is_deleted"]
        
        # Filter test rows to isolate from seeded users
        test_rows = [row for row in rows[1:] if row[3].startswith("test-delta-")]
        assert len(test_rows) == 3
        
        for r in test_rows:
            op, _, _, email, _, _, deleted = r
            if email == "test-delta-1@example.com":
                assert op == "INSERT"
                assert deleted == "FALSE"
            elif email == "test-delta-2@example.com":
                assert op == "UPDATE"
                assert deleted == "FALSE"
            elif email == "test-delta-3@example.com":
                assert op == "DELETE"
                assert deleted == "TRUE"
                
        # Watermark should update to max updated_at of all users (active or deleted) after watermark_time
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT last_exported_at FROM watermarks WHERE consumer_id = $1", consumer_id)
            max_updated = await conn.fetchval(
                "SELECT MAX(updated_at) FROM users WHERE updated_at > $1",
                watermark_time
            )
            assert row["last_exported_at"].replace(microsecond=0) == max_updated.replace(microsecond=0)
    finally:
        cleanup_file(output_path)

@pytest.mark.asyncio
async def test_worker_transaction_atomicity_on_failure(db_pool: asyncpg.Pool):
    consumer_id = "worker-test-fail-atomicity"
    output_filename = f"fail_{consumer_id}_test.csv"
    output_path = os.path.join(settings.OUTPUT_DIR, output_filename)
    
    cleanup_file(output_path)
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM watermarks WHERE consumer_id = $1", consumer_id)
        
    try:
        # Run export job with an invalid export type to trigger an exception during database execution
        await run_export_job("job-fail", consumer_id, "invalid_type", output_filename)
        
        # 1. Partial file must be deleted/non-existent
        assert not os.path.exists(output_path)
        
        # 2. Watermark must not be updated or created
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT last_exported_at FROM watermarks WHERE consumer_id = $1", consumer_id)
            assert row is None
    finally:
        cleanup_file(output_path)

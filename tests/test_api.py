import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_health_endpoint(client: AsyncClient):
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "timestamp" in data

@pytest.mark.asyncio
async def test_exports_endpoints_header_missing(client: AsyncClient):
    # Test all endpoints with missing header
    endpoints = [
        ("/exports/full", "POST"),
        ("/exports/incremental", "POST"),
        ("/exports/delta", "POST"),
        ("/exports/watermark", "GET"),
    ]
    for url, method in endpoints:
        if method == "POST":
            response = await client.post(url)
        else:
            response = await client.get(url)
        assert response.status_code == 400, f"Failed for {method} {url}"
        assert response.json()["detail"] == "Missing or empty X-Consumer-ID header"

@pytest.mark.asyncio
async def test_exports_endpoints_header_empty(client: AsyncClient):
    # Test all endpoints with empty or whitespace-only header
    endpoints = [
        ("/exports/full", "POST"),
        ("/exports/incremental", "POST"),
        ("/exports/delta", "POST"),
        ("/exports/watermark", "GET"),
    ]
    for url, method in endpoints:
        headers = {"X-Consumer-ID": "   "}
        if method == "POST":
            response = await client.post(url, headers=headers)
        else:
            response = await client.get(url, headers=headers)
        assert response.status_code == 400, f"Failed for {method} {url}"
        assert response.json()["detail"] == "Missing or empty X-Consumer-ID header"

@pytest.mark.asyncio
async def test_trigger_exports_success(client: AsyncClient, db_pool):
    # Triggers should return 202 Accepted
    headers = {"X-Consumer-ID": "test-consumer-api"}
    
    # Full export trigger
    response = await client.post("/exports/full", headers=headers)
    assert response.status_code == 202
    data = response.json()
    assert "jobId" in data
    assert data["status"] == "started"
    assert data["exportType"] == "full"
    assert "full_test-consumer-api_" in data["outputFilename"]

    # Incremental export trigger
    response = await client.post("/exports/incremental", headers=headers)
    assert response.status_code == 202
    data = response.json()
    assert "jobId" in data
    assert data["status"] == "started"
    assert data["exportType"] == "incremental"
    assert "incremental_test-consumer-api_" in data["outputFilename"]

    # Delta export trigger
    response = await client.post("/exports/delta", headers=headers)
    assert response.status_code == 202
    data = response.json()
    assert "jobId" in data
    assert data["status"] == "started"
    assert data["exportType"] == "delta"
    assert "delta_test-consumer-api_" in data["outputFilename"]

@pytest.mark.asyncio
async def test_get_watermark_not_found(client: AsyncClient, db_pool):
    headers = {"X-Consumer-ID": "non-existent-consumer"}
    # Clean up first in case it somehow exists
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM watermarks WHERE consumer_id = $1", "non-existent-consumer")
        
    response = await client.get("/exports/watermark", headers=headers)
    assert response.status_code == 404
    assert "No watermark found for consumer" in response.json()["detail"]

@pytest.mark.asyncio
async def test_get_watermark_success(client: AsyncClient, db_pool):
    headers = {"X-Consumer-ID": "test-consumer-watermark"}
    from datetime import datetime, timezone
    dummy_time = datetime(2026, 5, 20, 12, 0, 0, tzinfo=timezone.utc)
    
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM watermarks WHERE consumer_id = $1", "test-consumer-watermark")
        await conn.execute(
            "INSERT INTO watermarks (consumer_id, last_exported_at, updated_at) VALUES ($1, $2, NOW())",
            "test-consumer-watermark", dummy_time
        )
        
    response = await client.get("/exports/watermark", headers=headers)
    assert response.status_code == 200
    data = response.json()
    assert data["consumerId"] == "test-consumer-watermark"
    assert data["lastExportedAt"] == "2026-05-20T12:00:00Z"


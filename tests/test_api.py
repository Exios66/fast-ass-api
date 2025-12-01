# tests/test_api.py
import os
import io
import csv
import json
import pytest
from fastapi.testclient import TestClient

# Import app after CSV_DATA_DIR env has been set by conftest.py
from api.server.main import app

client = TestClient(app)


def test_empty_datasets_list():
    """Test listing datasets when none exist."""
    r = client.get("/datasets")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_create_row_creates_dataset_and_returns_id():
    """Test creating a row creates dataset and returns ID."""
    payload = {"name": "AgentAlpha", "role": "assistant", "notes": "initial"}
    r = client.post("/datasets/agents/rows", json=payload)
    assert r.status_code == 201
    body = r.json()
    assert "id" in body
    assert body["name"] == "AgentAlpha"
    # dataset should now appear in list
    ds = client.get("/datasets").json()
    assert any(f.endswith("agents.csv") for f in ds)


def test_schema_and_rows_and_search_pagination():
    """Test schema retrieval, row querying, and search."""
    # ensure schema present
    schema = client.get("/datasets/agents/schema")
    assert schema.status_code == 200
    cols = schema.json()["columns"]
    assert "id" in cols or "name" in cols

    # query rows
    q = client.get("/datasets/agents/rows")
    assert q.status_code == 200
    data = q.json()
    assert "total" in data and "rows" in data
    assert data["total"] >= 1

    # search for text
    s = client.get("/datasets/agents/rows", params={"search": "assistant"})
    assert s.status_code == 200
    assert s.json()["total"] >= 0


def test_update_and_delete_row():
    """Test updating and deleting a row."""
    # create a row
    r = client.post("/datasets/testrows/rows", json={"field": "value"})
    assert r.status_code == 201
    created = r.json()
    rid = created["id"]

    # update row
    up = client.put(f"/datasets/testrows/rows/{rid}", json={"field": "new", "extra": "x"})
    assert up.status_code == 200
    updated = up.json()
    assert updated["field"] == "new"
    assert updated["extra"] == "x"

    # delete row
    d = client.delete(f"/datasets/testrows/rows/{rid}")
    assert d.status_code == 204

    # verify deleted
    rows = client.get("/datasets/testrows/rows").json()
    assert rows["total"] == 0


def test_bulk_import_append_and_export(tmp_path):
    """Test bulk import with append mode and export."""
    # prepare a CSV to import
    csv_text = "name,role\nA,assistant\nB,assistant\n"
    files = {"file": ("imp.csv", csv_text, "text/csv")}
    # import into new dataset (append will create)
    r = client.post("/datasets/bulk1/import", files=files, params={"mode": "append"})
    assert r.status_code == 200
    resp = r.json()
    assert resp["status"] == "appended" or resp["status"] == "replaced"

    # export dataset and validate contents
    exp = client.get("/datasets/bulk1/export")
    assert exp.status_code == 200
    content = exp.content.decode("utf-8")
    reader = csv.DictReader(content.splitlines())
    rows = list(reader)
    assert any(row.get("name") == "A" for row in rows)
    assert any(row.get("name") == "B" for row in rows)

    # append again (import another CSV with different columns)
    csv_text2 = "name,level\nC,5\n"
    files2 = {"file": ("imp2.csv", csv_text2, "text/csv")}
    r2 = client.post("/datasets/bulk1/import", files=files2, params={"mode": "append"})
    assert r2.status_code == 200
    json2 = r2.json()
    assert json2["imported"] == 1

    # export and verify new row present and headers unified
    exp2 = client.get("/datasets/bulk1/export")
    assert exp2.status_code == 200
    content2 = exp2.content.decode("utf-8")
    reader2 = csv.DictReader(content2.splitlines())
    rows2 = list(reader2)
    assert any(r.get("name") == "C" for r in rows2)
    # ensure "level" column is present in header
    assert "level" in reader2.fieldnames


def test_bulk_import_replace_overwrites():
    """Test bulk import with replace mode overwrites existing data."""
    csv_text = "colA\nx\n"
    files = {"file": ("imp_rep.csv", csv_text, "text/csv")}
    # replace dataset
    r = client.post("/datasets/replace_me/import", files=files, params={"mode": "replace"})
    assert r.status_code == 200
    j = r.json()
    assert j["status"] == "replaced"

    # export and verify only new content present
    e = client.get("/datasets/replace_me/export")
    assert e.status_code == 200
    txt = e.content.decode("utf-8")
    reader = csv.DictReader(txt.splitlines())
    rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["colA"] == "x"


def test_health_endpoints():
    """Test health check endpoints."""
    # Basic health check
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "healthy"
    assert "timestamp" in data

    # Readiness check
    r2 = client.get("/health/ready")
    assert r2.status_code == 200
    data2 = r2.json()
    assert data2["status"] == "ready"
    assert data2["data_dir_writable"] is True


def test_metrics_endpoint():
    """Test metrics endpoint."""
    r = client.get("/metrics")
    assert r.status_code == 200
    data = r.json()
    assert "requests_total" in data
    assert "uptime_seconds" in data
    assert isinstance(data["requests_by_method"], dict)
    assert isinstance(data["requests_by_status"], dict)


def test_advanced_filtering():
    """Test advanced filtering options."""
    # Create test data
    client.post("/datasets/filter_test/rows", json={"name": "Item1", "value": "10", "score": "85"})
    client.post("/datasets/filter_test/rows", json={"name": "Item2", "value": "20", "score": "90"})
    client.post("/datasets/filter_test/rows", json={"name": "Item3", "value": "30", "score": "75"})

    # Test equals filter
    r = client.get("/datasets/filter_test/rows", params={"field": "name", "operator": "eq", "value": "Item1"})
    assert r.status_code == 200
    assert r.json()["total"] == 1

    # Test greater than filter
    r2 = client.get("/datasets/filter_test/rows", params={"field": "score", "operator": "gt", "value": "80"})
    assert r2.status_code == 200
    assert r2.json()["total"] >= 2

    # Test contains filter
    r3 = client.get("/datasets/filter_test/rows", params={"field": "name", "operator": "contains", "value": "Item"})
    assert r3.status_code == 200
    assert r3.json()["total"] >= 3


def test_bulk_create():
    """Test bulk create endpoint."""
    payload = {
        "rows": [
            {"name": "Bulk1", "value": "1"},
            {"name": "Bulk2", "value": "2"},
            {"name": "Bulk3", "value": "3"}
        ]
    }
    r = client.post("/datasets/bulk_create_test/rows/bulk", json=payload)
    assert r.status_code == 201
    data = r.json()
    assert data["created"] == 3
    assert len(data["rows"]) == 3
    assert all("id" in row for row in data["rows"])


def test_bulk_update():
    """Test bulk update endpoint."""
    # Create rows first
    r1 = client.post("/datasets/bulk_update_test/rows", json={"name": "Original1"})
    r2 = client.post("/datasets/bulk_update_test/rows", json={"name": "Original2"})
    id1 = r1.json()["id"]
    id2 = r2.json()["id"]

    # Bulk update
    payload = {
        "updates": [
            {"id": id1, "name": "Updated1"},
            {"id": id2, "name": "Updated2"}
        ]
    }
    r = client.put("/datasets/bulk_update_test/rows/bulk", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["updated"] == 2


def test_bulk_delete():
    """Test bulk delete endpoint."""
    # Create rows first
    r1 = client.post("/datasets/bulk_delete_test/rows", json={"name": "Delete1"})
    r2 = client.post("/datasets/bulk_delete_test/rows", json={"name": "Delete2"})
    id1 = r1.json()["id"]
    id2 = r2.json()["id"]

    # Bulk delete
    payload = {"ids": [id1, id2]}
    r = client.delete("/datasets/bulk_delete_test/rows/bulk", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["deleted"] == 2


def test_metadata_endpoints():
    """Test metadata get and update endpoints."""
    # Create a dataset first
    client.post("/datasets/metadata_test/rows", json={"name": "Test"})

    # Get metadata (should return defaults)
    r = client.get("/datasets/metadata_test/metadata")
    assert r.status_code == 200
    data = r.json()
    assert "description" in data
    assert "schema_version" in data

    # Update metadata
    payload = {"description": "Test dataset", "schema_version": "2.0"}
    r2 = client.put("/datasets/metadata_test/metadata", json=payload)
    assert r2.status_code == 200
    data2 = r2.json()
    assert data2["description"] == "Test dataset"
    assert data2["schema_version"] == "2.0"


def test_json_export():
    """Test JSON export format."""
    # Create test data
    client.post("/datasets/json_export_test/rows", json={"name": "Item1"})
    client.post("/datasets/json_export_test/rows", json={"name": "Item2"})

    # Export as JSON
    r = client.get("/datasets/json_export_test/export", params={"format": "json"})
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) >= 2


def test_pagination_links():
    """Test pagination with navigation links."""
    # Create multiple rows
    for i in range(15):
        client.post("/datasets/pagination_test/rows", json={"index": str(i)})

    # Get first page
    r = client.get("/datasets/pagination_test/rows", params={"limit": 5, "offset": 0})
    assert r.status_code == 200
    data = r.json()
    assert "links" in data
    assert data["links"]["first"] is not None
    assert data["links"]["next"] is not None
    assert data["links"]["prev"] is None  # First page


def test_dataset_name_validation():
    """Test that invalid dataset names are rejected."""
    # Test with invalid characters
    r = client.get("/datasets/../../etc/passwd/schema")
    assert r.status_code == 400

    # Test with valid name
    r2 = client.get("/datasets/valid_name/schema")
    # Should either 404 (not found) or 200 (exists), but not 400
    assert r2.status_code in [200, 404]


def test_error_handling():
    """Test error handling for various scenarios."""
    # 404 for non-existent dataset
    r = client.get("/datasets/nonexistent/rows")
    assert r.status_code == 404
    assert "error" in r.json()

    # 404 for non-existent row
    r2 = client.put("/datasets/test/rows/nonexistent", json={"name": "Test"})
    assert r2.status_code == 404

    # 422 for invalid JSON
    r3 = client.post("/datasets/test/rows", json="invalid")
    assert r3.status_code == 422


def test_sorting():
    """Test sorting functionality."""
    # Create rows with different values
    client.post("/datasets/sort_test/rows", json={"name": "Zebra", "value": "3"})
    client.post("/datasets/sort_test/rows", json={"name": "Alpha", "value": "1"})
    client.post("/datasets/sort_test/rows", json={"name": "Beta", "value": "2"})

    # Sort ascending
    r = client.get("/datasets/sort_test/rows", params={"sort_by": "name", "sort_order": "asc"})
    assert r.status_code == 200
    rows = r.json()["rows"]
    assert rows[0]["name"] == "Alpha"

    # Sort descending
    r2 = client.get("/datasets/sort_test/rows", params={"sort_by": "name", "sort_order": "desc"})
    assert r2.status_code == 200
    rows2 = r2.json()["rows"]
    assert rows2[0]["name"] == "Zebra"


def test_regex_filtering():
    """Test regex filtering."""
    # Create test data
    client.post("/datasets/regex_test/rows", json={"email": "user1@example.com"})
    client.post("/datasets/regex_test/rows", json={"email": "user2@test.com"})
    client.post("/datasets/regex_test/rows", json={"email": "admin@example.com"})

    # Filter with regex
    r = client.get("/datasets/regex_test/rows", params={
        "field": "email",
        "operator": "regex",
        "value": ".*@example\\.com"
    })
    assert r.status_code == 200
    data = r.json()
    assert data["total"] >= 2  # Should match user1 and admin


def test_date_filtering():
    """Test date range filtering."""
    # Create rows with dates
    client.post("/datasets/date_test/rows", json={"created_at": "2025-11-01T00:00:00Z"})
    client.post("/datasets/date_test/rows", json={"created_at": "2025-11-15T00:00:00Z"})
    client.post("/datasets/date_test/rows", json={"created_at": "2025-11-30T00:00:00Z"})

    # Filter date after
    r = client.get("/datasets/date_test/rows", params={
        "field": "created_at",
        "operator": "date_after",
        "value": "2025-11-10T00:00:00Z"
    })
    assert r.status_code == 200
    assert r.json()["total"] >= 2


def test_request_id_header():
    """Test that request ID is included in response headers."""
    r = client.get("/health")
    assert r.status_code == 200
    assert "X-Request-ID" in r.headers
    assert len(r.headers["X-Request-ID"]) > 0

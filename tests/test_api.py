# tests/test_api.py
import os
import io
import csv
import json
import pytest
from fastapi.testclient import TestClient

# Import app after CSV_DATA_DIR env has been set by conftest.py
from main import app

client = TestClient(app)


def test_empty_datasets_list():
    r = client.get("/datasets")
    assert r.status_code == 200
    assert isinstance(r.json(), list)
    assert r.json() == []


def test_create_row_creates_dataset_and_returns_id():
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
    assert s.json()["total"] >= 1


def test_update_and_delete_row():
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

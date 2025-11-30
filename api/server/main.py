# main.py
import os
import csv
import uuid
import tempfile
from typing import List, Dict, Any, Optional, Iterator
from fastapi import FastAPI, HTTPException, Path, Query, Body, status, UploadFile, File
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from filelock import FileLock
from pydantic import BaseModel

# Allow overriding data directory via env var for testing or deployment
DATA_DIR = os.environ.get("CSV_DATA_DIR") or os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

app = FastAPI(title="Agent Data CSV API", version="1.1.0")


def dataset_path(name: str) -> str:
    if not name.lower().endswith(".csv"):
        name = f"{name}.csv"
    path = os.path.join(DATA_DIR, name)
    return path


def load_csv_rows(path: str) -> List[Dict[str, str]]:
    """Return list of rows as dictionaries (strings)."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Dataset file not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [dict(row) for row in reader]
    return rows


def read_headers(path: str) -> List[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            return []
        return headers


def write_csv_atomic(path: str, fieldnames: List[str], rows: List[Dict[str, Any]]):
    """Write CSV atomically (temp file -> replace). Values coerced to strings."""
    dirpath = os.path.dirname(path) or "."
    fd, tmp_path = tempfile.mkstemp(dir=dirpath, suffix=".tmp")
    os.close(fd)
    try:
        with open(tmp_path, "w", newline="", encoding="utf-8") as tmpf:
            writer = csv.DictWriter(tmpf, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for r in rows:
                row = {k: ("" if r.get(k) is None else str(r.get(k))) for k in fieldnames}
                writer.writerow(row)
        os.replace(tmp_path, path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def ensure_id_column(path: str, lock: FileLock) -> str:
    """
    Ensure CSV has 'id' column. If missing, add 'id' (uuid4) to each row and rewrite the file.
    Returns the name of the id column (always 'id').
    """
    with lock:
        headers = read_headers(path)
        if "id" in headers:
            return "id"
        rows = load_csv_rows(path)
        for r in rows:
            r["id"] = str(uuid.uuid4())
        new_fieldnames = headers + ["id"]
        write_csv_atomic(path, new_fieldnames, rows)
        return "id"


@app.get("/datasets", summary="List available CSV datasets")
def list_datasets() -> List[str]:
    files = []
    for f in os.listdir(DATA_DIR):
        if f.lower().endswith(".csv"):
            files.append(f)
    return files


@app.get("/datasets/{name}/schema", summary="Get CSV column names")
def get_schema(name: str = Path(..., description="Dataset filename (with or without .csv)")):
    path = dataset_path(name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset not found")
    headers = read_headers(path)
    return {"columns": headers}


@app.get("/datasets/{name}/rows", summary="Query rows from a dataset")
def get_rows(
    name: str = Path(..., description="Dataset filename (with or without .csv)"),
    search: Optional[str] = Query(None, description="Search text (case-insensitive) across all fields"),
    limit: Optional[int] = Query(100, ge=1, le=10000, description="Max rows to return"),
    offset: Optional[int] = Query(0, ge=0),
    sort_by: Optional[str] = Query(None),
    sort_order: Optional[str] = Query("asc", regex="^(asc|desc)$")
):
    path = dataset_path(name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset not found")
    rows = load_csv_rows(path)

    # search
    if search:
        s = search.lower()
        rows = [r for r in rows if any((v or "").lower().find(s) != -1 for v in r.values())]

    total = len(rows)

    # sort
    if sort_by:
        rows.sort(key=lambda r: (r.get(sort_by) or ""), reverse=(sort_order == "desc"))

    # pagination
    start = offset
    end = offset + limit if limit is not None else None
    paged = rows[start:end]
    return {"total": total, "rows": paged}


class RowModel(BaseModel):
    __root__: Dict[str, Any]


@app.post("/datasets/{name}/rows", status_code=status.HTTP_201_CREATED, summary="Create a row in dataset")
def create_row(
    name: str = Path(..., description="Dataset filename (with or without .csv)"),
    payload: RowModel = Body(..., description="Row object with key/value pairs")
):
    path = dataset_path(name)
    lock = FileLock(path + ".lock")
    with lock:
        if not os.path.exists(path):
            data = payload.__root__.copy()
            if "id" not in data:
                data["id"] = str(uuid.uuid4())
            fieldnames = list(data.keys())
            write_csv_atomic(path, fieldnames, [data])
            return JSONResponse(status_code=201, content=data)

        ensure_id_column(path, lock)

        rows = load_csv_rows(path)
        headers = read_headers(path)
        data = payload.__root__.copy()
        if "id" not in data:
            data["id"] = str(uuid.uuid4())

        new_headers = list(dict.fromkeys(headers + list(k for k in data.keys() if k not in headers)))
        rows.append({k: ("" if data.get(k) is None else str(data.get(k))) for k in new_headers})
        write_csv_atomic(path, new_headers, rows)
        return JSONResponse(status_code=201, content=data)


@app.put("/datasets/{name}/rows/{id}", summary="Update a row by id")
def update_row(
    name: str = Path(..., description="Dataset filename (with or without .csv)"),
    id: str = Path(..., description="Row id"),
    payload: RowModel = Body(..., description="Partial or full row object")
):
    path = dataset_path(name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset not found")

    lock = FileLock(path + ".lock")
    with lock:
        ensure_id_column(path, lock)
        rows = load_csv_rows(path)
        headers = read_headers(path)

        updated = None
        for r in rows:
            if r.get("id") == id:
                for k, v in payload.__root__.items():
                    r[k] = "" if v is None else str(v)
                updated = r
                break
        if not updated:
            raise HTTPException(status_code=404, detail="Row with id not found")

        new_headers = list(dict.fromkeys(headers + list(payload.__root__.keys())))
        write_csv_atomic(path, new_headers, rows)
        return updated


@app.delete("/datasets/{name}/rows/{id}", status_code=status.HTTP_204_NO_CONTENT, summary="Delete a row by id")
def delete_row(
    name: str = Path(..., description="Dataset filename (with or without .csv)"),
    id: str = Path(..., description="Row id")
):
    path = dataset_path(name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset not found")

    lock = FileLock(path + ".lock")
    with lock:
        ensure_id_column(path, lock)
        rows = load_csv_rows(path)
        before = len(rows)
        rows = [r for r in rows if r.get("id") != id]
        after = len(rows)
        if before == after:
            raise HTTPException(status_code=404, detail="Row with id not found")
        headers = read_headers(path)
        write_csv_atomic(path, headers, rows)
    return JSONResponse(status_code=204, content=None)


# -----------------------
# Bulk import / export
# -----------------------

@app.post("/datasets/{name}/import", summary="Bulk import CSV into dataset")
def import_dataset(
    name: str = Path(..., description="Dataset filename (with or without .csv)"),
    file: UploadFile = File(..., description="CSV file to import"),
    mode: str = Query("append", regex="^(append|replace)$", description="append or replace")
):
    """
    mode=replace -> overwrite the dataset with uploaded CSV (ensures id column)
    mode=append  -> append rows from uploaded CSV to existing dataset (or create new)
    """
    path = dataset_path(name)
    # read uploaded file into list of dicts (decode to text)
    content = file.file.read()
    try:
        text = content.decode("utf-8")
    except Exception:
        raise HTTPException(status_code=400, detail="Uploaded file must be UTF-8 encoded CSV")

    # parse CSV
    lines = text.splitlines()
    reader = csv.DictReader(lines)
    imported_rows = [dict(r) for r in reader]
    imported_headers = list(reader.fieldnames or [])

    lock = FileLock(path + ".lock")
    with lock:
        if mode == "replace" or not os.path.exists(path):
            # ensure id column
            if "id" not in imported_headers:
                for r in imported_rows:
                    r["id"] = str(uuid.uuid4())
                final_headers = imported_headers + ["id"]
            else:
                final_headers = imported_headers
            write_csv_atomic(path, final_headers, imported_rows)
            return {"status": "replaced", "rows": len(imported_rows)}
        # else append
        # load existing
        existing_rows = load_csv_rows(path)
        existing_headers = read_headers(path)
        # ensure both have id column
        if "id" not in existing_headers:
            # ensure id for existing rows
            for r in existing_rows:
                r["id"] = str(uuid.uuid4())
            existing_headers = existing_headers + ["id"]
        if "id" not in imported_headers:
            for r in imported_rows:
                r["id"] = str(uuid.uuid4())
            imported_headers = imported_headers + ["id"]

        # unify headers (maintain order: existing headers then new ones)
        unified_headers = list(dict.fromkeys(existing_headers + imported_headers))
        # normalize row dicts to include all headers
        normalized_existing = [{k: (r.get(k) or "") for k in unified_headers} for r in existing_rows]
        normalized_imported = [{k: (r.get(k) or "") for k in unified_headers} for r in imported_rows]
        merged = normalized_existing + normalized_imported
        write_csv_atomic(path, unified_headers, merged)
        return {"status": "appended", "imported": len(imported_rows), "total": len(merged)}


@app.get("/datasets/{name}/export", summary="Export dataset CSV file")
def export_dataset(name: str = Path(..., description="Dataset filename (with or without .csv)")):
    path = dataset_path(name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset not found")
    # Use FileResponse so client receives proper filename and content-type
    filename = os.path.basename(path)
    return FileResponse(path, media_type="text/csv", filename=filename)

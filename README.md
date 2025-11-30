# Agent Data CSV API

A lightweight OpenAPI-first FastAPI service to expose CSV files in the repository as a read/write API.  
Supports single-row CRUD, searching, pagination, bulk import (append/replace) and CSV export.

This repository uses local CSV files stored in `data/` (configurable via `CSV_DATA_DIR`).

---

## Features

- List datasets (CSV files)
- Read dataset schema (headers)
- Query rows with search, sorting, pagination
- Create / Update / Delete individual rows (rows have `id`)
- Bulk import CSV (append or replace)
- Bulk export CSV (download)
- Safe writes (atomic file replace + file locks)
- Validation and robust error handling

---

## Quick start

1. Create a virtualenv and install:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
2.	Start server:

```bash
uvicorn main:app --reload --port 8000
```
3.	Open Swagger UI:

```bash
http://localhost:8000/docs
```
## Configuration
* CSV_DATA_DIR (env) — directory where CSV datasets live. Default: ./data
* MAX_UPLOAD_SIZE (env) — maximum upload size in bytes for import endpoints. Default: 10_485_760 (10 MiB)

Example:
```bash
export CSV_DATA_DIR=/path/to/data
export MAX_UPLOAD_SIZE=5242880  # 5 MB
uvicorn main:app --reload
```
### Repository Map

```bash
repo/
  data/                # CSV files (created automatically)
  main.py              # FastAPI application
  requirements.txt
  api/openapi.yaml     # (optional)
  tests/               # pytest tests (if present)
```

### Endpoints (summary)

* GET /datasets — list CSV filenames
* GET /datasets/{name}/schema — list column names
* GET /datasets/{name}/rows — query rows (search, limit, offset, sort_by, sort_order)
* POST /datasets/{name}/rows — create a row (JSON body)
* PUT /datasets/{name}/rows/{id} — update a row
* DELETE /datasets/{name}/rows/{id} — delete a row
* POST /datasets/{name}/import — bulk import CSV (multipart/form-data file; mode=append|replace)
* GET /datasets/{name}/export — download dataset CSV

Detailed request/response schemas are available in Swagger at /docs.

⸻

#### Dataset name rules

For safety, dataset name must only include: letters, numbers, underscore _, hyphen -.
Example valid names: agents, user_profiles, tool-1. The server automatically appends .csv.

This prevents path traversal and unwanted filesystem access.

⸻

#### Behavior notes
* When a dataset has no id column, the service will add an id (UUID) when a write operation occurs.
* All CSV values are strings. If you need typed columns, keep a separate metadata file or migrate to a DB.
* Concurrency: file locks (via filelock) protect against concurrent writes on a single host. For multi-instance deployments use a shared DB or object store.

⸻

#### Error handling

* Client errors return HTTP 4xx with JSON { "code": <int>, "message": <str>, "details": <optional> }.
* Server errors return HTTP 5xx with JSON { "code": 500, "message": "Internal server error", "trace_id": "<id>" }.
* Uploads larger than MAX_UPLOAD_SIZE return 413 Payload Too Large.

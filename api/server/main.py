"""
FastAPI application for CSV dataset management API.

Features:
- CORS support
- Request validation with Pydantic
- Custom error handling
- Rate limiting
- Structured logging with request IDs
- Health checks and metrics
- Advanced filtering (field-specific, date ranges, numeric comparisons, regex)
- Bulk operations
- Dataset metadata
- JSON export
- Enhanced pagination
- Input sanitization
"""
import os
import csv
import uuid
import tempfile
import re
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Iterator, Union
from collections import defaultdict
from contextlib import contextmanager

from fastapi import (
    FastAPI,
    HTTPException,
    Path,
    Query,
    Body,
    status,
    UploadFile,
    File,
    Request,
    Response,
    Depends,
)
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from filelock import FileLock
from pydantic import BaseModel, Field, validator, constr
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    SLOWAPI_AVAILABLE = True
except ImportError:
    SLOWAPI_AVAILABLE = False
    # Fallback rate limiter stub
    class Limiter:
        def __init__(self, *args, **kwargs):
            pass
        def limit(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator
    class RateLimitExceeded(Exception):
        pass
    def _rate_limit_exceeded_handler(*args, **kwargs):
        pass
    def get_remote_address(*args, **kwargs):
        return "unknown"

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(request_id)s] - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Allow overriding data directory via env var for testing or deployment
DATA_DIR = os.environ.get("CSV_DATA_DIR") or os.path.join(os.path.dirname(__file__), "..", "..", "data", "csv")
os.makedirs(DATA_DIR, exist_ok=True)

# Metadata directory for dataset metadata
METADATA_DIR = os.path.join(os.path.dirname(DATA_DIR), "metadata")
os.makedirs(METADATA_DIR, exist_ok=True)

# Rate limiting configuration
if SLOWAPI_AVAILABLE:
    limiter = Limiter(key_func=get_remote_address)
else:
    limiter = Limiter()  # Stub limiter

# Initialize FastAPI app
app = FastAPI(
    title="Agent Data CSV API",
    version="2.0.0",
    description="""
    A comprehensive REST API for managing CSV datasets with advanced features:
    
    - **CRUD Operations**: Create, read, update, delete rows
    - **Advanced Filtering**: Field-specific filters, date ranges, numeric comparisons, regex
    - **Bulk Operations**: Bulk create, update, delete
    - **Export Formats**: CSV and JSON export
    - **Metadata Management**: Store and retrieve dataset metadata
    - **Health & Metrics**: Health checks and monitoring endpoints
    - **Rate Limiting**: Built-in rate limiting for API protection
    
    All datasets automatically get an `id` column (UUID) for unique row identification.
    """,
    tags_metadata=[
        {
            "name": "datasets",
            "description": "List and manage datasets",
        },
        {
            "name": "rows",
            "description": "CRUD operations on dataset rows",
        },
        {
            "name": "bulk",
            "description": "Bulk operations on multiple rows",
        },
        {
            "name": "metadata",
            "description": "Dataset metadata management",
        },
        {
            "name": "health",
            "description": "Health check and monitoring endpoints",
        },
        {
            "name": "export",
            "description": "Export datasets in various formats",
        },
        {
            "name": "tokenizer",
            "description": "LLM tokenization and token analysis",
        },
    ],
)

# Add rate limiter to app
if SLOWAPI_AVAILABLE:
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID", "X-RateLimit-Limit", "X-RateLimit-Remaining"],
)

# Request ID middleware for logging
@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Add request ID to all requests for tracing."""
    request_id = str(uuid.uuid4())[:8]
    request.state.request_id = request_id
    
    # Add request ID to logger context
    old_factory = logging.getLogRecordFactory()
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.request_id = getattr(request.state, 'request_id', 'unknown')
        return record
    logging.setLogRecordFactory(record_factory)
    
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Process-Time"] = str(round(process_time, 4))
    
    logger.info(
        f"{request.method} {request.url.path} - {response.status_code} - {process_time:.4f}s"
    )
    
    return response

# Metrics tracking
_metrics = {
    "requests_total": 0,
    "requests_by_method": defaultdict(int),
    "requests_by_status": defaultdict(int),
    "errors_total": 0,
    "start_time": time.time(),
}

# Custom exception handlers
@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions with structured error responses."""
    _metrics["errors_total"] += 1
    _metrics["requests_by_status"][exc.status_code] += 1
    
    request_id = getattr(request.state, "request_id", "unknown")
    logger.warning(f"HTTP {exc.status_code}: {exc.detail} [Request ID: {request_id}]")
    
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": exc.status_code,
                "message": exc.detail,
                "request_id": request_id,
            }
        },
        headers={"X-Request-ID": request_id},
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors with detailed messages."""
    _metrics["errors_total"] += 1
    _metrics["requests_by_status"][422] += 1
    
    request_id = getattr(request.state, "request_id", "unknown")
    logger.warning(f"Validation error: {exc.errors()} [Request ID: {request_id}]")
    
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": 422,
                "message": "Validation error",
                "details": exc.errors(),
                "request_id": request_id,
            }
        },
        headers={"X-Request-ID": request_id},
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions."""
    _metrics["errors_total"] += 1
    _metrics["requests_by_status"][500] += 1
    
    request_id = getattr(request.state, "request_id", "unknown")
    logger.error(f"Unexpected error: {exc}", exc_info=True)
    
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": 500,
                "message": "Internal server error",
                "request_id": request_id,
            }
        },
        headers={"X-Request-ID": request_id},
    )

# Utility functions
def sanitize_dataset_name(name: str) -> str:
    """
    Sanitize dataset name to prevent path traversal and invalid characters.
    Only allows: letters, numbers, underscore, hyphen.
    """
    # Remove .csv extension if present
    if name.lower().endswith(".csv"):
        name = name[:-4]
    
    # Validate: only alphanumeric, underscore, hyphen
    if not re.match(r'^[a-zA-Z0-9_-]+$', name):
        raise HTTPException(
            status_code=400,
            detail="Dataset name can only contain letters, numbers, underscore (_), and hyphen (-)"
        )
    
    return name

def dataset_path(name: str) -> str:
    """Get the full path to a dataset CSV file."""
    name = sanitize_dataset_name(name)
    if not name.lower().endswith(".csv"):
        name = f"{name}.csv"
    path = os.path.join(DATA_DIR, name)
    return path

def metadata_path(name: str) -> str:
    """Get the full path to a dataset metadata JSON file."""
    name = sanitize_dataset_name(name)
    return os.path.join(METADATA_DIR, f"{name}.json")

def load_csv_rows(path: str) -> List[Dict[str, str]]:
    """Return list of rows as dictionaries (strings)."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Dataset file not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [dict(row) for row in reader]
    return rows

def read_headers(path: str) -> List[str]:
    """Read CSV headers from a file."""
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

def load_metadata(name: str) -> Dict[str, Any]:
    """Load dataset metadata from JSON file."""
    path = metadata_path(name)
    if not os.path.exists(path):
        return {
            "description": "",
            "schema_version": "1.0",
            "created_at": None,
            "updated_at": None,
        }
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "description": "",
            "schema_version": "1.0",
            "created_at": None,
            "updated_at": None,
        }

def save_metadata(name: str, metadata: Dict[str, Any]):
    """Save dataset metadata to JSON file."""
    path = metadata_path(name)
    existing = load_metadata(name)
    existing.update(metadata)
    existing["updated_at"] = datetime.utcnow().isoformat() + "Z"
    if not existing.get("created_at"):
        existing["created_at"] = datetime.utcnow().isoformat() + "Z"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)

# Advanced filtering functions
def apply_field_filter(rows: List[Dict[str, str]], field: str, operator: str, value: str) -> List[Dict[str, str]]:
    """Apply field-specific filter with operator."""
    filtered = []
    for row in rows:
        field_value = row.get(field, "")
        if field_value is None:
            field_value = ""
        
        match = False
        if operator == "eq":
            match = str(field_value) == value
        elif operator == "ne":
            match = str(field_value) != value
        elif operator == "contains":
            match = value.lower() in str(field_value).lower()
        elif operator == "not_contains":
            match = value.lower() not in str(field_value).lower()
        elif operator == "starts_with":
            match = str(field_value).lower().startswith(value.lower())
        elif operator == "ends_with":
            match = str(field_value).lower().endswith(value.lower())
        elif operator == "regex":
            try:
                match = bool(re.search(value, str(field_value), re.IGNORECASE))
            except re.error:
                continue  # Invalid regex, skip
        elif operator == "gt":
            try:
                match = float(field_value) > float(value)
            except (ValueError, TypeError):
                continue
        elif operator == "gte":
            try:
                match = float(field_value) >= float(value)
            except (ValueError, TypeError):
                continue
        elif operator == "lt":
            try:
                match = float(field_value) < float(value)
            except (ValueError, TypeError):
                continue
        elif operator == "lte":
            try:
                match = float(field_value) <= float(value)
            except (ValueError, TypeError):
                continue
        elif operator == "in":
            values = [v.strip() for v in value.split(",")]
            match = str(field_value) in values
        elif operator == "not_in":
            values = [v.strip() for v in value.split(",")]
            match = str(field_value) not in values
        elif operator == "date_after":
            try:
                row_date = datetime.fromisoformat(str(field_value).replace("Z", "+00:00"))
                filter_date = datetime.fromisoformat(value.replace("Z", "+00:00"))
                match = row_date > filter_date
            except (ValueError, TypeError):
                continue
        elif operator == "date_before":
            try:
                row_date = datetime.fromisoformat(str(field_value).replace("Z", "+00:00"))
                filter_date = datetime.fromisoformat(value.replace("Z", "+00:00"))
                match = row_date < filter_date
            except (ValueError, TypeError):
                continue
        elif operator == "date_between":
            try:
                dates = value.split(",")
                if len(dates) != 2:
                    continue
                row_date = datetime.fromisoformat(str(field_value).replace("Z", "+00:00"))
                start_date = datetime.fromisoformat(dates[0].strip().replace("Z", "+00:00"))
                end_date = datetime.fromisoformat(dates[1].strip().replace("Z", "+00:00"))
                match = start_date <= row_date <= end_date
            except (ValueError, TypeError):
                continue
        
        if match:
            filtered.append(row)
    
    return filtered

# Pydantic models
class RowModel(BaseModel):
    """Model for a single row with flexible schema."""
    __root__: Dict[str, Any]
    
    class Config:
        schema_extra = {
            "example": {
                "__root__": {
                    "name": "Example",
                    "value": 123,
                    "active": True
                }
            }
        }

class BulkCreateRequest(BaseModel):
    """Request model for bulk row creation."""
    rows: List[Dict[str, Any]] = Field(..., min_items=1, max_items=1000, description="Array of row objects")
    
    class Config:
        schema_extra = {
            "example": {
                "rows": [
                    {"name": "Item 1", "value": 10},
                    {"name": "Item 2", "value": 20}
                ]
            }
        }

class BulkUpdateRequest(BaseModel):
    """Request model for bulk row updates."""
    updates: List[Dict[str, Any]] = Field(..., min_items=1, max_items=1000, description="Array of update objects with 'id' field")
    
    class Config:
        schema_extra = {
            "example": {
                "updates": [
                    {"id": "123", "name": "Updated Item 1"},
                    {"id": "456", "value": 99}
                ]
            }
        }

class BulkDeleteRequest(BaseModel):
    """Request model for bulk row deletion."""
    ids: List[str] = Field(..., min_items=1, max_items=1000, description="Array of row IDs to delete")
    
    class Config:
        schema_extra = {
            "example": {
                "ids": ["123", "456", "789"]
            }
        }

class MetadataUpdate(BaseModel):
    """Model for updating dataset metadata."""
    description: Optional[str] = Field(None, description="Dataset description")
    schema_version: Optional[str] = Field(None, description="Schema version string")
    
    class Config:
        schema_extra = {
            "example": {
                "description": "User dataset with contact information",
                "schema_version": "2.0"
            }
        }

class PaginatedResponse(BaseModel):
    """Paginated response model with navigation links."""
    total: int = Field(..., description="Total number of rows matching the query")
    rows: List[Dict[str, Any]] = Field(..., description="Array of row objects")
    limit: int = Field(..., description="Maximum number of rows returned")
    offset: int = Field(..., description="Offset for pagination")
    links: Dict[str, Optional[str]] = Field(..., description="Pagination navigation links")
    
    class Config:
        schema_extra = {
            "example": {
                "total": 100,
                "rows": [{"id": "1", "name": "Example"}],
                "limit": 10,
                "offset": 0,
                "links": {
                    "first": "/datasets/example/rows?limit=10&offset=0",
                    "last": "/datasets/example/rows?limit=10&offset=90",
                    "next": "/datasets/example/rows?limit=10&offset=10",
                    "prev": None
                }
            }
        }

# Health check endpoints
@app.get("/health", tags=["health"], summary="Health check endpoint")
async def health_check():
    """
    Basic health check endpoint.
    Returns 200 if the service is running.
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "version": "2.0.0"
    }

@app.get("/health/ready", tags=["health"], summary="Readiness check endpoint")
async def readiness_check():
    """
    Readiness check endpoint.
    Returns 200 if the service is ready to accept requests.
    """
    # Check if data directory is writable
    try:
        test_file = os.path.join(DATA_DIR, ".health_check")
        with open(test_file, "w") as f:
            f.write("test")
        os.remove(test_file)
        writable = True
    except Exception:
        writable = False
    
    if not writable:
        raise HTTPException(
            status_code=503,
            detail="Service not ready: data directory is not writable"
        )
    
    return {
        "status": "ready",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "data_dir_writable": writable
    }

@app.get("/metrics", tags=["health"], summary="Metrics endpoint")
async def get_metrics():
    """
    Basic metrics endpoint for monitoring.
    Returns request counts, error counts, and uptime.
    """
    uptime = time.time() - _metrics["start_time"]
    return {
        "requests_total": _metrics["requests_total"],
        "requests_by_method": dict(_metrics["requests_by_method"]),
        "requests_by_status": dict(_metrics["requests_by_status"]),
        "errors_total": _metrics["errors_total"],
        "uptime_seconds": round(uptime, 2),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

# Dataset management endpoints
@app.get("/datasets", tags=["datasets"], summary="List available CSV datasets")
@limiter.limit("100/minute")
async def list_datasets(request: Request):
    """
    List all available CSV datasets.
    
    Returns an array of dataset filenames (with .csv extension).
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["GET"] += 1
    
    files = []
    for f in os.listdir(DATA_DIR):
        if f.lower().endswith(".csv"):
            files.append(f)
    return sorted(files)

@app.get("/datasets/{name}/schema", tags=["datasets"], summary="Get CSV column names")
@limiter.limit("100/minute")
async def get_schema(
    request: Request,
    name: str = Path(..., description="Dataset filename (with or without .csv)", example="users")
):
    """
    Get the schema (column names) of a dataset.
    
    Returns the list of column names in the CSV file.
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["GET"] += 1
    
    path = dataset_path(name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset not found")
    headers = read_headers(path)
    return {"columns": headers}

@app.get("/datasets/{name}/rows", tags=["rows"], summary="Query rows from a dataset", response_model=PaginatedResponse)
@limiter.limit("200/minute")
async def get_rows(
    request: Request,
    name: str = Path(..., description="Dataset filename (with or without .csv)", example="users"),
    search: Optional[str] = Query(None, description="Search text (case-insensitive) across all fields"),
    field: Optional[str] = Query(None, description="Field name to filter on"),
    operator: Optional[str] = Query(
        None,
        regex="^(eq|ne|contains|not_contains|starts_with|ends_with|regex|gt|gte|lt|lte|in|not_in|date_after|date_before|date_between)$",
        description="Filter operator: eq, ne, contains, not_contains, starts_with, ends_with, regex, gt, gte, lt, lte, in, not_in, date_after, date_before, date_between"
    ),
    value: Optional[str] = Query(None, description="Filter value (for date_between use comma-separated: start,end)"),
    limit: Optional[int] = Query(100, ge=1, le=10000, description="Max rows to return"),
    offset: Optional[int] = Query(0, ge=0, description="Offset for pagination"),
    sort_by: Optional[str] = Query(None, description="Field name to sort by"),
    sort_order: Optional[str] = Query("asc", regex="^(asc|desc)$", description="Sort order: asc or desc")
):
    """
    Query rows from a dataset with advanced filtering options.
    
    Supports:
    - Global search across all fields
    - Field-specific filtering with various operators
    - Sorting and pagination
    - Regex matching
    - Date range filtering
    - Numeric comparisons
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["GET"] += 1
    
    path = dataset_path(name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    rows = load_csv_rows(path)
    
    # Apply global search
    if search:
        s = search.lower()
        rows = [r for r in rows if any((v or "").lower().find(s) != -1 for v in r.values())]
    
    # Apply field-specific filter
    if field and operator and value is not None:
        rows = apply_field_filter(rows, field, operator, value)
    
    total = len(rows)
    
    # Sort
    if sort_by:
        rows.sort(key=lambda r: (r.get(sort_by) or ""), reverse=(sort_order == "desc"))
    
    # Pagination
    start = offset
    end = offset + limit if limit is not None else None
    paged = rows[start:end]
    
    # Build pagination links
    base_url = str(request.url).split("?")[0]
    query_params = dict(request.query_params)
    query_params.pop("offset", None)
    
    def build_link(offset_val: Optional[int]) -> Optional[str]:
        if offset_val is None:
            return None
        params = query_params.copy()
        params["limit"] = str(limit)
        params["offset"] = str(offset_val)
        return f"{base_url}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
    
    links = {
        "first": build_link(0) if total > 0 else None,
        "last": build_link(max(0, ((total - 1) // limit) * limit)) if total > limit else None,
        "next": build_link(offset + limit) if offset + limit < total else None,
        "prev": build_link(max(0, offset - limit)) if offset > 0 else None,
    }
    
    return PaginatedResponse(
        total=total,
        rows=paged,
        limit=limit,
        offset=offset,
        links=links
    )

@app.post("/datasets/{name}/rows", status_code=status.HTTP_201_CREATED, tags=["rows"], summary="Create a row in dataset")
@limiter.limit("100/minute")
async def create_row(
    request: Request,
    name: str = Path(..., description="Dataset filename (with or without .csv)", example="users"),
    payload: RowModel = Body(..., description="Row object with key/value pairs")
):
    """
    Create a new row in a dataset.
    
    If the dataset doesn't exist, it will be created.
    An `id` field (UUID) will be automatically added if not provided.
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["POST"] += 1
    
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

@app.put("/datasets/{name}/rows/{id}", tags=["rows"], summary="Update a row by id")
@limiter.limit("100/minute")
async def update_row(
    request: Request,
    name: str = Path(..., description="Dataset filename (with or without .csv)", example="users"),
    id: str = Path(..., description="Row id", example="123e4567-e89b-12d3-a456-426614174000"),
    payload: RowModel = Body(..., description="Partial or full row object")
):
    """
    Update an existing row by its ID.
    
    Only the fields provided in the payload will be updated.
    Other fields remain unchanged.
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["PUT"] += 1
    
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

@app.delete("/datasets/{name}/rows/{id}", status_code=status.HTTP_204_NO_CONTENT, tags=["rows"], summary="Delete a row by id")
@limiter.limit("100/minute")
async def delete_row(
    request: Request,
    name: str = Path(..., description="Dataset filename (with or without .csv)", example="users"),
    id: str = Path(..., description="Row id", example="123e4567-e89b-12d3-a456-426614174000")
):
    """
    Delete a row from a dataset by its ID.
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["DELETE"] += 1
    
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
    return Response(status_code=204)

# Bulk operations
@app.post("/datasets/{name}/rows/bulk", status_code=status.HTTP_201_CREATED, tags=["bulk"], summary="Bulk create rows")
@limiter.limit("50/minute")
async def bulk_create_rows(
    request: Request,
    name: str = Path(..., description="Dataset filename (with or without .csv)", example="users"),
    payload: BulkCreateRequest = Body(..., description="Array of row objects to create")
):
    """
    Create multiple rows in a dataset at once.
    
    Maximum 1000 rows per request.
    Each row will get an `id` field (UUID) if not provided.
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["POST"] += 1
    
    path = dataset_path(name)
    lock = FileLock(path + ".lock")
    
    created_rows = []
    with lock:
        existing_rows = []
        existing_headers = []
        
        if os.path.exists(path):
            existing_rows = load_csv_rows(path)
            existing_headers = read_headers(path)
            ensure_id_column(path, lock)
            existing_rows = load_csv_rows(path)
            existing_headers = read_headers(path)
        
        for row_data in payload.rows:
            data = row_data.copy()
            if "id" not in data:
                data["id"] = str(uuid.uuid4())
            created_rows.append(data)
        
        # Merge headers
        all_headers = list(dict.fromkeys(existing_headers + [k for row in created_rows for k in row.keys()]))
        
        # Normalize existing rows
        normalized_existing = [{k: (r.get(k) or "") for k in all_headers} for r in existing_rows]
        
        # Normalize new rows
        normalized_new = [{k: ("" if r.get(k) is None else str(r.get(k))) for k in all_headers} for r in created_rows]
        
        # Combine
        all_rows = normalized_existing + normalized_new
        write_csv_atomic(path, all_headers, all_rows)
    
    return JSONResponse(
        status_code=201,
        content={
            "created": len(created_rows),
            "rows": created_rows
        }
    )

@app.put("/datasets/{name}/rows/bulk", tags=["bulk"], summary="Bulk update rows")
@limiter.limit("50/minute")
async def bulk_update_rows(
    request: Request,
    name: str = Path(..., description="Dataset filename (with or without .csv)", example="users"),
    payload: BulkUpdateRequest = Body(..., description="Array of update objects with 'id' field")
):
    """
    Update multiple rows in a dataset at once.
    
    Maximum 1000 rows per request.
    Each update object must contain an 'id' field to identify the row.
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["PUT"] += 1
    
    path = dataset_path(name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    lock = FileLock(path + ".lock")
    with lock:
        ensure_id_column(path, lock)
        rows = load_csv_rows(path)
        headers = read_headers(path)
        
        updated_count = 0
        updated_rows = []
        row_dict = {r.get("id"): r for r in rows}
        
        for update_data in payload.updates:
            row_id = update_data.get("id")
            if not row_id:
                continue
            
            if row_id in row_dict:
                row = row_dict[row_id]
                for k, v in update_data.items():
                    if k != "id":
                        row[k] = "" if v is None else str(v)
                updated_count += 1
                updated_rows.append(row)
        
        # Update headers if new fields were added
        new_headers = list(dict.fromkeys(headers + [k for u in payload.updates for k in u.keys() if k != "id"]))
        write_csv_atomic(path, new_headers, rows)
    
    return {
        "updated": updated_count,
        "rows": updated_rows
    }

@app.delete("/datasets/{name}/rows/bulk", tags=["bulk"], summary="Bulk delete rows")
@limiter.limit("50/minute")
async def bulk_delete_rows(
    request: Request,
    name: str = Path(..., description="Dataset filename (with or without .csv)", example="users"),
    payload: BulkDeleteRequest = Body(..., description="Array of row IDs to delete")
):
    """
    Delete multiple rows from a dataset at once.
    
    Maximum 1000 IDs per request.
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["DELETE"] += 1
    
    path = dataset_path(name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    lock = FileLock(path + ".lock")
    with lock:
        ensure_id_column(path, lock)
        rows = load_csv_rows(path)
        ids_to_delete = set(payload.ids)
        
        before_count = len(rows)
        rows = [r for r in rows if r.get("id") not in ids_to_delete]
        after_count = len(rows)
        
        deleted_count = before_count - after_count
        headers = read_headers(path)
        write_csv_atomic(path, headers, rows)
    
    return {
        "deleted": deleted_count,
        "requested": len(payload.ids)
    }

# Metadata endpoints
@app.get("/datasets/{name}/metadata", tags=["metadata"], summary="Get dataset metadata")
@limiter.limit("100/minute")
async def get_metadata(
    request: Request,
    name: str = Path(..., description="Dataset filename (with or without .csv)", example="users")
):
    """
    Get metadata for a dataset.
    
    Returns description, schema version, and timestamps.
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["GET"] += 1
    
    path = dataset_path(name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    metadata = load_metadata(name)
    return metadata

@app.put("/datasets/{name}/metadata", tags=["metadata"], summary="Update dataset metadata")
@limiter.limit("50/minute")
async def update_metadata(
    request: Request,
    name: str = Path(..., description="Dataset filename (with or without .csv)", example="users"),
    payload: MetadataUpdate = Body(..., description="Metadata fields to update")
):
    """
    Update metadata for a dataset.
    
    Only provided fields will be updated.
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["PUT"] += 1
    
    path = dataset_path(name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    metadata = {}
    if payload.description is not None:
        metadata["description"] = payload.description
    if payload.schema_version is not None:
        metadata["schema_version"] = payload.schema_version
    
    save_metadata(name, metadata)
    return load_metadata(name)

# Export endpoints
@app.get("/datasets/{name}/export", tags=["export"], summary="Export dataset in CSV or JSON format")
@limiter.limit("100/minute")
async def export_dataset(
    request: Request,
    name: str = Path(..., description="Dataset filename (with or without .csv)", example="users"),
    format: str = Query("csv", regex="^(csv|json)$", description="Export format: csv or json")
):
    """
    Export a dataset in CSV or JSON format.
    
    - CSV: Returns the raw CSV file
    - JSON: Returns a JSON array of row objects
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["GET"] += 1
    
    path = dataset_path(name)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    if format == "csv":
        filename = os.path.basename(path)
        return FileResponse(path, media_type="text/csv", filename=filename)
    else:  # json
        rows = load_csv_rows(path)
        return JSONResponse(content=rows)

# Bulk import endpoint
@app.post("/datasets/{name}/import", tags=["datasets"], summary="Bulk import CSV into dataset")
@limiter.limit("20/minute")
async def import_dataset(
    request: Request,
    name: str = Path(..., description="Dataset filename (with or without .csv)", example="users"),
    file: UploadFile = File(..., description="CSV file to import"),
    mode: str = Query("append", regex="^(append|replace)$", description="append or replace")
):
    """
    Bulk import CSV file into a dataset.
    
    - **append**: Add rows to existing dataset (or create new)
    - **replace**: Overwrite existing dataset with uploaded CSV
    
    The uploaded file must be UTF-8 encoded CSV.
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["POST"] += 1
    
    path = dataset_path(name)
    # read uploaded file into list of dicts (decode to text)
    content = await file.read()
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

# Tokenizer endpoints
try:
    import tiktoken
    TIKTOKEN_AVAILABLE = True
except ImportError:
    TIKTOKEN_AVAILABLE = False

class TokenizeRequest(BaseModel):
    """Request model for tokenization."""
    text: str = Field(..., description="Text to tokenize", min_length=1)
    model: str = Field(
        "gpt-4",
        description="Model to use for tokenization",
        regex="^(gpt-4|gpt-4-turbo|gpt-3.5-turbo|gpt-3.5-turbo-16k|text-davinci-003|text-davinci-002|text-curie-001|text-babbage-001|text-ada-001|cl100k_base|p50k_base|p50k_edit|r50k_base)$"
    )
    
    class Config:
        schema_extra = {
            "example": {
                "text": "Hello, how are you today?",
                "model": "gpt-4"
            }
        }

# Model context window sizes (approximate)
MODEL_CONTEXT_WINDOWS = {
    "gpt-4": 8192,
    "gpt-4-turbo": 128000,
    "gpt-3.5-turbo": 4096,
    "gpt-3.5-turbo-16k": 16384,
    "text-davinci-003": 4097,
    "text-davinci-002": 4097,
    "text-curie-001": 2049,
    "text-babbage-001": 2049,
    "text-ada-001": 2049,
    "cl100k_base": 8192,
    "p50k_base": 2048,
    "p50k_edit": 2048,
    "r50k_base": 2048,
}

# Model to encoding mapping
MODEL_ENCODINGS = {
    "gpt-4": "cl100k_base",
    "gpt-4-turbo": "cl100k_base",
    "gpt-3.5-turbo": "cl100k_base",
    "gpt-3.5-turbo-16k": "cl100k_base",
    "text-davinci-003": "p50k_base",
    "text-davinci-002": "p50k_base",
    "text-curie-001": "r50k_base",
    "text-babbage-001": "r50k_base",
    "text-ada-001": "r50k_base",
    "cl100k_base": "cl100k_base",
    "p50k_base": "p50k_base",
    "p50k_edit": "p50k_edit",
    "r50k_base": "r50k_base",
}

def get_encoding_for_model(model: str) -> str:
    """Get the encoding name for a model."""
    return MODEL_ENCODINGS.get(model, "cl100k_base")

@app.post("/tokenize", tags=["tokenizer"], summary="Tokenize text using LLM tokenizer")
@limiter.limit("200/minute")
async def tokenize_text(
    request: Request,
    payload: TokenizeRequest = Body(..., description="Text and model to tokenize with")
):
    """
    Tokenize text using the specified LLM tokenizer.
    
    Returns detailed tokenization information including:
    - Token IDs
    - Token strings
    - Token count
    - Context window information
    - Token breakdown by character/word
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["POST"] += 1
    
    if not TIKTOKEN_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Tokenization service unavailable: tiktoken not installed"
        )
    
    try:
        encoding_name = get_encoding_for_model(payload.model)
        encoding = tiktoken.get_encoding(encoding_name)
        
        # Tokenize the text
        tokens = encoding.encode(payload.text)
        token_strings = [encoding.decode_single_token_bytes(token).decode('utf-8', errors='replace') for token in tokens]
        
        # Get context window size
        context_window = MODEL_CONTEXT_WINDOWS.get(payload.model, 8192)
        
        # Calculate statistics
        token_count = len(tokens)
        char_count = len(payload.text)
        word_count = len(payload.text.split())
        tokens_per_char = token_count / char_count if char_count > 0 else 0
        tokens_per_word = token_count / word_count if word_count > 0 else 0
        context_usage_percent = (token_count / context_window * 100) if context_window > 0 else 0
        
        # Create token breakdown
        token_breakdown = []
        for i, (token_id, token_str) in enumerate(zip(tokens, token_strings)):
            token_breakdown.append({
                "index": i,
                "token_id": token_id,
                "token_string": token_str,
                "byte_length": len(token_str.encode('utf-8'))
            })
        
        # Estimate cost (rough estimates for GPT-4)
        estimated_cost_per_1k_tokens = {
            "gpt-4": 0.03,  # $0.03 per 1K input tokens
            "gpt-4-turbo": 0.01,
            "gpt-3.5-turbo": 0.0015,
        }
        cost_per_1k = estimated_cost_per_1k_tokens.get(payload.model, 0.01)
        estimated_cost = (token_count / 1000) * cost_per_1k
        
        return {
            "text": payload.text,
            "model": payload.model,
            "encoding": encoding_name,
            "token_count": token_count,
            "character_count": char_count,
            "word_count": word_count,
            "tokens_per_character": round(tokens_per_char, 4),
            "tokens_per_word": round(tokens_per_word, 4),
            "context_window_size": context_window,
            "context_usage_percent": round(context_usage_percent, 2),
            "tokens_remaining": max(0, context_window - token_count),
            "estimated_cost_usd": round(estimated_cost, 6),
            "tokens": token_breakdown,
            "token_ids": tokens,
        }
    except Exception as e:
        logger.error(f"Tokenization error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Tokenization failed: {str(e)}"
        )

@app.get("/tokenize/models", tags=["tokenizer"], summary="List available tokenization models")
@limiter.limit("100/minute")
async def list_tokenizer_models(request: Request):
    """
    List all available tokenization models with their context window sizes.
    """
    _metrics["requests_total"] += 1
    _metrics["requests_by_method"]["GET"] += 1
    
    models = []
    for model, context_window in MODEL_CONTEXT_WINDOWS.items():
        encoding = MODEL_ENCODINGS.get(model, "unknown")
        models.append({
            "model": model,
            "encoding": encoding,
            "context_window": context_window,
            "description": f"{model} model with {context_window:,} token context window"
        })
    
    return {
        "models": models,
        "available": TIKTOKEN_AVAILABLE
    }

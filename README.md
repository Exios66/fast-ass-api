# Agent Data CSV API

A comprehensive, production-ready FastAPI service to expose CSV files as a REST API with advanced features including bulk operations, advanced filtering, metadata management, and comprehensive monitoring.

This repository uses local CSV files stored in `data/csv/` (configurable via `CSV_DATA_DIR`).

---

## Features

### Core Features

- **CRUD Operations**: Create, read, update, delete individual rows
- **Bulk Operations**: Bulk create, update, and delete multiple rows
- **Advanced Filtering**: Field-specific filters, date ranges, numeric comparisons, regex support
- **Export Formats**: CSV and JSON export
- **Metadata Management**: Store and retrieve dataset metadata (description, schema version)
- **Health & Metrics**: Health check endpoints and monitoring metrics
- **Rate Limiting**: Built-in rate limiting for API protection
- **Structured Logging**: Request IDs and detailed logging

### API Features

- **CORS Support**: Configurable cross-origin resource sharing
- **Request Validation**: Enhanced Pydantic models for request/response validation
- **Error Handling**: Custom exception handlers with proper status codes
- **Pagination**: Enhanced pagination with navigation links (first, last, next, prev)
- **Input Sanitization**: Strict validation of dataset names to prevent path traversal

### Developer Experience

- **OpenAPI Documentation**: Comprehensive API docs with examples and tags
- **Interactive UI**: Enhanced web interface with dark/light theme
- **Request History**: Track and replay API requests
- **Code Examples**: Examples in cURL, Python, and JavaScript
- **Docker Support**: Containerized deployment ready

---

## Quick Start

### Local Development

1. **Create a virtualenv and install dependencies:**

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

2. **Start the server:**

```bash
uvicorn api.server.main:app --reload --port 8000
```

3. **Access the API:**

- **Swagger UI**: <http://localhost:8000/docs>
- **ReDoc**: <http://localhost:8000/redoc>
- **Interactive UI**: Open `index.html` in a browser

### Docker Deployment

```bash
# Build and run with Docker Compose
docker-compose up -d

# Or build manually
docker build -t fast-ass-api .
docker run -p 8000:8000 -v $(pwd)/data:/app/data fast-ass-api
```

---

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `CSV_DATA_DIR` | Directory where CSV datasets are stored | `./data/csv` |
| `CORS_ORIGINS` | Comma-separated list of allowed CORS origins | `*` |
| `MAX_UPLOAD_SIZE` | Maximum upload size in bytes | `10485760` (10 MB) |
| `LOG_LEVEL` | Logging level (DEBUG, INFO, WARNING, ERROR) | `INFO` |

### Example Configuration

```bash
export CSV_DATA_DIR=/path/to/data/csv
export CORS_ORIGINS=https://app.example.com,https://admin.example.com
export MAX_UPLOAD_SIZE=52428800  # 50 MB
export LOG_LEVEL=INFO
uvicorn api.server.main:app --reload
```

---

## API Endpoints

### Dataset Management

- `GET /datasets` - List available CSV datasets
- `GET /datasets/{name}/schema` - Get dataset column names
- `GET /datasets/{name}/metadata` - Get dataset metadata
- `PUT /datasets/{name}/metadata` - Update dataset metadata

### Row Operations

- `GET /datasets/{name}/rows` - Query rows with advanced filtering
- `POST /datasets/{name}/rows` - Create a row
- `PUT /datasets/{name}/rows/{id}` - Update a row
- `DELETE /datasets/{name}/rows/{id}` - Delete a row

### Bulk Operations

- `POST /datasets/{name}/rows/bulk` - Bulk create rows
- `PUT /datasets/{name}/rows/bulk` - Bulk update rows
- `DELETE /datasets/{name}/rows/bulk` - Bulk delete rows

### Import/Export

- `POST /datasets/{name}/import` - Bulk import CSV (append or replace)
- `GET /datasets/{name}/export` - Export dataset (CSV or JSON)

### Health & Monitoring

- `GET /health` - Health check endpoint
- `GET /health/ready` - Readiness check endpoint
- `GET /metrics` - Application metrics

---

## Advanced Filtering

The API supports advanced filtering with various operators:

### Filter Operators

- **Text**: `eq`, `ne`, `contains`, `not_contains`, `starts_with`, `ends_with`, `regex`
- **Numeric**: `gt`, `gte`, `lt`, `lte`
- **Lists**: `in`, `not_in`
- **Dates**: `date_after`, `date_before`, `date_between`

### Examples

```bash
# Filter by exact match
GET /datasets/users/rows?field=role&operator=eq&value=admin

# Filter by date range
GET /datasets/users/rows?field=created_at&operator=date_after&value=2025-01-01T00:00:00Z

# Filter with regex
GET /datasets/users/rows?field=email&operator=regex&value=.*@example\.com

# Numeric comparison
GET /datasets/agents/rows?field=score&operator=gt&value=80
```

---

## Repository Structure

```
fast-ass-api/
├── api/
│   ├── server/
│   │   └── main.py          # FastAPI application
│   └── openapi.yaml         # OpenAPI specification
├── data/
│   ├── csv/                 # CSV dataset files
│   └── metadata/            # Dataset metadata JSON files
├── tests/                   # Test suite
├── scripts/                 # Utility scripts
├── .github/
│   └── workflows/
│       └── ci.yml           # CI/CD pipeline
├── Dockerfile               # Docker configuration
├── docker-compose.yml       # Docker Compose setup
├── requirements.txt        # Python dependencies
├── CHANGELOG.md            # Auto-updated changelog
├── CONTRIBUTING.md         # Contribution guidelines
├── DEPLOYMENT.md           # Deployment guide
└── index.html              # Interactive web UI
```

---

## Dataset Name Rules

For security, dataset names must only contain:

- Letters (a-z, A-Z)
- Numbers (0-9)
- Underscore (_)
- Hyphen (-)

Examples: `users`, `user_profiles`, `agent-data-2024`

The server automatically appends `.csv` extension.

---

## Behavior Notes

- **Automatic IDs**: When a dataset has no `id` column, the service automatically adds UUID-based IDs on write operations
- **Data Types**: All CSV values are stored as strings. For typed data, use metadata or migrate to a database
- **Concurrency**: File locks protect against concurrent writes on a single host. For multi-instance deployments, use shared storage or a database
- **Pagination**: Default page size is 100 rows, maximum is 10,000 rows per request

---

## Error Handling

- **Client Errors (4xx)**: Return JSON with `error.code`, `error.message`, and `error.request_id`
- **Server Errors (5xx)**: Return JSON with error details and request ID for tracing
- **Validation Errors (422)**: Detailed validation error messages
- **Rate Limiting (429)**: Rate limit exceeded responses

---

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=api/server --cov-report=html

# Run specific test file
pytest tests/test_api.py -v
```

---

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines and contribution instructions.

### Code Quality

```bash
# Format code
black api/ tests/

# Lint code
flake8 api/ tests/

# Type checking
mypy api/
```

---

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed deployment instructions for various platforms.

### Quick Deploy Options

- **Docker**: `docker-compose up -d`
- **Systemd**: See DEPLOYMENT.md for service file
- **Cloud**: Supports AWS, GCP, Heroku, Railway, etc.

---

## Automated Changelog

This project uses an automated changelog system that updates `CHANGELOG.md` on each push to the main branch.

### How It Works

1. **GitHub Actions Workflow**: On push to main, the CI pipeline runs
2. **Commit Parsing**: Uses `git-cliff` to parse conventional commits
3. **Changelog Generation**: Automatically generates changelog entries
4. **Auto-commit**: Commits and pushes the updated changelog

### Commit Format

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(api): add bulk delete endpoint
fix(filter): correct date range filtering
docs(readme): update installation instructions
```

### Manual Update

You can also manually update the changelog:

```bash
# Using git-cliff (if installed)
git-cliff --output CHANGELOG.md --latest

# Using Python script
python scripts/update-changelog.py
```

---

## License

See [LICENSE](LICENSE) file for details.

---

## Support

- **Documentation**: See `/docs` endpoint for interactive API documentation
- **Issues**: Open an issue on GitHub
- **Contributing**: See [CONTRIBUTING.md](CONTRIBUTING.md)

---

**Version**: 2.0.0  
**Last Updated**: 2025-11-29

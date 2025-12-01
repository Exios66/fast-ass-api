# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial release with comprehensive features

## [2.0.0] - 2025-11-29

### Added

- CORS middleware with configurable origins
- Request validation with enhanced Pydantic models
- Custom exception handlers with proper status codes
- Rate limiting middleware (slowapi)
- Structured logging with request IDs
- Health check endpoints (`/health`, `/health/ready`)
- Metrics endpoint (`/metrics`)
- Advanced filtering (field-specific, date ranges, numeric comparisons, regex)
- Bulk operations endpoints (create, update, delete)
- Dataset metadata endpoints (GET/PUT `/datasets/{name}/metadata`)
- JSON export format support
- Enhanced pagination with navigation links
- Input sanitization for dataset names
- Enhanced OpenAPI documentation with examples and tags
- Comprehensive frontend UI with dark/light theme
- Request history tracking
- Query presets
- Data visualization support
- Code examples in multiple languages
- Rich data examples (7 new datasets)

### Changed

- Improved error messages with actionable suggestions
- Better response pagination with first/last/next/prev links
- Enhanced dataset examples with more fields and realistic data

### Fixed

- Dataset name validation to prevent path traversal
- Date filtering timezone handling
- CSV import/export encoding issues

## [1.1.0] - Previous Version

### Added

- Basic CRUD operations
- CSV import/export
- Search and pagination
- Basic error handling

---

*This changelog is automatically updated on each push to the main branch.*

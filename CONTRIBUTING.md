# Contributing to Fast-Ass API

Thank you for your interest in contributing to Fast-Ass API! This document provides guidelines and instructions for contributing.

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers and help them learn
- Focus on constructive feedback
- Respect different viewpoints and experiences

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/your-username/fast-ass-api.git`
3. Create a virtual environment: `python -m venv .venv`
4. Activate it: `source .venv/bin/activate` (or `.\venv\Scripts\activate` on Windows)
5. Install dependencies: `pip install -r requirements.txt`
6. Install development dependencies: `pip install -r requirements-dev.txt` (if exists)
7. Create a branch: `git checkout -b feature/your-feature-name`

## Development Setup

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=api/server --cov-report=html

# Run specific test file
pytest tests/test_api.py

# Run with verbose output
pytest -v
```

### Running the Server

```bash
# Development mode with auto-reload
uvicorn api.server.main:app --reload --port 8000

# Production mode
uvicorn api.server.main:app --host 0.0.0.0 --port 8000
```

### Code Style

We use:
- **Black** for code formatting
- **flake8** for linting
- **mypy** for type checking

```bash
# Format code
black api/ tests/

# Check linting
flake8 api/ tests/

# Type checking
mypy api/
```

## Making Changes

### Branch Naming

- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation updates
- `refactor/description` - Code refactoring
- `test/description` - Test additions/updates

### Commit Messages

Follow the [Conventional Commits](https://www.conventionalcommits.org/) format:

```
<type>(<scope>): <subject>

<body>

<footer>
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Test additions or changes
- `chore`: Maintenance tasks

Examples:
```
feat(api): add bulk delete endpoint

Add POST /datasets/{name}/rows/bulk endpoint for deleting multiple rows at once.

Closes #123
```

```
fix(filter): correct date range filtering

Fix issue where date_between operator was not working correctly with timezone-aware dates.

Fixes #456
```

### Pull Request Process

1. Ensure all tests pass: `pytest`
2. Ensure code is formatted: `black api/ tests/`
3. Ensure no linting errors: `flake8 api/ tests/`
4. Update documentation if needed
5. Add tests for new features
6. Update CHANGELOG.md (or let automation handle it)
7. Create a pull request with a clear description

### PR Description Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Tests pass locally
- [ ] Added tests for new features
- [ ] Updated existing tests

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Comments added for complex code
- [ ] Documentation updated
- [ ] No new warnings generated
```

## Testing Guidelines

- Write tests for all new features
- Maintain or improve test coverage
- Test edge cases and error conditions
- Use descriptive test names
- Follow the existing test structure

Example test:

```python
def test_bulk_delete_removes_multiple_rows():
    """Test that bulk delete removes all specified rows."""
    # Setup
    client.post("/datasets/test/rows", json={"name": "Item 1"})
    client.post("/datasets/test/rows", json={"name": "Item 2"})
    
    # Get IDs
    rows = client.get("/datasets/test/rows").json()["rows"]
    ids = [r["id"] for r in rows]
    
    # Delete
    response = client.delete(
        "/datasets/test/rows/bulk",
        json={"ids": ids}
    )
    
    # Assert
    assert response.status_code == 200
    assert response.json()["deleted"] == 2
```

## Documentation

- Update README.md for user-facing changes
- Add docstrings to new functions/classes
- Update API documentation if endpoints change
- Add examples for new features

## Questions?

- Open an issue for discussion
- Check existing issues and PRs
- Review the codebase to understand patterns

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.


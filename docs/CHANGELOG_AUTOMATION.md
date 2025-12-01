# Automated Changelog System

This document explains how the automated changelog system works in this project.

## Overview

The project automatically updates `CHANGELOG.md` on each push to the main branch using GitHub Actions. The system uses `git-cliff` to parse git commits and generate changelog entries following the [Keep a Changelog](https://keepachangelog.com/) format.

## How It Works

### 1. GitHub Actions Workflow

The `.github/workflows/ci.yml` file contains a job called `update-changelog` that:

1. Runs only on pushes to the `main` branch
2. Checks out the repository with full git history
3. Installs `git-cliff` tool
4. Generates changelog from commits since the last tag
5. Commits and pushes the updated changelog

### 2. Commit Parsing

The system uses `git-cliff` configured via `cliff.toml` to:

- Parse conventional commits
- Group commits by type (Added, Fixed, Changed, etc.)
- Extract issue numbers and links
- Format entries consistently

### 3. Fallback System

If `git-cliff` fails, the workflow falls back to a Python script (`scripts/update-changelog.py`) that:

- Parses git commits manually
- Groups by conventional commit types
- Generates changelog entries

## Commit Format

To ensure proper changelog generation, use [Conventional Commits](https://www.conventionalcommits.org/):

### Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

- `feat`: New feature → **Added** section
- `fix`: Bug fix → **Fixed** section
- `docs`: Documentation → **Documentation** section
- `style`: Code style changes → **Changed** section
- `refactor`: Code refactoring → **Changed** section
- `perf`: Performance improvements → **Changed** section
- `test`: Test additions/changes → **Changed** section
- `build`: Build system changes → **Changed** section
- `ci`: CI/CD changes → **Changed** section
- `chore`: Maintenance tasks → **Changed** section
- `revert`: Revert commits → **Fixed** section

### Examples

```bash
# Feature
git commit -m "feat(api): add bulk delete endpoint"

# Bug fix
git commit -m "fix(filter): correct date range filtering"

# Documentation
git commit -m "docs(readme): update installation instructions"

# With issue reference
git commit -m "feat(api): add rate limiting (#123)"

# Breaking change
git commit -m "feat(api)!: change response format"
```

## Configuration

### cliff.toml

The `cliff.toml` file configures:

- Changelog header and footer
- Commit parsing rules
- Section grouping
- Link generation
- Tag pattern matching

### Customization

To customize the changelog format:

1. Edit `cliff.toml` for git-cliff configuration
2. Edit `scripts/update-changelog.py` for Python script behavior
3. Modify `.github/workflows/ci.yml` for workflow changes

## Manual Updates

### Using git-cliff

```bash
# Install git-cliff
# Linux
curl -L https://github.com/orhun/git-cliff/releases/latest/download/git-cliff-1.4.0-x86_64-unknown-linux-gnu.tar.gz | tar -xz
sudo mv git-cliff /usr/local/bin/

# macOS
brew install git-cliff

# Generate changelog
git-cliff --output CHANGELOG.md --latest --strip header
```

### Using Python Script

```bash
python scripts/update-changelog.py
```

### Using Shell Script

```bash
bash scripts/update-changelog.sh
```

## Troubleshooting

### Changelog Not Updating

1. **Check GitHub Actions**: Verify the workflow runs successfully
2. **Check Permissions**: Ensure `GITHUB_TOKEN` has write permissions
3. **Check Commits**: Verify commits follow conventional format
4. **Check Tags**: Ensure there are git tags for version tracking

### Incorrect Formatting

1. **Review cliff.toml**: Check configuration matches your needs
2. **Test Locally**: Run git-cliff or Python script locally
3. **Check Commit Format**: Ensure commits follow conventional format

### Missing Commits

1. **Check Tag**: Verify the latest tag is correct
2. **Check Range**: Commits must be after the latest tag
3. **Check Filtering**: Some commits may be filtered out

## Best Practices

1. **Use Conventional Commits**: Always use the conventional commit format
2. **Reference Issues**: Include issue numbers in commit messages
3. **Write Clear Messages**: Descriptive commit messages improve changelog quality
4. **Tag Releases**: Create git tags for version releases
5. **Review Changelog**: Check auto-generated changelog before releases

## Version Management

### Creating a Release

1. Update version in code (if applicable)
2. Create a git tag: `git tag -a v2.0.0 -m "Release version 2.0.0"`
3. Push tag: `git push origin v2.0.0`
4. The changelog will automatically update on the next push to main

### Version Format

- Follow [Semantic Versioning](https://semver.org/)
- Tag format: `v1.2.3` or `1.2.3`
- Changelog will extract version from tags

## Integration with CI/CD

The changelog update is integrated into the CI/CD pipeline:

1. **On Push to Main**: Changelog updates automatically
2. **On Tag Creation**: New version section is created
3. **Skip CI**: Changelog commits use `[skip ci]` to avoid loops

## Future Enhancements

Potential improvements:

- Integration with release notes
- Automatic version bumping
- Integration with package managers
- Multi-format output (Markdown, JSON, etc.)

---

For questions or issues, see the main [CONTRIBUTING.md](../CONTRIBUTING.md) file.


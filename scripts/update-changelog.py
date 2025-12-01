#!/usr/bin/env python3
"""
Automated changelog updater using git commits.
This script parses git commits and updates CHANGELOG.md following Keep a Changelog format.
"""
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple

CHANGELOG_PATH = Path(__file__).parent.parent / "CHANGELOG.md"


def get_git_log(since: str = None) -> List[str]:
    """Get git log since a specific tag or all commits."""
    cmd = ["git", "log", "--pretty=format:%H|%s|%b", "--reverse"]
    if since:
        cmd.append(f"{since}..HEAD")
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return [line for line in result.stdout.strip().split("\n") if line]


def parse_commit(commit_line: str) -> Dict:
    """Parse a commit line into structured data."""
    parts = commit_line.split("|", 2)
    if len(parts) < 2:
        return None
    
    hash_short = parts[0][:7]
    message = parts[1]
    body = parts[2] if len(parts) > 2 else ""
    
    # Parse conventional commit format
    pattern = r"^(?P<type>\w+)(?:\((?P<scope>[^)]+)\))?:\s*(?P<description>.+)$"
    match = re.match(pattern, message)
    
    if match:
        commit_type = match.group("type")
        scope = match.group("scope")
        description = match.group("description")
        
        # Map commit types to changelog sections
        type_map = {
            "feat": "Added",
            "fix": "Fixed",
            "docs": "Documentation",
            "style": "Changed",
            "refactor": "Changed",
            "perf": "Changed",
            "test": "Changed",
            "build": "Changed",
            "ci": "Changed",
            "chore": "Changed",
            "revert": "Fixed",
        }
        
        section = type_map.get(commit_type, "Changed")
        
        # Extract issue numbers
        issues = re.findall(r"#(\d+)", message + " " + body)
        
        return {
            "hash": hash_short,
            "type": commit_type,
            "scope": scope,
            "description": description,
            "section": section,
            "issues": issues,
            "breaking": "!" in message or "BREAKING" in message.upper(),
        }
    return None


def get_latest_tag() -> str:
    """Get the latest git tag."""
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None


def group_commits(commits: List[Dict]) -> Dict[str, List[Dict]]:
    """Group commits by section."""
    grouped = {}
    for commit in commits:
        section = commit["section"]
        if section not in grouped:
            grouped[section] = []
        grouped[section].append(commit)
    return grouped


def format_changelog_entry(commits: List[Dict], version: str = None) -> str:
    """Format commits into changelog entry."""
    grouped = group_commits(commits)
    
    if version:
        date = datetime.now().strftime("%Y-%m-%d")
        header = f"## [{version}] - {date}\n\n"
    else:
        header = "## [Unreleased]\n\n"
    
    sections_order = ["Added", "Changed", "Fixed", "Documentation", "Removed", "Security"]
    lines = [header]
    
    for section in sections_order:
        if section in grouped:
            lines.append(f"### {section}\n")
            for commit in grouped[section]:
                breaking = "[**breaking**] " if commit["breaking"] else ""
                issues = ""
                if commit["issues"]:
                    issues = " (" + ", ".join(f"#{i}" for i in commit["issues"]) + ")"
                desc = commit["description"].capitalize()
                lines.append(f"- {breaking}{desc}{issues}\n")
            lines.append("\n")
    
    return "".join(lines)


def update_changelog(new_entry: str):
    """Update CHANGELOG.md with new entry."""
    if not CHANGELOG_PATH.exists():
        # Create new changelog
        content = """# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

"""
        content += new_entry
        content += "\n---\n\n*This changelog is automatically updated on each push to the main branch.*\n"
    else:
        # Read existing changelog
        content = CHANGELOG_PATH.read_text()
        
        # Find [Unreleased] section and replace with new entry
        pattern = r"## \[Unreleased\].*?(?=## \[|\Z)"
        if re.search(pattern, content, re.DOTALL):
            content = re.sub(pattern, new_entry, content, flags=re.DOTALL)
        else:
            # Insert after header
            header_end = content.find("## [")
            if header_end == -1:
                header_end = len(content)
            content = content[:header_end] + new_entry + "\n" + content[header_end:]
    
    CHANGELOG_PATH.write_text(content)


def main():
    """Main function."""
    latest_tag = get_latest_tag()
    print(f"Latest tag: {latest_tag or 'None (using all commits)'}")
    
    commits_raw = get_git_log(since=latest_tag)
    commits = [parse_commit(c) for c in commits_raw if parse_commit(c)]
    
    if not commits:
        print("No new commits to add to changelog")
        return
    
    print(f"Found {len(commits)} commits")
    
    # Determine version (could be extracted from tag or use Unreleased)
    version = None
    if latest_tag:
        # Extract version from tag (remove 'v' prefix if present)
        version = latest_tag.lstrip("v")
    
    entry = format_changelog_entry(commits, version)
    update_changelog(entry)
    
    print(f"Updated {CHANGELOG_PATH}")


if __name__ == "__main__":
    main()


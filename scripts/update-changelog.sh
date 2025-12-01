#!/bin/bash
# Script to update CHANGELOG.md automatically
# This script uses git-cliff to generate changelog from git commits

set -e

# Check if git-cliff is installed
if ! command -v git-cliff &> /dev/null; then
    echo "git-cliff is not installed. Installing..."
    # Try to install git-cliff
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        curl -L https://github.com/orhun/git-cliff/releases/latest/download/git-cliff-1.4.0-x86_64-unknown-linux-gnu.tar.gz | tar -xz
        sudo mv git-cliff /usr/local/bin/ || mv git-cliff ~/.local/bin/
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        brew install git-cliff || echo "Please install git-cliff manually: brew install git-cliff"
    else
        echo "Please install git-cliff manually from https://github.com/orhun/git-cliff"
        exit 1
    fi
fi

# Get the latest tag or use "Unreleased"
LATEST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")

if [ -z "$LATEST_TAG" ]; then
    echo "No tags found, generating changelog from all commits"
    git-cliff --output CHANGELOG.md --latest --strip header
else
    echo "Generating changelog since $LATEST_TAG"
    git-cliff --output CHANGELOG.md --latest --strip header --tag "$LATEST_TAG"
fi

echo "CHANGELOG.md updated successfully"


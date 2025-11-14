#!/bin/bash

set -e

echo "========================================="
echo "Initializing Gradle Wrapper"
echo "========================================="

# Check if gradle is installed
if ! command -v gradle &> /dev/null
then
    echo "Error: Gradle is not installed."
    echo ""
    echo "Please install Gradle first:"
    echo "  macOS: brew install gradle"
    echo "  Linux: apt-get install gradle or yum install gradle"
    echo "  Or download from: https://gradle.org/install/"
    exit 1
fi

# Generate Gradle wrapper
echo "Generating Gradle wrapper..."
gradle wrapper --gradle-version 8.4

echo ""
echo "✓ Gradle wrapper initialized successfully"
echo ""
echo "You can now run: ./setup.sh"
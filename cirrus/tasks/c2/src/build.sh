#!/bin/bash
set -e

# Planetary Computer to S3 Task Deployment Script
# This script packages the Python code into a Lambda-compatible ZIP file

TASK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${TASK_DIR}/build"
DEPLOYMENT_ZIP="${TASK_DIR}/deployment.zip"

echo "📦 Building Planetary Computer to S3 task..."

# Check if zip is available, install if needed
if ! command -v zip &> /dev/null; then
    echo "📦 Installing zip package..."
    if command -v yum &> /dev/null; then
        yum install -y zip
    elif command -v apt-get &> /dev/null; then
        apt-get update && apt-get install -y zip
    elif command -v apk &> /dev/null; then
        apk add zip
    else
        echo "⚠️ zip command not found, will use Python zipfile instead"
        USE_PYTHON_ZIP=true
    fi
else
    USE_PYTHON_ZIP=false
fi

# Clean previous build
rm -rf "${BUILD_DIR}" "${DEPLOYMENT_ZIP}"
mkdir -p "${BUILD_DIR}"

echo "📚 Installing Python dependencies..."
# Install packages without platform targeting to avoid cross-compilation issues
pip install -r "${TASK_DIR}/requirements.txt" -t "${BUILD_DIR}" --upgrade

# Copy source code
echo "📝 Copying source code..."
cp "${TASK_DIR}/handler.py" "${BUILD_DIR}/"

# Create ZIP package
echo "🗜️ Creating deployment package..."
cd "${BUILD_DIR}"

if [ "$USE_PYTHON_ZIP" = true ]; then
    echo "Using Python zipfile module..."
    python -c "
import zipfile
import os
import sys

def create_zip(source_dir, output_path):
    with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arc_name = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arc_name)

create_zip('.', '${DEPLOYMENT_ZIP}')
print('ZIP file created successfully')
"
else
    zip -r "${DEPLOYMENT_ZIP}" . -q
fi

# Cleanup
cd "${TASK_DIR}"
rm -rf "${BUILD_DIR}"

PACKAGE_SIZE=$(du -h "${DEPLOYMENT_ZIP}" | cut -f1)
echo "✅ Deployment package created: ${DEPLOYMENT_ZIP}"
echo "📏 Package size: ${PACKAGE_SIZE}"

# Check if package is too large for Lambda
PACKAGE_SIZE_BYTES=$(stat -f%z "${DEPLOYMENT_ZIP}" 2>/dev/null || stat -c%s "${DEPLOYMENT_ZIP}")
if [ "$PACKAGE_SIZE_BYTES" -gt 52428800 ]; then  # 50MB limit
    echo "⚠️  Warning: Package size (${PACKAGE_SIZE}) exceeds 50MB Lambda limit."
    echo "   Consider using container deployment instead."
fi

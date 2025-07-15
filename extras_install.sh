#!/usr/bin/env bash

OS_NAME=$(grep -oP '^NAME="\K[^"]+' /etc/os-release || echo "Unknown")
ARCHITECTURE=$(uname -m)
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d. -f1)
PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d. -f2)

echo "Platform: $OS_NAME, Arch: $ARCHITECTURE, Python: $PYTHON_VERSION"

# Decide pip flags based on Python version
PIP_FLAG=""
if [ "$PYTHON_MAJOR" -eq 3 ]; then
    if [ "$PYTHON_MINOR" -ge 11 ] && [ "$PYTHON_MINOR" -lt 12 ]; then
        PIP_FLAG="--break-system-packages"
    elif [ "$PYTHON_MINOR" -ge 12 ]; then
        PIP_FLAG="--ignore-installed --break-system-packages"
    fi
fi

# Upgrade pip if on Ubuntu ARM64
if [ "$OS_NAME" = "Ubuntu" ] && [ "$ARCHITECTURE" = "aarch64" ]; then
    echo "Upgrading pip for Ubuntu ARM64..."
    python3 -m pip install --upgrade pip $PIP_FLAG
fi

# Decide which pandas version to install
if [ "$PYTHON_MAJOR" -eq 3 ]; then
    if [ "$PYTHON_MINOR" -le 10 ]; then
        PANDAS_VERSION="pandas==1.1.5"
    elif [ "$PYTHON_MINOR" -eq 11 ]; then
        PANDAS_VERSION="pandas>=1.5,<2.3"
    elif [ "$PYTHON_MINOR" -ge 12 ]; then
        PANDAS_VERSION="pandas>=2.1,<2.3"
    else
        echo "Unsupported Python version: $PYTHON_VERSION"
        exit 1
    fi
else
    echo "Unsupported Python major version: $PYTHON_VERSION"
    exit 1
fi

echo "Installing $PANDAS_VERSION..."
python3 -m pip install "$PANDAS_VERSION" $PIP_FLAG


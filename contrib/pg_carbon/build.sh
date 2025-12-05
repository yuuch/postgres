#!/bin/bash
set -e

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

# Define the Postgres installation directory (relative to this script)
# Adjust this path if your postgres installation is elsewhere
PG_INSTALL_DIR="../../pg_carbon_build_dir"

# Resolve absolute path for robustness
if [ -d "$PG_INSTALL_DIR" ]; then
    PG_INSTALL_DIR_ABS="$(cd "$PG_INSTALL_DIR" && pwd)"
else
    echo "Error: Postgres install directory not found at: $PG_INSTALL_DIR"
    echo "Please ensure you have built/installed postgres in the root 'pg_carbon_build_dir' or update this script."
    exit 1
fi

# Add pg_config to PATH
export PATH="$PG_INSTALL_DIR_ABS/bin:$PATH"

echo "Using pg_config: $(which pg_config)"
echo "Target Install Dir: $PG_INSTALL_DIR_ABS"

BUILD_DIR="build_standalone"

# Configure Meson
if [ ! -d "$BUILD_DIR" ]; then
    echo "Configuring build in $BUILD_DIR..."
    meson setup "$BUILD_DIR"
else
    echo "Reconfiguring build in $BUILD_DIR..."
    meson setup "$BUILD_DIR" --reconfigure
fi

# Compile
echo "Compiling..."
meson compile -C "$BUILD_DIR"

# Install
echo "Installing..."
meson install -C "$BUILD_DIR"

echo "pg_carbon built and installed successfully!"

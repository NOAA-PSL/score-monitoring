#!/bin/bash

# ----------------------------------------------------------------------
# Usage Instructions:
# ----------------------------------------------------------------------
# This script installs all files from the 'scripts/' directory into a
# specified cylc workflow directory. It
# will prompt the user before overwriting any existing files unless
# the '--force' option is provided.
#
# Usage:
#   ./install_scripts.sh <cylc_workflow_directory> [--force]
#
# Parameters:
#   <cylc_workflow_directory>  - The cylc workflow directory to which the scripts will be installed.
#   [--force]                - Optional flag to overwrite files without prompting.
#
# Example:
#   ./install_scripts.sh cylc_test_flow/
#   ./install_scripts.sh cylc_test_flow/ --force
#
# Notes:
# - Ensure the 'scripts/' directory exists and contains the scripts you want to install.
# - The cylc workflow  directory must exist.
# - The script will not install scripts if the workflow directory is missing or invalid.
# - The script will ask for user confirmation before overwriting any existing files unless
#   the '--force' option is provided.
# ----------------------------------------------------------------------

# Check if the destination directory is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <cylc_workflow_directory> [--force]"
  exit 1
fi

# Parse arguments
DEST_DIR="$1"
FORCE=false

# Check if '--force' flag is set
if [ "$2" == "--force" ]; then
  FORCE=true
fi

# Check if the destination directory exists
if [ ! -d "$DEST_DIR" ]; then
  echo "Error: The workflow directory '$DEST_DIR' does not exist."
  exit 2
fi

# Define the 'bin/' directory inside the destination
BIN_DIR="$DEST_DIR/bin/"

# Check if the source 'scripts/' directory exists
SOURCE_DIR="scripts/"
if [ ! -d "$SOURCE_DIR" ]; then
  echo "Error: The source directory '$SOURCE_DIR' does not exist."
  exit 3
fi

# Create the 'bin/' directory if it doesn't exist
if [ ! -d "$BIN_DIR" ]; then
  echo "Creating 'bin/' directory in '$DEST_DIR'..."
  mkdir "$BIN_DIR"
fi

# Loop through each file in the 'scripts/' directory
for file in "$SOURCE_DIR"/*.py; do
  # Extract the filename from the path
  filename=$(basename "$file")

  # Check if the file already exists in the 'bin/' directory
  if [ -e "$BIN_DIR$filename" ]; then
    # If force is enabled, overwrite without prompting
    if [ "$FORCE" = true ]; then
      cp "$file" "$BIN_DIR"
      echo "Forced overwrite of '$filename'."
    else
      # Ask the user if they want to overwrite the file
      read -p "File '$filename' already exists in '$BIN_DIR'. Do you want to overwrite it? (y/n): " choice
      case "$choice" in
        [Yy]*)
          # Overwrite the file
          cp "$file" "$BIN_DIR"
          echo "Overwritten '$filename'."
          ;;
        [Nn]*)
          # Skip overwriting the file
          echo "Skipped '$filename'."
          ;;
        *)
          # Invalid input, skip the file
          echo "Invalid choice. Skipping '$filename'."
          ;;
      esac
    fi
  else
    # Copy the file if it does not exist
    cp "$file" "$BIN_DIR"
    echo "Copied '$filename'."
  fi
done

echo "Installation of scripts/*.py completed."

